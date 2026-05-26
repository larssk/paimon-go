package manifest

import (
	"bytes"
	"context"
	"fmt"

	"github.com/hamba/avro/v2/ocf"

	"github.com/larssk/paimon-go/fileio"
	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/internal/pathutil"
	"github.com/larssk/paimon-go/schema"
)

const indexTypeDV = "DELETION_VECTORS_INDEX"

// DeletionVectorMeta describes the location of a single deletion vector within
// an index file. The offset and length point into the index file at
// IndexFilePath; pass them to [fileio.FileIO.ReadRange] and then to
// [deletionvector.Decode].
type DeletionVectorMeta struct {
	// IndexFilePath is the full storage path to the index file containing this DV.
	IndexFilePath string
	// Offset is the byte offset within the index file at which the DV record starts
	// (i.e. the first byte of the 4-byte total_length field).
	Offset int64
	// Length is the total byte count of the DV record starting at Offset.
	// This includes the 4-byte total_length prefix, magic, roaring bytes, and CRC.
	Length int32
	// Cardinality is the number of deleted rows, or -1 if not recorded.
	Cardinality int64
}

// ReadDeletionVectors reads the index manifest Avro file referenced by
// indexManifestName and returns a map of data file name → DeletionVectorMeta
// for all DELETION_VECTORS_INDEX entries in the manifest.
//
// Only entries whose (partition, bucket) pair appears in the provided
// ManifestEntry slice are included. Pass all entries for the current snapshot
// to get the complete DV map for a plan.
//
// Returns an empty map (not nil) when no DV entries are found.
func ReadDeletionVectors(
	ctx context.Context,
	fio fileio.FileIO,
	manifestDir string,
	indexManifestName string,
	partFields []schema.DataField,
	entries []ManifestEntry,
) (map[string]DeletionVectorMeta, error) {
	// Build a (partitionHex+bucket) set from the manifest entries so we only
	// return DVs for files that are actually in the read plan.
	type partBucket struct {
		part   string
		bucket int
	}
	relevant := make(map[partBucket]struct{}, len(entries))
	for _, e := range entries {
		key := partBucket{part: partitionHex(e.Partition), bucket: e.Bucket}
		relevant[key] = struct{}{}
	}

	p := pathutil.Join(manifestDir, indexManifestName)
	data, err := fio.ReadAll(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("index manifest: read %s: %w", indexManifestName, err)
	}

	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("index manifest: parse avro %s: %w", indexManifestName, err)
	}

	result := make(map[string]DeletionVectorMeta)

	for dec.HasNext() {
		var rec map[string]interface{}
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("index manifest: decode record: %w", err)
		}

		// Parse partition.
		var partition *binaryrow.BinaryRow
		if raw, ok := rec["_PARTITION"]; ok && raw != nil {
			if b, ok := raw.([]byte); ok && len(b) > 0 {
				brow, err := binaryrow.New(b)
				if err == nil {
					partition = brow
				}
			}
		}

		bucket := 0
		if v, ok := rec["_BUCKET"]; ok && v != nil {
			bucket = int(idxToInt64(v))
		}

		key := partBucket{part: partitionHex(partition), bucket: bucket}
		if _, ok := relevant[key]; !ok {
			continue
		}

		// Parse index file metadata.
		indexType, _ := rec["_INDEX_TYPE"].(string)
		if indexType != indexTypeDV {
			continue
		}

		fileName, _ := rec["_FILE_NAME"].(string)
		if fileName == "" {
			continue
		}

		indexFilePath := pathutil.Join(manifestDir, "..", "index", fileName)

		// Parse dv_ranges: _DELETIONS_VECTORS_RANGES is []map[string]interface{}
		dvList, _ := rec["_DELETIONS_VECTORS_RANGES"].([]interface{})
		for _, item := range dvList {
			dvRec, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			dataFileName, _ := dvRec["f0"].(string)
			offset := idxToInt64(dvRec["f1"])
			length := idxToInt32(dvRec["f2"])
			cardinality := int64(-1)
			if c, ok := dvRec["_CARDINALITY"]; ok && c != nil {
				cardinality = idxToInt64(c)
			}

			if dataFileName == "" {
				continue
			}
			result[dataFileName] = DeletionVectorMeta{
				IndexFilePath: indexFilePath,
				Offset:        offset,
				Length:        length,
				Cardinality:   cardinality,
			}
		}
	}
	if err := dec.Error(); err != nil {
		return nil, fmt.Errorf("index manifest: %w", err)
	}
	return result, nil
}

// partitionHex returns a stable string key for a partition BinaryRow.
// Nil partition (unpartitioned table) returns an empty string.
func partitionHex(p *binaryrow.BinaryRow) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%x", p.Bytes())
}

func idxToInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	return 0
}

func idxToInt32(v interface{}) int32 {
	return int32(idxToInt64(v))
}
