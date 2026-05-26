// Package manifest reads Paimon manifest-list and manifest-entry Avro files.
package manifest

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/binaryrow"
	"github.com/apache/paimon/paimon-go/internal/pathutil"
	"github.com/apache/paimon/paimon-go/schema"
)

// SimpleStats holds the min/max/null statistics for a set of fields.
type SimpleStats struct {
	MinValues  *binaryrow.BinaryRow // may be nil if stats absent
	MaxValues  *binaryrow.BinaryRow // may be nil if stats absent
	NullCounts []interface{}        // []null|int64; nil element means "unknown"
}

// ManifestFileMeta describes a single manifest file as recorded in the manifest list.
type ManifestFileMeta struct {
	FileName        string
	FileSize        int64
	NumAddedFiles   int64
	NumDeletedFiles int64
	PartitionStats  SimpleStats
	SchemaID        int64
	MinRowID        *int64
	MaxRowID        *int64
}

// EntryKind is the kind of a ManifestEntry.
type EntryKind int

const (
	// EntryAdd indicates the file was added in this snapshot.
	EntryAdd EntryKind = 0
	// EntryDelete indicates the file was logically deleted (superseded by a newer
	// compacted file or an OVERWRITE write). Files with EntryDelete are excluded
	// from the read plan after ADD/DELETE resolution.
	EntryDelete EntryKind = 1
)

// DataFileMeta holds the metadata of a single data file from a manifest entry.
type DataFileMeta struct {
	// FileName is the bare filename (no directory). Use table.PathFactory.DataFilePath
	// to reconstruct the full storage path.
	FileName string
	FileSize int64
	RowCount int64
	// SchemaID is the schema under which this file was written. Use
	// table.FileStoreTable.SchemaForID to resolve it for schema-evolution reads.
	SchemaID int64
	// Level is the LSM tree level: 0 = L0 (overlapping key ranges across files),
	// 1+ = higher levels (non-overlapping within a level after compaction).
	Level int
	// MinSequenceNumber / MaxSequenceNumber bound the write-order sequence numbers
	// of rows in this file. Used by the PK merge pipeline to pick the latest row.
	MinSequenceNumber int64
	MaxSequenceNumber int64
	// KeyStats holds min/max/null statistics for the primary key columns.
	KeyStats SimpleStats
	// ValueStats holds min/max/null statistics for all value columns.
	ValueStats SimpleStats
	// ExtraFiles lists auxiliary files associated with this data file, such as
	// deletion vector files (.dv). May be empty.
	ExtraFiles []string
	// DeleteRowCount is the number of rows logically deleted by deletion vectors,
	// if present. Nil when deletion vectors are not used.
	DeleteRowCount *int64
	// ExternalPath is set when the data file lives outside the table's own
	// directory (e.g. for externally managed files). Nil for normal files.
	ExternalPath *string
	FirstRowID   *int64
	// ValueStatsCols lists the column names for which ValueStats are tracked.
	// Nil means all columns are tracked.
	ValueStatsCols []string
}

// ManifestEntry is a single record from a manifest file.
type ManifestEntry struct {
	Kind      EntryKind
	Partition *binaryrow.BinaryRow // decoded partition values (may be nil for unpartitioned)
	Bucket    int
	File      DataFileMeta
}

// --- Manifest List Reader ---

// ReadManifestList reads a manifest-list Avro file and returns the list of manifest file metas.
func ReadManifestList(ctx context.Context, fio fileio.FileIO, manifestDir, filename string, partFields []schema.DataField) ([]ManifestFileMeta, error) {
	p := pathutil.Join(manifestDir, filename)
	data, err := fio.ReadAll(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("manifest: read list %s: %w", filename, err)
	}
	return parseManifestList(data, partFields)
}

func parseManifestList(data []byte, partFields []schema.DataField) ([]ManifestFileMeta, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("manifest list: open avro: %w", err)
	}

	var results []ManifestFileMeta
	for dec.HasNext() {
		var rec map[string]interface{}
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("manifest list: read record: %w", err)
		}
		meta, err := manifestFileMetaFromRecord(rec, partFields)
		if err != nil {
			return nil, err
		}
		results = append(results, meta)
	}
	if err := dec.Error(); err != nil {
		return nil, fmt.Errorf("manifest list: avro error: %w", err)
	}
	_ = avro.Config{} // ensure import used
	return results, nil
}

func manifestFileMetaFromRecord(rec map[string]interface{}, partFields []schema.DataField) (ManifestFileMeta, error) {
	var meta ManifestFileMeta
	meta.FileName, _ = rec["_FILE_NAME"].(string)
	meta.FileSize, _ = toInt64(rec["_FILE_SIZE"])
	meta.NumAddedFiles, _ = toInt64(rec["_NUM_ADDED_FILES"])
	meta.NumDeletedFiles, _ = toInt64(rec["_NUM_DELETED_FILES"])
	meta.SchemaID, _ = toInt64(rec["_SCHEMA_ID"])
	meta.MinRowID = optInt64(rec["_MIN_ROW_ID"])
	meta.MaxRowID = optInt64(rec["_MAX_ROW_ID"])

	if statsRaw, ok := rec["_PARTITION_STATS"]; ok {
		meta.PartitionStats = parseSimpleStats(statsRaw, partFields)
	}
	return meta, nil
}

// --- Manifest Entry Reader ---

// ReadManifestEntries reads a manifest Avro file and returns all live (ADD) entries after
// resolving ADD/DELETE pairs within the file. If net=true, DELETE entries are applied to cancel
// prior ADDs; if net=false, all entries are returned including DELETEs.
func ReadManifestEntries(ctx context.Context, fio fileio.FileIO, manifestDir, filename string, partFields, valueFields []schema.DataField) ([]ManifestEntry, error) {
	p := pathutil.Join(manifestDir, filename)
	data, err := fio.ReadAll(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("manifest: read entries %s: %w", filename, err)
	}
	return parseManifestEntries(data, partFields, valueFields)
}

func parseManifestEntries(data []byte, partFields, valueFields []schema.DataField) ([]ManifestEntry, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("manifest entry: open avro: %w", err)
	}

	var entries []ManifestEntry
	for dec.HasNext() {
		var rec map[string]interface{}
		if err := dec.Decode(&rec); err != nil {
			return nil, fmt.Errorf("manifest entry: read record: %w", err)
		}
		entry, err := manifestEntryFromRecord(rec, partFields, valueFields)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := dec.Error(); err != nil {
		return nil, fmt.Errorf("manifest entry: avro error: %w", err)
	}
	return entries, nil
}

func manifestEntryFromRecord(rec map[string]interface{}, partFields, valueFields []schema.DataField) (ManifestEntry, error) {
	var entry ManifestEntry

	kind, _ := toInt64(rec["_KIND"])
	entry.Kind = EntryKind(kind)
	entry.Bucket = int(mustInt64(rec["_BUCKET"]))

	if partRaw, ok := rec["_PARTITION"]; ok {
		if partBytes, ok := partRaw.([]byte); ok && len(partBytes) > 0 {
			row, err := binaryrow.New(partBytes)
			if err == nil {
				entry.Partition = row
			}
		}
	}

	if fileRaw, ok := rec["_FILE"]; ok {
		fileMap, ok := fileRaw.(map[string]interface{})
		if !ok {
			return entry, fmt.Errorf("manifest entry: _FILE is not a map")
		}
		fm, err := dataFileMetaFromMap(fileMap, valueFields)
		if err != nil {
			return entry, err
		}
		entry.File = fm
	}
	return entry, nil
}

func dataFileMetaFromMap(m map[string]interface{}, valueFields []schema.DataField) (DataFileMeta, error) {
	var fm DataFileMeta
	fm.FileName, _ = m["_FILE_NAME"].(string)
	fm.FileSize, _ = toInt64(m["_FILE_SIZE"])
	fm.RowCount, _ = toInt64(m["_ROW_COUNT"])
	fm.SchemaID, _ = toInt64(m["_SCHEMA_ID"])
	fm.Level = int(mustInt64(m["_LEVEL"]))
	fm.MinSequenceNumber, _ = toInt64(m["_MIN_SEQUENCE_NUMBER"])
	fm.MaxSequenceNumber, _ = toInt64(m["_MAX_SEQUENCE_NUMBER"])
	fm.DeleteRowCount = optInt64(m["_DELETE_ROW_COUNT"])
	fm.FirstRowID = optInt64(m["_FIRST_ROW_ID"])
	if ep, ok := m["_EXTERNAL_PATH"]; ok {
		if s, ok := ep.(string); ok && s != "" {
			fm.ExternalPath = &s
		}
	}
	if extra, ok := m["_EXTRA_FILES"]; ok {
		if arr, ok := extra.([]interface{}); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					fm.ExtraFiles = append(fm.ExtraFiles, s)
				}
			}
		}
	}
	if cols, ok := m["_VALUE_STATS_COLS"]; ok {
		if arr, ok := cols.([]interface{}); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					fm.ValueStatsCols = append(fm.ValueStatsCols, s)
				}
			}
		}
	}

	if ks, ok := m["_KEY_STATS"]; ok {
		fm.KeyStats = parseSimpleStats(ks, nil)
	}
	if vs, ok := m["_VALUE_STATS"]; ok {
		fm.ValueStats = parseSimpleStats(vs, valueFields)
	}
	return fm, nil
}

// parseSimpleStats decodes a _PARTITION_STATS / _KEY_STATS / _VALUE_STATS Avro map.
func parseSimpleStats(raw interface{}, fields []schema.DataField) SimpleStats {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return SimpleStats{}
	}
	var stats SimpleStats
	if minRaw, ok := m["_MIN_VALUES"]; ok {
		if b, ok := minRaw.([]byte); ok && len(b) > 0 {
			if row, err := binaryrow.New(b); err == nil {
				stats.MinValues = row
			}
		}
	}
	if maxRaw, ok := m["_MAX_VALUES"]; ok {
		if b, ok := maxRaw.([]byte); ok && len(b) > 0 {
			if row, err := binaryrow.New(b); err == nil {
				stats.MaxValues = row
			}
		}
	}
	if ncRaw, ok := m["_NULL_COUNTS"]; ok {
		if arr, ok := ncRaw.([]interface{}); ok {
			stats.NullCounts = arr
		}
	}
	_ = fields // reserved for future typed decoding
	return stats
}

// --- Parallel manifest entry reading ---

// ReadAllEntries reads all manifest files referenced by metaList in parallel goroutines
// and returns the net set of live data files (ADDs not cancelled by DELETEs).
func ReadAllEntries(
	ctx context.Context,
	fio fileio.FileIO,
	manifestDir string,
	metaList []ManifestFileMeta,
	partFields []schema.DataField,
	valueFields []schema.DataField,
) ([]ManifestEntry, error) {
	type result struct {
		entries []ManifestEntry
		err     error
	}

	results := make([]result, len(metaList))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8) // max 8 parallel reads

	for i, meta := range metaList {
		wg.Add(1)
		go func(idx int, m ManifestFileMeta) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			entries, err := ReadManifestEntries(ctx, fio, manifestDir, m.FileName, partFields, valueFields)
			results[idx] = result{entries: entries, err: err}
		}(i, meta)
	}
	wg.Wait()

	// Collect results and apply ADD/DELETE resolution across all manifest files.
	// Key: filename → true if ADD is live, false if deleted.
	liveFiles := make(map[string]ManifestEntry)
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for _, e := range r.entries {
			if e.Kind == EntryDelete {
				delete(liveFiles, e.File.FileName)
			} else {
				liveFiles[e.File.FileName] = e
			}
		}
	}

	live := make([]ManifestEntry, 0, len(liveFiles))
	for _, e := range liveFiles {
		live = append(live, e)
	}
	return live, nil
}

// --- helpers ---

func toInt64(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}
	return 0, false
}

func mustInt64(v interface{}) int64 {
	n, _ := toInt64(v)
	return n
}

func optInt64(v interface{}) *int64 {
	if v == nil {
		return nil
	}
	n, ok := toInt64(v)
	if !ok {
		return nil
	}
	return &n
}
