// Package deletionvector decodes Paimon deletion vector records.
//
// A deletion vector encodes which rows in a Parquet data file have been
// logically deleted, as a compressed RoaringBitmap of row positions (0-based).
// The read pipeline uses this to filter out deleted rows before returning
// record batches to the caller.
//
// # Index file layout
//
// Deletion vectors are stored in index files under <table>/index/. Each index
// file begins with a 1-byte version (0x01) followed by concatenated DV records.
// The byte offset and length of each record are stored in the index manifest.
//
// # DV record format (BitmapDeletionVector32)
//
//	[4 bytes, big-endian int32]  total_length  ← byte count of (magic + roaring_bytes)
//	[4 bytes, big-endian int32]  magic         ← 1581511376 (0x5E2D3AB0)
//	[total_length - 4 bytes]    RoaringBitmap32 in standard Roaring serialisation
//	[4 bytes, big-endian int32]  CRC32         ← skipped on read
//
// The offset stored in DeletionVectorMeta points to the first byte of total_length.
package deletionvector

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

const (
	// magicV1 is the magic number for BitmapDeletionVector32 (Paimon v1).
	magicV1 = int32(1581511376)

	// headerSize is the byte count of [total_length] + [magic].
	headerSize = 8
	// crcSize is the byte count of the trailing CRC32 field.
	crcSize = 4
)

// Decode decodes a BitmapDeletionVector32 record from data.
//
// data must contain exactly the bytes for one DV record starting at its first
// byte (i.e. the 4-byte total_length field). Typically the caller reads
// DeletionVectorMeta.Length bytes from the index file at DeletionVectorMeta.Offset.
//
// Returns ErrUnsupportedMagic if the magic number indicates a 64-bit bitmap
// (Bitmap64DeletionVector) — the caller should treat the file as having no
// deletions and log a warning.
func Decode(data []byte) (*roaring.Bitmap, error) {
	if len(data) < headerSize {
		return nil, fmt.Errorf("deletionvector: record too short (%d bytes)", len(data))
	}

	totalLength := int32(binary.BigEndian.Uint32(data[0:4]))
	if totalLength < 4 {
		return nil, fmt.Errorf("deletionvector: invalid total_length %d", totalLength)
	}

	magic := int32(binary.BigEndian.Uint32(data[4:8]))
	if magic != magicV1 {
		return nil, &ErrUnsupportedMagic{Magic: magic}
	}

	// Roaring bytes: data[8 : 4+totalLength]
	// (total_length covers magic(4) + roaring_bytes, so roaring_bytes = total_length-4)
	roaringEnd := 4 + int(totalLength)
	if len(data) < roaringEnd {
		return nil, fmt.Errorf("deletionvector: data truncated: need %d bytes, have %d", roaringEnd, len(data))
	}
	roaringBytes := data[headerSize:roaringEnd]

	bm := roaring.New()
	if _, err := bm.FromBuffer(roaringBytes); err != nil {
		return nil, fmt.Errorf("deletionvector: deserialize roaring bitmap: %w", err)
	}
	return bm, nil
}

// ErrUnsupportedMagic is returned when the DV record uses an unsupported
// magic number (e.g. Bitmap64DeletionVector).
type ErrUnsupportedMagic struct {
	Magic int32
}

func (e *ErrUnsupportedMagic) Error() string {
	return fmt.Sprintf("deletionvector: unsupported magic number %d (only BitmapDeletionVector32 is supported)", e.Magic)
}
