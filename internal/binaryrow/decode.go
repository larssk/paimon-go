// Package binaryrow decodes Paimon's compact BinaryRow binary format.
//
// Wire format (matches Java org.apache.paimon.data.BinaryRow):
//
//	[4 bytes big-endian: arity]
//	[null_bits_size bytes: header + per-field null flags]
//	  byte 0 = RowKind (0=INSERT,1=UPDATE_BEFORE,2=UPDATE_AFTER,3=DELETE)
//	  bits 8+: one null flag per field (bit set = null)
//	[arity * 8 bytes: fixed-size slots]
//	  fixed types stored inline; variable-length types store pointer
//	[variable-length data region]
//
// String/bytes slot encoding:
//
//	inline (len <= 7): data in bytes 0–6 of slot, byte 7 = 0x80 | len
//	heap   (len >  7): int64 little-endian = (offset << 32) | length
package binaryrow

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"time"
)

// RowKind mirrors Paimon's RowKind enum.
type RowKind byte

const (
	Insert        RowKind = 0
	UpdateBefore  RowKind = 1
	UpdateAfter   RowKind = 2
	Delete        RowKind = 3
)

const headerSizeInBits = 8 // RowKind occupies the first byte of the bit-set

// BinaryRow holds raw Paimon binary row bytes and decodes fields lazily.
type BinaryRow struct {
	raw    []byte // includes the 4-byte arity prefix
	actual []byte // raw[4:]
	arity  int
}

// New wraps raw BinaryRow bytes (including the 4-byte arity prefix).
func New(data []byte) (*BinaryRow, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("binaryrow: data too short (%d bytes)", len(data))
	}
	arity := int(binary.BigEndian.Uint32(data[:4]))
	return &BinaryRow{raw: data, actual: data[4:], arity: arity}, nil
}

// Arity returns the number of fields.
func (r *BinaryRow) Arity() int { return r.arity }

// Bytes returns the raw bytes of the BinaryRow (including the 4-byte arity prefix).
// The returned slice shares the underlying array; do not modify it.
func (r *BinaryRow) Bytes() []byte { return r.raw }

// RowKind returns the row kind encoded in the first byte of the bit-set.
func (r *BinaryRow) RowKind() RowKind {
	if len(r.actual) == 0 {
		return Insert
	}
	return RowKind(r.actual[0])
}

// IsNull returns true if field i is null.
func (r *BinaryRow) IsNull(i int) bool {
	return isNullAt(r.actual, i)
}

func isNullAt(data []byte, pos int) bool {
	bitIndex := pos + headerSizeInBits
	byteIndex := bitIndex / 8
	bitPos := uint(bitIndex % 8)
	if byteIndex >= len(data) {
		return true
	}
	return (data[byteIndex]>>bitPos)&1 != 0
}

// NullBitsSize returns the number of bytes occupied by the header + null bit-set.
func NullBitsSize(arity int) int {
	return ((arity + 63 + headerSizeInBits) / 64) * 8
}

// fieldOffset returns the byte offset of field i's 8-byte slot within actual[].
func fieldOffset(arity, i int) int {
	return NullBitsSize(arity) + i*8
}

// --- Field type decoders ---

// GetBool decodes a BOOLEAN field.
func (r *BinaryRow) GetBool(i int) (bool, error) {
	off := fieldOffset(r.arity, i)
	if off >= len(r.actual) {
		return false, r.oob(i)
	}
	return r.actual[off] != 0, nil
}

// GetInt8 decodes a TINYINT field.
func (r *BinaryRow) GetInt8(i int) (int8, error) {
	off := fieldOffset(r.arity, i)
	if off >= len(r.actual) {
		return 0, r.oob(i)
	}
	return int8(r.actual[off]), nil
}

// GetInt16 decodes a SMALLINT field.
func (r *BinaryRow) GetInt16(i int) (int16, error) {
	off := fieldOffset(r.arity, i)
	if off+2 > len(r.actual) {
		return 0, r.oob(i)
	}
	return int16(binary.LittleEndian.Uint16(r.actual[off : off+2])), nil
}

// GetInt32 decodes an INT/INTEGER field.
func (r *BinaryRow) GetInt32(i int) (int32, error) {
	off := fieldOffset(r.arity, i)
	if off+4 > len(r.actual) {
		return 0, r.oob(i)
	}
	return int32(binary.LittleEndian.Uint32(r.actual[off : off+4])), nil
}

// GetInt64 decodes a BIGINT field.
func (r *BinaryRow) GetInt64(i int) (int64, error) {
	off := fieldOffset(r.arity, i)
	if off+8 > len(r.actual) {
		return 0, r.oob(i)
	}
	return int64(binary.LittleEndian.Uint64(r.actual[off : off+8])), nil
}

// GetFloat32 decodes a FLOAT field.
func (r *BinaryRow) GetFloat32(i int) (float32, error) {
	off := fieldOffset(r.arity, i)
	if off+4 > len(r.actual) {
		return 0, r.oob(i)
	}
	bits := binary.LittleEndian.Uint32(r.actual[off : off+4])
	return math.Float32frombits(bits), nil
}

// GetFloat64 decodes a DOUBLE field.
func (r *BinaryRow) GetFloat64(i int) (float64, error) {
	off := fieldOffset(r.arity, i)
	if off+8 > len(r.actual) {
		return 0, r.oob(i)
	}
	bits := binary.LittleEndian.Uint64(r.actual[off : off+8])
	return math.Float64frombits(bits), nil
}

// GetDate decodes a DATE field (stored as int32 days since epoch).
func (r *BinaryRow) GetDate(i int) (time.Time, error) {
	days, err := r.GetInt32(i)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, 0).UTC().AddDate(0, 0, int(days)), nil
}

// GetTimestampMillis decodes a TIMESTAMP field (stored as epoch millis int64).
func (r *BinaryRow) GetTimestampMillis(i int) (time.Time, error) {
	ms, err := r.GetInt64(i)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

// GetTimestampNanos decodes a TIMESTAMP(p>=7) field.
// Paimon stores these as two int32 values: millis in the lower 4 bytes, nanos-of-millis adjustment in upper 4.
func (r *BinaryRow) GetTimestampNanos(i int) (time.Time, error) {
	off := fieldOffset(r.arity, i)
	if off+8 > len(r.actual) {
		return time.Time{}, r.oob(i)
	}
	millis := int64(int32(binary.LittleEndian.Uint32(r.actual[off : off+4])))
	nanoAdj := int32(binary.LittleEndian.Uint32(r.actual[off+4 : off+8]))
	t := time.UnixMilli(millis).UTC().Add(time.Duration(nanoAdj))
	return t, nil
}

// GetBytes decodes a BINARY/VARBINARY/BYTES field (inline or heap).
func (r *BinaryRow) GetBytes(i int) ([]byte, error) {
	return r.getVarLen(i)
}

// GetString decodes a STRING/CHAR/VARCHAR field (inline or heap).
func (r *BinaryRow) GetString(i int) (string, error) {
	b, err := r.getVarLen(i)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// GetDecimal decodes a DECIMAL field.
// Paimon stores decimals unscaled as a 16-byte big-endian two's-complement integer in the heap.
// For precision <= 18, it is stored inline as int64 little-endian.
func (r *BinaryRow) GetDecimal(i int, precision, scale int) (*big.Rat, error) {
	if precision <= 18 {
		v, err := r.GetInt64(i)
		if err != nil {
			return nil, err
		}
		rat := new(big.Rat).SetFrac(
			new(big.Int).SetInt64(v),
			new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil),
		)
		return rat, nil
	}
	// heap-stored 16-byte unscaled big-endian
	b, err := r.getVarLen(i)
	if err != nil {
		return nil, err
	}
	unscaled := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		// negative: two's complement
		mask := new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8))
		unscaled.Sub(unscaled, mask)
	}
	rat := new(big.Rat).SetFrac(
		unscaled,
		new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil),
	)
	return rat, nil
}

// getVarLen decodes a variable-length field (string or bytes).
// Inline: byte 7 of the 8-byte slot has high bit set; lower 7 bits = length; data at slot[0:len].
// Heap:   int64 LE = (offset_from_row_base << 32) | length.
func (r *BinaryRow) getVarLen(i int) ([]byte, error) {
	off := fieldOffset(r.arity, i)
	if off+8 > len(r.actual) {
		return nil, r.oob(i)
	}
	slot := r.actual[off : off+8]
	// Check inline flag: high bit of byte 7
	if slot[7]&0x80 != 0 {
		length := int(slot[7] & 0x7F)
		result := make([]byte, length)
		copy(result, slot[:length])
		return result, nil
	}
	// Heap pointer: little-endian int64
	word := int64(binary.LittleEndian.Uint64(slot))
	dataOffset := int((word >> 32) & 0xFFFFFFFF)
	length := int(word & 0xFFFFFFFF)
	if dataOffset+length > len(r.actual) {
		return nil, fmt.Errorf("binaryrow: field %d heap data out of bounds (offset=%d len=%d datasize=%d)",
			i, dataOffset, length, len(r.actual))
	}
	result := make([]byte, length)
	copy(result, r.actual[dataOffset:dataOffset+length])
	return result, nil
}

func (r *BinaryRow) oob(i int) error {
	return fmt.Errorf("binaryrow: field %d out of bounds (arity=%d)", i, r.arity)
}

// GetField decodes field i as an interface{} based on the provided type tag.
// typeTag values match Paimon type names (uppercase), e.g. "INT", "BIGINT", "STRING", etc.
// For DECIMAL pass "DECIMAL:precision:scale" (handled by caller mapping — use typed methods directly
// for precision/scale control).
func GetField(r *BinaryRow, i int, typeTag string) (interface{}, error) {
	if r.IsNull(i) {
		return nil, nil
	}
	switch typeTag {
	case "BOOLEAN", "BOOL":
		return r.GetBool(i)
	case "TINYINT", "BYTE":
		return r.GetInt8(i)
	case "SMALLINT", "SHORT":
		return r.GetInt16(i)
	case "INT", "INTEGER":
		return r.GetInt32(i)
	case "BIGINT", "LONG":
		return r.GetInt64(i)
	case "FLOAT", "REAL":
		return r.GetFloat32(i)
	case "DOUBLE":
		return r.GetFloat64(i)
	case "DATE":
		return r.GetDate(i)
	case "TIMESTAMP", "TIMESTAMP_LTZ":
		return r.GetTimestampMillis(i)
	case "STRING", "VARCHAR", "CHAR":
		return r.GetString(i)
	case "BINARY", "VARBINARY", "BYTES":
		return r.GetBytes(i)
	default:
		return r.GetString(i) // fallback
	}
}
