package binaryrow

import (
	"encoding/binary"
	"math/bits"

	"github.com/larssk/paimon-go/schema"
)

// BucketKeyHash matches Paimon's BinaryRow.hashCode for the supported scalar
// bucket-key types. The serialized arity prefix is NOT part of the hash.
// Unsupported types/values return false so callers retain all buckets.
// Floating-point keys are deliberately unsupported: +0 and -0 compare equal
// in predicates but have different binary representations.
func BucketKeyHash(fields []schema.DataField, values []interface{}) (int32, bool) {
	if len(fields) != len(values) || len(fields) == 0 {
		return 0, false
	}
	nulls := NullBitsSize(len(fields))
	data := make([]byte, nulls+8*len(fields))
	for i, f := range fields {
		tag := schema.TypeTag(f.Type)
		switch tag {
		case "BOOLEAN", "BOOL", "TINYINT", "BYTE", "SMALLINT", "SHORT", "INT", "INTEGER", "DATE", "BIGINT", "LONG", "STRING", "VARCHAR", "CHAR":
		default:
			return 0, false
		}
		v := values[i]
		if v == nil {
			bit := i + headerSizeInBits
			data[bit/8] |= 1 << (bit % 8)
			continue
		}
		slot := data[nulls+i*8 : nulls+(i+1)*8]
		switch tag {
		case "BOOLEAN", "BOOL":
			n, ok := v.(bool)
			if !ok {
				return 0, false
			}
			if n {
				slot[0] = 1
			}
		case "TINYINT", "BYTE":
			n, ok := v.(int8)
			if !ok {
				return 0, false
			}
			slot[0] = byte(n)
		case "SMALLINT", "SHORT":
			n, ok := v.(int16)
			if !ok {
				return 0, false
			}
			binary.LittleEndian.PutUint16(slot, uint16(n))
		case "INT", "INTEGER", "DATE":
			n, ok := v.(int32)
			if !ok {
				return 0, false
			}
			binary.LittleEndian.PutUint32(slot, uint32(n))
		case "BIGINT", "LONG":
			n, ok := v.(int64)
			if !ok {
				return 0, false
			}
			binary.LittleEndian.PutUint64(slot, uint64(n))
		case "STRING", "VARCHAR", "CHAR":
			text, ok := v.(string)
			if !ok {
				return 0, false
			}
			if len(text) <= 7 {
				copy(slot, text)
				slot[7] = 0x80 | byte(len(text))
			} else {
				binary.LittleEndian.PutUint64(slot, uint64(len(data))<<32|uint64(len(text)))
				data = append(data, []byte(text)...)
				data = append(data, make([]byte, (8-len(text)%8)%8)...)
			}
		}
	}
	// Murmur3 x86_32 by aligned 32-bit words, seed 42, as used by
	// org.apache.paimon.utils.MurmurHashUtils.hashBytesByWords.
	h := uint32(42)
	for i := 0; i < len(data); i += 4 {
		k := binary.LittleEndian.Uint32(data[i:]) * 0xcc9e2d51
		k = bits.RotateLeft32(k, 15) * 0x1b873593
		h ^= k
		h = bits.RotateLeft32(h, 13)*5 + 0xe6546b64
	}
	h ^= uint32(len(data))
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return int32(h), true
}
