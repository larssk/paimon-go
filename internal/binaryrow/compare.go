package binaryrow

import (
	"bytes"
	"math/big"
	"strings"
	"time"

	"github.com/apache/paimon/paimon-go/schema"
)

// Compare compares two BinaryRows field-by-field using the provided key fields
// (which must correspond to positions 0..len(fields)-1 in each row).
// Returns -1, 0, or +1. Nulls sort before non-nulls.
func Compare(a, b *BinaryRow, fields []schema.DataField) int {
	for i, f := range fields {
		aNil := a == nil || a.IsNull(i)
		bNil := b == nil || b.IsNull(i)
		if aNil && bNil {
			continue
		}
		if aNil {
			return -1
		}
		if bNil {
			return 1
		}
		c := compareField(a, b, i, f.Type)
		if c != 0 {
			return c
		}
	}
	return 0
}

func compareField(a, b *BinaryRow, i int, dt schema.DataType) int {
	upper := strings.ToUpper(dt.Type)
	switch upper {
	case "BOOLEAN", "BOOL":
		av, _ := a.GetBool(i)
		bv, _ := b.GetBool(i)
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1

	case "TINYINT", "BYTE":
		av, _ := a.GetInt8(i)
		bv, _ := b.GetInt8(i)
		return cmpInt64(int64(av), int64(bv))

	case "SMALLINT", "SHORT":
		av, _ := a.GetInt16(i)
		bv, _ := b.GetInt16(i)
		return cmpInt64(int64(av), int64(bv))

	case "INT", "INTEGER":
		av, _ := a.GetInt32(i)
		bv, _ := b.GetInt32(i)
		return cmpInt64(int64(av), int64(bv))

	case "BIGINT", "LONG":
		av, _ := a.GetInt64(i)
		bv, _ := b.GetInt64(i)
		return cmpInt64(av, bv)

	case "FLOAT", "REAL":
		av, _ := a.GetFloat32(i)
		bv, _ := b.GetFloat32(i)
		return cmpFloat64(float64(av), float64(bv))

	case "DOUBLE":
		av, _ := a.GetFloat64(i)
		bv, _ := b.GetFloat64(i)
		return cmpFloat64(av, bv)

	case "DATE":
		av, _ := a.GetDate(i)
		bv, _ := b.GetDate(i)
		return cmpTime(av, bv)

	case "TIMESTAMP", "TIMESTAMP_LTZ":
		av, _ := a.GetTimestampMillis(i)
		bv, _ := b.GetTimestampMillis(i)
		return cmpTime(av, bv)

	case "STRING", "VARCHAR", "CHAR":
		av, _ := a.GetString(i)
		bv, _ := b.GetString(i)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0

	case "BINARY", "VARBINARY", "BYTES":
		av, _ := a.GetBytes(i)
		bv, _ := b.GetBytes(i)
		return bytes.Compare(av, bv)

	case "DECIMAL":
		av, _ := a.GetDecimal(i, dt.Precision, dt.Scale)
		bv, _ := b.GetDecimal(i, dt.Precision, dt.Scale)
		return cmpRat(av, bv)

	default:
		// Fallback: lexicographic byte comparison on raw slot bytes
		av, _ := a.GetBytes(i)
		bv, _ := b.GetBytes(i)
		return bytes.Compare(av, bv)
	}
}

func cmpInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpFloat64(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func cmpTime(a, b time.Time) int {
	if a.Before(b) {
		return -1
	}
	if a.After(b) {
		return 1
	}
	return 0
}

func cmpRat(a, b *big.Rat) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return a.Cmp(b)
}
