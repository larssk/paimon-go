package binaryrow

import (
	"math"
	"testing"

	"github.com/larssk/paimon-go/schema"
)

// Expected hashes are emitted by testdata/BucketHashVectors.java using the
// actual Apache Paimon 1.1.1 BinaryRowWriter and BinaryRow.hashCode classes.
func TestBucketKeyHash_JavaPaimonVectors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		types  []string
		values []interface{}
		hash   int32
	}{
		{"int_zero", []string{"INT"}, []interface{}{int32(0)}, -300363099},
		{"int_one", []string{"INT"}, []interface{}{int32(1)}, 1465514398},
		{"int_negative", []string{"INT"}, []interface{}{int32(-1)}, 1133687267},
		{"int_min", []string{"INT"}, []interface{}{int32(math.MinInt32)}, 916225219},
		{"long_min", []string{"BIGINT"}, []interface{}{int64(math.MinInt64)}, 302122119},
		{"long_large", []string{"BIGINT"}, []interface{}{int64(9007199254740993)}, 1801556886},
		{"byte_negative", []string{"TINYINT"}, []interface{}{int8(-1)}, 2004758659},
		{"short_negative", []string{"SMALLINT"}, []interface{}{int16(-1)}, 2143727727},
		{"boolean_true", []string{"BOOLEAN"}, []interface{}{true}, 1465514398},
		{"null", []string{"INT"}, []interface{}{nil}, -1748325344},
		{"empty", []string{"STRING"}, []interface{}{""}, 302122119},
		{"inline", []string{"STRING"}, []interface{}{"1234567"}, 199144172},
		{"heap", []string{"STRING"}, []interface{}{"12345678"}, -391021801},
		{"unicode", []string{"STRING"}, []interface{}{"æøå😀"}, 103868050},
		{"composite", []string{"STRING", "STRING", "INT", "BIGINT", "BOOLEAN"}, []interface{}{"profile-123", "asset-987", int32(42), int64(-7), true}, 419515100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := make([]schema.DataField, len(tc.types))
			for i, typ := range tc.types {
				fields[i].Type.Type = typ
			}
			if hash, ok := BucketKeyHash(fields, tc.values); !ok || hash != tc.hash {
				t.Fatalf("want Java hash %d, got %d (supported=%v)", tc.hash, hash, ok)
			}
		})
	}
	fields, values := make([]schema.DataField, 65), make([]interface{}, 65)
	for i := range fields {
		fields[i].Type.Type = "INT"
		values[i] = int32(i)
	}
	values[64] = nil
	if hash, ok := BucketKeyHash(fields, values); !ok || hash != -2061175185 {
		t.Fatalf("multiword null bitmap: got %d (supported=%v)", hash, ok)
	}
}

func TestBucketKeyHash_UnsupportedTypesFallBack(t *testing.T) {
	for _, typ := range []string{"FLOAT", "DOUBLE", "DECIMAL", "TIMESTAMP", "ARRAY"} {
		if _, ok := BucketKeyHash([]schema.DataField{{Type: schema.DataType{Type: typ}}}, []interface{}{nil}); ok {
			t.Errorf("unsupported type %s must not enable pruning", typ)
		}
	}
	if _, ok := BucketKeyHash([]schema.DataField{{Type: schema.DataType{Type: "INT"}}}, []interface{}{int64(1)}); ok {
		t.Fatal("mismatched value types must not enable pruning")
	}
}
