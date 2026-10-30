package read

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

func TestKeyFilterForRecord_RebindsOnlyCompatibleKeys(t *testing.T) {
	fields := pkSchema().Fields
	pb := predicate.NewBuilder(fields)
	id, _ := pb.Equal("id", int32(1))
	val, _ := pb.Equal("val", int64(10))
	sc := pkPhysicalArrowSchema()
	keyFields := pkSchema().PrimaryKeyFields()
	if got := keyFilterForRecord(predicate.Or(id, val), keyFields, sc); got != nil {
		t.Fatal("OR with mutable value became a key restriction")
	}
	got := keyFilterForRecord(predicate.And(id, val), keyFields, sc)
	if got == nil || len(got.Children) != 1 || got.Children[0].FieldIdx != 0 {
		t.Fatalf("AND did not retain the necessary key condition: %+v", got)
	}
	// Filter positions must be rebound to physical positions, not key order.
	shifted := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	if got := keyFilterForRecord(id, keyFields, shifted); got == nil || got.FieldIdx != 1 {
		t.Fatalf("key condition did not bind to source column: %+v", got)
	}
	keys := []schema.DataField{{Name: "ts", Type: schema.DataType{Type: "TIMESTAMP", Precision: 6}}}
	ts, _ := predicate.NewBuilder(keys).Equal("ts", int64(1000000))
	ms := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ms}}, nil)
	if got := keyFilterForRecord(ts, keys, ms); got != nil {
		t.Fatal("timestamp unit conversion must not be evaluated before projection")
	}
}
