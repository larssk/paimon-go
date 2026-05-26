package predicate_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/paimon/paimon-go/predicate"
	"github.com/apache/paimon/paimon-go/schema"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// twoColSchema returns a schema with an INT32 "id" and a VARCHAR "name".
func twoColSchema() []schema.DataField {
	return []schema.DataField{
		{ID: 0, Name: "id", Type: schema.DataType{Type: "INT", Nullable: false}},
		{ID: 1, Name: "name", Type: schema.DataType{Type: "VARCHAR", Nullable: true}},
	}
}

// buildRecord builds a single-row RecordBatch with the given id (int32) and name (string, may be "").
// If name == "" the name column is appended as null.
func buildRecord(t *testing.T, id int32, name string, nameNull bool) arrow.RecordBatch {
	t.Helper()
	alloc := memory.NewGoAllocator()

	idB := array.NewInt32Builder(alloc)
	idB.Append(id)
	idArr := idB.NewArray()
	defer idArr.Release()

	nameB := array.NewStringBuilder(alloc)
	if nameNull {
		nameB.AppendNull()
	} else {
		nameB.Append(name)
	}
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	s := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	return array.NewRecord(s, []arrow.Array{idArr, nameArr}, 1)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestEvalRow_Equal_Match(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.Equal("id", int32(7))

	rec := buildRecord(t, 7, "alice", false)
	defer rec.Release()

	if !predicate.EvalRow(p, rec, 0) {
		t.Error("EvalRow: expected true for id==7 on row with id=7")
	}
}

func TestEvalRow_Equal_NoMatch(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.Equal("id", int32(99))

	rec := buildRecord(t, 7, "alice", false)
	defer rec.Release()

	if predicate.EvalRow(p, rec, 0) {
		t.Error("EvalRow: expected false for id==99 on row with id=7")
	}
}

func TestEvalRow_Equal_String(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.Equal("name", "Series")

	rec := buildRecord(t, 1, "Series", false)
	defer rec.Release()

	if !predicate.EvalRow(p, rec, 0) {
		t.Error("EvalRow: expected true for name=='Series'")
	}

	rec2 := buildRecord(t, 2, "Standalone", false)
	defer rec2.Release()

	if predicate.EvalRow(p, rec2, 0) {
		t.Error("EvalRow: expected false for name=='Standalone' against filter name=='Series'")
	}
}

func TestEvalRow_IsNull(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.IsNull("name")

	recNull := buildRecord(t, 1, "", true)
	defer recNull.Release()
	if !predicate.EvalRow(p, recNull, 0) {
		t.Error("EvalRow: expected true for IS NULL on null name")
	}

	recNotNull := buildRecord(t, 2, "bob", false)
	defer recNotNull.Release()
	if predicate.EvalRow(p, recNotNull, 0) {
		t.Error("EvalRow: expected false for IS NULL on non-null name")
	}
}

func TestEvalRow_IsNotNull(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.IsNotNull("name")

	recNull := buildRecord(t, 1, "", true)
	defer recNull.Release()
	if predicate.EvalRow(p, recNull, 0) {
		t.Error("EvalRow: expected false for IS NOT NULL on null name")
	}

	recNotNull := buildRecord(t, 2, "bob", false)
	defer recNotNull.Release()
	if !predicate.EvalRow(p, recNotNull, 0) {
		t.Error("EvalRow: expected true for IS NOT NULL on non-null name")
	}
}

func TestEvalRow_In(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.In("name", "Series", "Movie")

	for _, name := range []string{"Series", "Movie"} {
		rec := buildRecord(t, 1, name, false)
		if !predicate.EvalRow(p, rec, 0) {
			t.Errorf("EvalRow: expected true for name IN (Series, Movie) with name=%q", name)
		}
		rec.Release()
	}

	rec := buildRecord(t, 2, "Standalone", false)
	defer rec.Release()
	if predicate.EvalRow(p, rec, 0) {
		t.Error("EvalRow: expected false for name IN (Series, Movie) with name='Standalone'")
	}
}

func TestEvalRow_NullValueNeverMatchesComparison(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	p, _ := pb.Equal("name", "anything")

	recNull := buildRecord(t, 1, "", true)
	defer recNull.Release()
	if predicate.EvalRow(p, recNull, 0) {
		t.Error("EvalRow: null value should never match an equality comparison")
	}
}

func TestEvalRow_And(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	pID, _ := pb.Equal("id", int32(5))
	pName, _ := pb.Equal("name", "Series")
	p := predicate.And(pID, pName)

	// Both match.
	rec := buildRecord(t, 5, "Series", false)
	defer rec.Release()
	if !predicate.EvalRow(p, rec, 0) {
		t.Error("EvalRow AND: expected true when both conditions match")
	}

	// Only one matches.
	rec2 := buildRecord(t, 5, "Standalone", false)
	defer rec2.Release()
	if predicate.EvalRow(p, rec2, 0) {
		t.Error("EvalRow AND: expected false when only id matches")
	}
}

func TestEvalRow_Or(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	pA, _ := pb.Equal("id", int32(1))
	pB, _ := pb.Equal("id", int32(2))
	p := predicate.Or(pA, pB)

	for _, id := range []int32{1, 2} {
		rec := buildRecord(t, id, "x", false)
		if !predicate.EvalRow(p, rec, 0) {
			t.Errorf("EvalRow OR: expected true for id=%d", id)
		}
		rec.Release()
	}
	rec := buildRecord(t, 3, "x", false)
	defer rec.Release()
	if predicate.EvalRow(p, rec, 0) {
		t.Error("EvalRow OR: expected false for id=3")
	}
}
