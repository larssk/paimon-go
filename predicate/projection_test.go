package predicate_test

import (
	"testing"

	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
)

func TestWithProjection_DoesNotStrengthenUnknownBranches(t *testing.T) {
	fields := twoColSchema()
	pb := predicate.NewBuilder(fields)
	id, _ := pb.Equal("id", int32(1))
	name, _ := pb.Equal("name", "alice")
	for _, p := range []*predicate.Predicate{
		predicate.Or(id, name),
		predicate.Not(predicate.And(id, name)),
		predicate.Or(predicate.And(id, name), name),
		predicate.Not(predicate.Or(id, name)),
	} {
		if got := predicate.WithProjection(p, fields[:1]); got != nil {
			t.Errorf("unsafe partial predicate retained: %+v", got)
		}
	}
	p := predicate.WithProjection(predicate.And(id, name), fields[:1])
	if p == nil || len(p.Children) != 1 || p.Children[0].FieldIdx != 0 {
		t.Fatalf("AND should retain known id condition: %+v", p)
	}
	if id.FieldIdx != 0 || name.FieldIdx != 1 {
		t.Fatal("original predicates were mutated")
	}
}

func TestTestByStats_NegativeIndexIsUnknown(t *testing.T) {
	p := &predicate.Predicate{Op: predicate.OpIsNull, FieldIdx: -1}
	if !predicate.TestByStats(p, manifest.SimpleStats{NullCounts: []interface{}{int64(0)}}, 1) {
		t.Fatal("unknown column must keep file")
	}
}
