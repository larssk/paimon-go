package read

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

// filterFields includes predicate columns even when they are not projected.
func filterFields(p *predicate.Predicate, fields []schema.DataField) []schema.DataField {
	names := make(map[string]bool)
	var visit func(*predicate.Predicate)
	visit = func(p *predicate.Predicate) {
		if p == nil {
			return
		}
		names[p.FieldName] = true
		for _, c := range p.Children {
			visit(c)
		}
	}
	visit(p)
	var result []schema.DataField
	for _, f := range fields {
		if names[f.Name] {
			result = append(result, f)
		}
	}
	return result
}

// Rebind only columns whose source representation matches the logical read
// representation. A schema-evolution cast (notably timestamp units) must not
// make a pre-merge predicate stronger than its post-merge evaluation.
func keyFilterForRecord(p *predicate.Predicate, fields []schema.DataField, sc *arrow.Schema) *predicate.Predicate {
	if p == nil {
		return nil
	}
	compatible := make(map[string]bool)
	for _, f := range fields {
		expected, err := schema.ToArrowField(f)
		indices := sc.FieldIndices(f.Name)
		if err == nil && len(indices) == 1 && arrowTypesCompatible(sc.Field(indices[0]).Type, expected.Type) {
			compatible[f.Name] = true
		}
	}
	// Retain the source column positions, even for columns not eligible for
	// pruning, by leaving their names empty rather than compacting the slice.
	projected := make([]schema.DataField, sc.NumFields())
	for i, f := range sc.Fields() {
		if compatible[f.Name] {
			projected[i].Name = f.Name
		}
	}
	return predicate.WithProjection(p, projected)
}
