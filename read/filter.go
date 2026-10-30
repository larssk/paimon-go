package read

import (
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
