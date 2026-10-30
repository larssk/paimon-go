// Package predicate provides filter predicates for Paimon table scans.
//
// Predicates are used at two levels:
//  1. Stats-based pruning: applied against SimpleStats (min/max/null-count per file)
//     to skip entire data files during planning.
//  2. Row-level filtering: applied in the read path (pushed down to the Parquet reader).
package predicate

import (
	"bytes"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/schema"
)

// Op is a predicate operator.
type Op int

const (
	OpEqual          Op = iota // field == value
	OpNotEqual                 // field != value
	OpLessThan                 // field < value
	OpLessOrEqual              // field <= value
	OpGreaterThan              // field > value
	OpGreaterOrEqual           // field >= value
	OpIsNull                   // field IS NULL
	OpIsNotNull                // field IS NOT NULL
	OpIn                       // field IN (values...)
	OpAnd                      // logical AND of Children
	OpOr                       // logical OR of Children
	OpNot                      // logical NOT of Children[0]
)

// Predicate is a filter expression over a table's fields.
//
// A Predicate is either a leaf (Op is one of the comparison operators, FieldIdx
// and Literals are set) or a compound node (Op is OpAnd / OpOr / OpNot,
// Children are set). The tree is built by [Builder] methods and the [And],
// [Or], [Not] combinators; callers do not normally construct Predicate values
// directly.
//
// FieldIdx is the zero-based index of the field in the read/projected schema
// and must be rebound with [WithProjection] whenever the column set changes.
type Predicate struct {
	Op        Op
	FieldIdx  int
	FieldName string
	// TypeTag as returned by schema.TypeTag (used for BinaryRow decoding).
	TypeTag  string
	Literals []interface{}
	// For AND / OR / NOT: child predicates.
	Children []*Predicate
}

// And combines two or more predicates with logical AND.
// Returns a predicate that is true only when all children are true.
// Passing a single predicate is valid and returns an OpAnd wrapper around it.
func And(preds ...*Predicate) *Predicate {
	return &Predicate{Op: OpAnd, Children: preds}
}

// Or combines two or more predicates with logical OR.
// Returns a predicate that is true when at least one child is true.
func Or(preds ...*Predicate) *Predicate {
	return &Predicate{Op: OpOr, Children: preds}
}

// Not negates a predicate.
// Note: NOT is not applied during stats-based file pruning (conservative: the
// file is always kept). It is applied at row-level evaluation by [EvalRow].
func Not(p *Predicate) *Predicate {
	return &Predicate{Op: OpNot, Children: []*Predicate{p}}
}

// Builder constructs leaf predicates against a given schema.
//
// Obtain a Builder from [read.ReadBuilder.NewPredicateBuilder] (preferred) or
// directly via [NewBuilder]. The Builder is bound to a specific set of fields;
// if the field set changes (e.g. after projection) call [WithProjection] to
// rebind FieldIdx values.
//
// The value argument to comparison methods must match the Go type that
// corresponds to the Paimon field type:
//
//	Paimon type          Go value type
//	INT / INTEGER        int32
//	BIGINT               int64
//	FLOAT                float32
//	DOUBLE               float64
//	BOOLEAN              bool
//	STRING / VARCHAR     string
//	DATE                 int32  (days since 1970-01-01)
//	TIMESTAMP            int64  (epoch milliseconds)
//
// Passing the wrong type does not panic at construction time but will produce
// incorrect results during stats-based pruning and row-level evaluation.
type Builder struct {
	fields []schema.DataField
	// index map: field name → position in fields slice
	index map[string]int
}

// NewBuilder creates a Builder bound to the given field slice.
// Prefer [read.ReadBuilder.NewPredicateBuilder] which binds automatically to
// the effective (possibly projected) read schema.
func NewBuilder(fields []schema.DataField) *Builder {
	idx := make(map[string]int, len(fields))
	for i, f := range fields {
		idx[f.Name] = i
	}
	return &Builder{fields: fields, index: idx}
}

func (b *Builder) field(name string) (int, schema.DataField, error) {
	i, ok := b.index[name]
	if !ok {
		return 0, schema.DataField{}, fmt.Errorf("predicate: unknown field %q", name)
	}
	return i, b.fields[i], nil
}

func (b *Builder) leaf(op Op, name string, literals ...interface{}) (*Predicate, error) {
	i, f, err := b.field(name)
	if err != nil {
		return nil, err
	}
	return &Predicate{
		Op:        op,
		FieldIdx:  i,
		FieldName: name,
		TypeTag:   schema.TypeTag(f.Type),
		Literals:  literals,
	}, nil
}

// Equal builds a predicate that matches rows where name == value.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) Equal(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpEqual, name, value)
}

// NotEqual builds a predicate that matches rows where name != value.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) NotEqual(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpNotEqual, name, value)
}

// LessThan builds a predicate that matches rows where name < value.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) LessThan(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpLessThan, name, value)
}

// LessOrEqual builds a predicate that matches rows where name <= value.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) LessOrEqual(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpLessOrEqual, name, value)
}

// GreaterThan builds a predicate that matches rows where name > value.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) GreaterThan(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpGreaterThan, name, value)
}

// GreaterOrEqual builds a predicate that matches rows where name >= value.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) GreaterOrEqual(name string, value interface{}) (*Predicate, error) {
	return b.leaf(OpGreaterOrEqual, name, value)
}

// IsNull builds a predicate that matches rows where name IS NULL.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) IsNull(name string) (*Predicate, error) {
	return b.leaf(OpIsNull, name)
}

// IsNotNull builds a predicate that matches rows where name IS NOT NULL.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) IsNotNull(name string) (*Predicate, error) {
	return b.leaf(OpIsNotNull, name)
}

// In builds a predicate that matches rows where name is equal to any of values.
// Returns an error if name is not present in the builder's field set.
func (b *Builder) In(name string, values ...interface{}) (*Predicate, error) {
	return b.leaf(OpIn, name, values...)
}

// --- Stats-based file pruning ---

// ColumnStats describes scalar bounds in the same logical representation used
// by predicates. Nil bounds or NullCount mean unknown. It can describe a whole
// file or a Parquet row group without converting statistics to BinaryRows.
type ColumnStats struct {
	Min, Max  interface{}
	NullCount *int64
	RowCount  int64
}

// TestByStats returns false if the predicate is guaranteed NOT to match any row
// in a file with the given stats. Returns true if the file may contain matching rows.
func TestByStats(p *Predicate, stats manifest.SimpleStats, rowCount int64) bool {
	return TestByColumnStats(p, func(leaf *Predicate) ColumnStats {
		idx := leaf.FieldIdx
		result := ColumnStats{RowCount: rowCount}
		if idx < 0 {
			return result
		}
		if idx < len(stats.NullCounts) {
			if n, ok := stats.NullCounts[idx].(int64); ok {
				result.NullCount = &n
			}
		}
		result.Min = getStatField(stats.MinValues, idx, leaf.TypeTag)
		result.Max = getStatField(stats.MaxValues, idx, leaf.TypeTag)
		return result
	})
}

// TestByColumnStats conservatively evaluates a predicate using lazily supplied
// column statistics. It returns false only when a match can be disproved.
// The lookup must return unknown statistics for missing/unsupported columns.
func TestByColumnStats(p *Predicate, lookup func(*Predicate) ColumnStats) bool {
	if p == nil {
		return true
	}
	switch p.Op {
	case OpAnd:
		for _, c := range p.Children {
			if !TestByColumnStats(c, lookup) {
				return false
			}
		}
		return true
	case OpOr:
		for _, c := range p.Children {
			if TestByColumnStats(c, lookup) {
				return true
			}
		}
		return false
	case OpNot:
		// Conservative: can't negate stats-based checks safely; keep the file.
		return true
	}

	stats := lookup(p)
	nullCount, rowCount := stats.NullCount, stats.RowCount

	if p.Op == OpIsNull {
		return nullCount == nil || *nullCount > 0
	}
	if p.Op == OpIsNotNull {
		return nullCount == nil || rowCount == 0 || *nullCount < rowCount
	}

	minVal, maxVal := stats.Min, stats.Max
	if minVal == nil || maxVal == nil {
		return true // no stats — keep file
	}
	if nullCount != nil && rowCount > 0 && *nullCount == rowCount {
		return true // all nulls — keep file conservatively
	}

	switch p.Op {
	case OpEqual:
		if len(p.Literals) == 0 {
			return true
		}
		cmpMin := compare(p.Literals[0], minVal)
		cmpMax := compare(p.Literals[0], maxVal)
		return cmpMin >= 0 && cmpMax <= 0
	case OpNotEqual:
		if len(p.Literals) == 0 {
			return true
		}
		// Only skip if all values equal the literal (min == max == literal)
		return !(compare(minVal, maxVal) == 0 && compare(p.Literals[0], minVal) == 0)
	case OpLessThan:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], minVal) > 0
	case OpLessOrEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], minVal) >= 0
	case OpGreaterThan:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], maxVal) < 0
	case OpGreaterOrEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], maxVal) <= 0
	case OpIn:
		for _, lit := range p.Literals {
			cmpMin := compare(lit, minVal)
			cmpMax := compare(lit, maxVal)
			if cmpMin >= 0 && cmpMax <= 0 {
				return true
			}
		}
		return false
	}
	return true
}

func getStatField(row *binaryrow.BinaryRow, idx int, typeTag string) interface{} {
	if row == nil || idx < 0 || idx >= row.Arity() || row.IsNull(idx) {
		return nil
	}
	var v interface{}
	var err error
	switch typeTag {
	case "DATE":
		// Predicates and Arrow DATE values use epoch days, not time.Time.
		v, err = row.GetInt32(idx)
	case "TIMESTAMP", "TIMESTAMP_LTZ":
		// TypeTag does not carry timestamp precision, so the BinaryRow and
		// Arrow units cannot be reconciled safely here. Keep the file.
		return nil
	case "BOOLEAN", "BOOL", "TINYINT", "BYTE", "SMALLINT", "SHORT", "INT", "INTEGER", "BIGINT", "LONG", "FLOAT", "REAL", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "VARBINARY", "BYTES":
		v, err = binaryrow.GetField(row, idx, typeTag)
	default:
		return nil // never interpret unsupported statistics as strings
	}
	if err != nil {
		return nil
	}
	switch v := v.(type) {
	case float32:
		if math.IsNaN(float64(v)) {
			return nil
		}
	case float64:
		if math.IsNaN(v) {
			return nil
		}
	}
	return v
}

// compare performs a generic comparison between two values.
// Returns negative if a < b, 0 if a == b, positive if a > b.
func compare(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case bool:
		bv, _ := b.(bool)
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	case []byte:
		bv, _ := b.([]byte)
		return bytes.Compare(av, bv)
	case int8:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case int16:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case int32:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case int64:
		bv, _ := toComparableInt(b)
		return compareInt64(av, bv)
	case int:
		bv, _ := toComparableInt(b)
		return compareInt64(int64(av), bv)
	case float32:
		bv, _ := toComparableFloat(b)
		if float64(av) < bv {
			return -1
		}
		if float64(av) > bv {
			return 1
		}
		return 0
	case float64:
		bv, _ := toComparableFloat(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, _ := b.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	}
	return 0
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func toComparableInt(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}
	return 0, false
}

func toComparableFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int64:
		return float64(x), true
	}
	return 0, false
}

// WithProjection rebinds predicate leaf FieldIdx values to match a projected field slice.
// Missing fields are treated as unknown: AND may retain its known children, but
// OR and NOT are discarded if removing a child could exclude matching rows.
// A nil result means no restriction can safely be applied to this field set.
func WithProjection(p *Predicate, projectedFields []schema.DataField) *Predicate {
	if p == nil {
		return nil
	}
	idx := make(map[string]int, len(projectedFields))
	for i, f := range projectedFields {
		idx[f.Name] = i
	}
	result, _ := rebind(p, idx)
	return result
}

func rebind(p *Predicate, idx map[string]int) (*Predicate, bool) {
	if p == nil {
		return nil, false
	}
	switch p.Op {
	case OpAnd, OpOr, OpNot:
		children := make([]*Predicate, 0, len(p.Children))
		complete := true
		for _, c := range p.Children {
			rc, known := rebind(c, idx)
			complete = complete && known
			if p.Op == OpOr && rc == nil {
				return nil, false
			}
			if rc != nil {
				children = append(children, rc)
			}
		}
		if len(children) == 0 || (p.Op == OpNot && !complete) {
			return nil, false
		}
		return &Predicate{Op: p.Op, Children: children}, complete
	default:
		newIdx, ok := idx[p.FieldName]
		if !ok {
			return nil, false
		}
		clone := *p
		clone.FieldIdx = newIdx
		return &clone, true
	}
}

// TestByKeyRange tests a lexicographic primary-key interval conservatively.
// Unlike value statistics, key bounds are whole tuples, not independent column
// minima/maxima. A suffix column is bounded only while all preceding columns
// have identical endpoints. Missing or incompatible bounds keep the file.
func TestByKeyRange(p *Predicate, stats manifest.SimpleStats, fields []schema.DataField) bool {
	min, max := stats.MinValues, stats.MaxValues
	if min == nil || max == nil || min.Arity() != len(fields) || max.Arity() != len(fields) {
		return true
	}
	// A full-key equality is a point lookup candidate. Compare the entire
	// tuple so boundary suffixes can prune even when the first key varies.
	if point := equalityPoint(p, fields); point != nil {
		compareBound := func(row *binaryrow.BinaryRow) (int, bool) {
			for i, f := range fields {
				v := getStatField(row, i, schema.TypeTag(f.Type))
				if v == nil {
					return 0, false
				}
				switch v.(type) {
				case bool, int8, int16, int32, int64, float32, float64, string, []byte:
				default:
					return 0, false
				}
				if cmp := compare(point[i], v); cmp != 0 {
					return cmp, true
				}
			}
			return 0, true
		}
		if cmp, ok := compareBound(min); ok && cmp < 0 {
			return false
		}
		if cmp, ok := compareBound(max); ok && cmp > 0 {
			return false
		}
	}
	bounded := 0
	for i, f := range fields {
		lo := getStatField(min, i, schema.TypeTag(f.Type))
		hi := getStatField(max, i, schema.TypeTag(f.Type))
		if lo == nil || hi == nil {
			break
		}
		switch lo.(type) {
		case bool, int8, int16, int32, int64, float32, float64, string, []byte:
		default:
			return true
		}
		bounded = i + 1
		if compare(lo, hi) != 0 {
			break
		}
	}
	// Null counts in key metadata are not needed for range pruning.
	stats.NullCounts = nil
	return TestByStats(WithProjection(p, fields[:bounded]), stats, 0)
}

func equalityPoint(p *Predicate, fields []schema.DataField) []interface{} {
	values := make(map[string]interface{})
	var visit func(*Predicate)
	visit = func(p *Predicate) {
		if p == nil {
			return
		}
		if p.Op == OpAnd {
			for _, c := range p.Children {
				visit(c)
			}
		} else if p.Op == OpEqual && len(p.Literals) == 1 {
			values[p.FieldName] = p.Literals[0]
		}
	}
	visit(p)
	point := make([]interface{}, len(fields))
	for i, f := range fields {
		v, ok := values[f.Name]
		if !ok || v == nil {
			return nil
		}
		switch v.(type) {
		case bool, int8, int16, int32, int64, float32, float64, string, []byte:
		default:
			return nil
		}
		point[i] = v
	}
	return point
}

// --- Row-level evaluation ---

// EvalRow evaluates a predicate against a single row of an Arrow RecordBatch.
// p.FieldIdx must be bound to the column indices of rec (i.e. the read/projected schema).
func EvalRow(p *Predicate, rec arrow.RecordBatch, row int) bool {
	if p == nil {
		return true
	}
	switch p.Op {
	case OpAnd:
		for _, c := range p.Children {
			if !EvalRow(c, rec, row) {
				return false
			}
		}
		return true
	case OpOr:
		for _, c := range p.Children {
			if EvalRow(c, rec, row) {
				return true
			}
		}
		return false
	case OpNot:
		if len(p.Children) == 0 {
			return true
		}
		return !EvalRow(p.Children[0], rec, row)
	}

	// Leaf predicate — bounds check first.
	if p.FieldIdx < 0 || p.FieldIdx >= int(rec.NumCols()) {
		return true // unknown column: conservative keep
	}
	col := rec.Column(p.FieldIdx)

	if p.Op == OpIsNull {
		return col.IsNull(row)
	}
	if p.Op == OpIsNotNull {
		return col.IsValid(row)
	}

	// For comparison ops a null value never matches.
	if col.IsNull(row) {
		return false
	}
	val := arrowValue(col, row)

	switch p.Op {
	case OpEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], val) == 0
	case OpNotEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(p.Literals[0], val) != 0
	case OpLessThan:
		if len(p.Literals) == 0 {
			return true
		}
		// val < literal  ↔  compare(val, literal) < 0  ↔  compare(literal, val) > 0
		return compare(val, p.Literals[0]) < 0
	case OpLessOrEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(val, p.Literals[0]) <= 0
	case OpGreaterThan:
		if len(p.Literals) == 0 {
			return true
		}
		// val > literal  ↔  compare(val, literal) > 0
		return compare(val, p.Literals[0]) > 0
	case OpGreaterOrEqual:
		if len(p.Literals) == 0 {
			return true
		}
		return compare(val, p.Literals[0]) >= 0
	case OpIn:
		for _, lit := range p.Literals {
			if compare(lit, val) == 0 {
				return true
			}
		}
		return false
	}
	return true
}

// arrowValue extracts the Go value at position row from an Arrow array.
// Returns nil for null entries (callers should check IsNull first).
func arrowValue(col arrow.Array, row int) interface{} {
	if col.IsNull(row) {
		return nil
	}
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(row)
	case *array.Int8:
		return c.Value(row)
	case *array.Int16:
		return c.Value(row)
	case *array.Int32:
		return c.Value(row)
	case *array.Int64:
		return c.Value(row)
	case *array.Uint8:
		return c.Value(row)
	case *array.Uint16:
		return c.Value(row)
	case *array.Uint32:
		return c.Value(row)
	case *array.Uint64:
		return c.Value(row)
	case *array.Float32:
		return c.Value(row)
	case *array.Float64:
		return c.Value(row)
	case *array.String:
		return c.Value(row)
	case *array.LargeString:
		return c.Value(row)
	case *array.Date32:
		return int32(c.Value(row))
	case *array.Date64:
		return int64(c.Value(row))
	case *array.Timestamp:
		return int64(c.Value(row))
	default:
		return nil
	}
}
