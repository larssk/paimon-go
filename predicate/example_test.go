package predicate_test

import (
	"fmt"

	"github.com/apache/paimon/paimon-go/predicate"
	"github.com/apache/paimon/paimon-go/schema"
)

// fields is a small schema used across examples.
var fields = []schema.DataField{
	{ID: 0, Name: "user_id", Type: schema.DataType{Type: "BIGINT"}},
	{ID: 1, Name: "status", Type: schema.DataType{Type: "STRING"}},
	{ID: 2, Name: "amount", Type: schema.DataType{Type: "DOUBLE"}},
	{ID: 3, Name: "region", Type: schema.DataType{Type: "STRING"}},
	{ID: 4, Name: "score", Type: schema.DataType{Type: "INT"}},
}

// ExampleBuilder demonstrates the typical pattern: create a Builder from the
// table schema, build individual leaf predicates, then combine them.
//
// In practice obtain the Builder from [read.ReadBuilder.NewPredicateBuilder]
// rather than constructing it directly — that automatically uses the effective
// (possibly projected) read schema.
func ExampleBuilder() {
	b := predicate.NewBuilder(fields)

	// amount > 100.0
	gt, err := b.GreaterThan("amount", float64(100))
	if err != nil {
		panic(err)
	}

	// status == "PAID"
	eq, err := b.Equal("status", "PAID")
	if err != nil {
		panic(err)
	}

	// Combine: amount > 100.0 AND status == "PAID"
	filter := predicate.And(gt, eq)

	fmt.Println(filter.Op == predicate.OpAnd)
	// Output: true
}

// ExampleBuilder_range shows how to express a range filter (low <= col <= high)
// by combining two leaf predicates with And.
func ExampleBuilder_range() {
	b := predicate.NewBuilder(fields)

	low, err := b.GreaterOrEqual("score", int32(10))
	if err != nil {
		panic(err)
	}
	high, err := b.LessOrEqual("score", int32(100))
	if err != nil {
		panic(err)
	}

	// 10 <= score <= 100
	filter := predicate.And(low, high)

	fmt.Println(filter.Op == predicate.OpAnd)
	// Output: true
}

// ExampleBuilder_in shows how to match a column against a fixed set of values.
func ExampleBuilder_in() {
	b := predicate.NewBuilder(fields)

	// status IN ("PAID", "PENDING")
	p, err := b.In("status", "PAID", "PENDING")
	if err != nil {
		panic(err)
	}

	fmt.Println(p.Op == predicate.OpIn)
	// Output: true
}

// ExampleBuilder_isNull shows null and not-null checks.
func ExampleBuilder_isNull() {
	b := predicate.NewBuilder(fields)

	// rows where user_id IS NOT NULL
	notNull, err := b.IsNotNull("user_id")
	if err != nil {
		panic(err)
	}

	// rows where region IS NULL
	isNull, err := b.IsNull("region")
	if err != nil {
		panic(err)
	}

	// either condition
	filter := predicate.Or(notNull, isNull)

	fmt.Println(filter.Op == predicate.OpOr)
	// Output: true
}

// ExampleNot shows how to negate a predicate.
// Note: Not is applied at row-level evaluation but is skipped during
// stats-based file pruning (the file is kept conservatively).
func ExampleNot() {
	b := predicate.NewBuilder(fields)

	eu, err := b.Equal("region", "EU")
	if err != nil {
		panic(err)
	}

	// region != "EU" (via NOT)
	notEU := predicate.Not(eu)

	fmt.Println(notEU.Op == predicate.OpNot)
	// Output: true
}

// ExampleBuilder_unknownField shows the error returned when a field name is
// not present in the schema.
func ExampleBuilder_unknownField() {
	b := predicate.NewBuilder(fields)

	_, err := b.Equal("nonexistent", "value")
	fmt.Println(err)
	// Output: predicate: unknown field "nonexistent"
}
