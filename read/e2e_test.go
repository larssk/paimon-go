//go:build e2e

// Package read_test contains end-to-end tests that read real Paimon fixtures
// written by pypaimon and verify that the Go reader produces correct output.
//
// Run with:
//
//	go test -tags e2e ./read/...
//
// Fixtures are committed under testdata/fixtures/warehouse and must be
// regenerated (and re-committed) after schema or format changes:
//
//	python3 testdata/gen_fixtures.py
package read_test

import (
	"context"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	paimon "github.com/larssk/paimon-go"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/read"
)

// fixtureWarehouse returns the absolute path to the committed fixture warehouse.
func fixtureWarehouse(t *testing.T) string {
	t.Helper()
	// This file lives at paimon-go/read/e2e_test.go; testdata is one level up.
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "testdata", "fixtures", "warehouse")
}

// readAllRows opens the given table (database.tableName) and reads all rows
// via the full scan pipeline.  It returns a flat list of Arrow record batches.
// The caller is responsible for releasing each record.
func readAllRows(t *testing.T, warehouse, database, tableName string) []arrow.RecordBatch {
	t.Helper()
	ctx := context.Background()

	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: warehouse})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	defer cat.Close()

	tbl, err := cat.GetTable(ctx, database, tableName)
	if err != nil {
		t.Fatalf("GetTable %s.%s: %v", database, tableName, err)
	}

	rb := read.NewReadBuilder(tbl)
	plan, err := rb.NewScan().Plan(ctx)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	rdr, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rdr.Release()

	var batches []arrow.RecordBatch
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		batches = append(batches, rec)
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader.Err: %v", err)
	}
	return batches
}

// totalRows sums row counts across all batches.
func totalRows(batches []arrow.RecordBatch) int64 {
	var n int64
	for _, b := range batches {
		n += b.NumRows()
	}
	return n
}

// releaseAll releases all record batches.
func releaseAll(batches []arrow.RecordBatch) {
	for _, b := range batches {
		b.Release()
	}
}

// collectInt32Col gathers all int32 values from a named column across all batches.
func collectInt32Col(t *testing.T, batches []arrow.RecordBatch, colName string) []int32 {
	t.Helper()
	var vals []int32
	for _, b := range batches {
		idx := b.Schema().FieldIndices(colName)
		if len(idx) == 0 {
			t.Fatalf("column %q not found in schema %v", colName, b.Schema())
		}
		col := b.Column(idx[0]).(*array.Int32)
		for i := 0; i < col.Len(); i++ {
			vals = append(vals, col.Value(i))
		}
	}
	return vals
}

// collectFloat64Col gathers all float64 values for a named column keyed by the
// int32 primary key column.  Returns a map[keyVal]float64.
func collectFloat64ByInt32Key(t *testing.T, batches []arrow.RecordBatch, keyCol, valCol string) map[int32]float64 {
	t.Helper()
	m := make(map[int32]float64)
	for _, b := range batches {
		kidx := b.Schema().FieldIndices(keyCol)
		vidx := b.Schema().FieldIndices(valCol)
		if len(kidx) == 0 || len(vidx) == 0 {
			t.Fatalf("columns %q or %q not found", keyCol, valCol)
		}
		keys := b.Column(kidx[0]).(*array.Int32)
		vals := b.Column(vidx[0]).(*array.Float64)
		for i := 0; i < keys.Len(); i++ {
			m[keys.Value(i)] = vals.Value(i)
		}
	}
	return m
}

// --- Tests ------------------------------------------------------------------

// TestE2E_AppendBasic verifies that a simple append-only table reads all rows.
func TestE2E_AppendBasic(t *testing.T) {
	wh := fixtureWarehouse(t)
	batches := readAllRows(t, wh, "default", "append_basic")
	defer releaseAll(batches)

	got := totalRows(batches)
	if got != 4 {
		t.Errorf("append_basic: want 4 rows, got %d", got)
	}
}

// TestE2E_PK_Deduplicate reads the pk_basic fixture (2 commits, id=2 updated in
// commit 2) and asserts the merged result has 5 rows with id=2 carrying the
// updated value of 99.0.
func TestE2E_PK_Deduplicate(t *testing.T) {
	wh := fixtureWarehouse(t)
	batches := readAllRows(t, wh, "default", "pk_basic")
	defer releaseAll(batches)

	got := totalRows(batches)
	if got != 5 {
		t.Errorf("pk_basic: want 5 rows, got %d", got)
	}

	byID := collectFloat64ByInt32Key(t, batches, "id", "value")

	if v, ok := byID[2]; !ok || v != 99.0 {
		t.Errorf("pk_basic: id=2 value: want 99.0, got %v (present=%v)", v, ok)
	}
	// Spot-check unchanged rows.
	if v := byID[1]; v != 1.0 {
		t.Errorf("pk_basic: id=1 value: want 1.0, got %v", v)
	}
	if v := byID[5]; v != 5.0 {
		t.Errorf("pk_basic: id=5 value: want 5.0, got %v", v)
	}
}

// TestE2E_PK_DeleteDropped reads the pk_delete fixture which uses an OVERWRITE
// commit to replace the table with only ids 1, 3, 4.  The merged result must
// have exactly 3 rows and id=2 must be absent.
func TestE2E_PK_DeleteDropped(t *testing.T) {
	wh := fixtureWarehouse(t)
	batches := readAllRows(t, wh, "default", "pk_delete")
	defer releaseAll(batches)

	got := totalRows(batches)
	if got != 3 {
		t.Errorf("pk_delete: want 3 rows, got %d", got)
	}

	ids := collectInt32Col(t, batches, "id")
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	want := []int32{1, 3, 4}
	if len(ids) != len(want) {
		t.Fatalf("pk_delete: id list len: want %v, got %v", want, ids)
	}
	for i, w := range want {
		if ids[i] != w {
			t.Errorf("pk_delete: ids[%d]: want %d, got %d", i, w, ids[i])
		}
	}
}

// TestE2E_PK_Partitioned reads the pk_partitioned fixture (partitioned by dt,
// 2 commits, id=1/dt='2024-01-01' updated to value=10.0) and asserts 4 total
// rows with the updated value.
func TestE2E_PK_Partitioned(t *testing.T) {
	wh := fixtureWarehouse(t)
	batches := readAllRows(t, wh, "default", "pk_partitioned")
	defer releaseAll(batches)

	got := totalRows(batches)
	if got != 4 {
		t.Errorf("pk_partitioned: want 4 rows, got %d", got)
	}

	byID := collectFloat64ByInt32Key(t, batches, "id", "value")

	if v, ok := byID[1]; !ok || v != 10.0 {
		t.Errorf("pk_partitioned: id=1 value: want 10.0, got %v (present=%v)", v, ok)
	}
}

// TestE2E_PK_Projection reads only the "id" column from pk_basic and asserts
// that the output schema contains exactly that one column and still has 5 rows.
func TestE2E_PK_Projection(t *testing.T) {
	ctx := context.Background()
	wh := fixtureWarehouse(t)

	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: wh})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	defer cat.Close()

	tbl, err := cat.GetTable(ctx, "default", "pk_basic")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}

	rb := read.NewReadBuilder(tbl).WithProjection([]string{"id"})
	plan, err := rb.NewScan().Plan(ctx)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	rdr, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rdr.Release()

	// Verify schema has only "id".
	sc := rdr.Schema()
	if sc.NumFields() != 1 || sc.Field(0).Name != "id" {
		t.Errorf("projection: want schema with only 'id', got %v", sc)
	}

	var total int64
	for rdr.Next() {
		rec := rdr.Record()
		total += rec.NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader.Err: %v", err)
	}
	if total != 5 {
		t.Errorf("projection: want 5 rows, got %d", total)
	}
}

// TestE2E_PK_Filter applies a predicate (id > 3) to pk_basic and asserts that
// only ids 4 and 5 are returned (2 rows).
func TestE2E_PK_Filter(t *testing.T) {
	ctx := context.Background()
	wh := fixtureWarehouse(t)

	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: wh})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	defer cat.Close()

	tbl, err := cat.GetTable(ctx, "default", "pk_basic")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}

	rb := read.NewReadBuilder(tbl)
	pb := rb.NewPredicateBuilder()
	gt, err := pb.GreaterThan("id", int32(3))
	if err != nil {
		t.Fatalf("GreaterThan: %v", err)
	}
	rb = rb.WithFilter(gt)

	plan, err := rb.NewScan().Plan(ctx)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	rdr, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rdr.Release()

	var ids []int32
	for rdr.Next() {
		rec := rdr.Record()
		col := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int32)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader.Err: %v", err)
	}

	for _, id := range ids {
		if id <= 3 {
			t.Errorf("filter (id>3): unexpected id=%d in result", id)
		}
	}
	if len(ids) != 2 {
		t.Errorf("filter (id>3): want 2 rows, got %d (ids=%v)", len(ids), ids)
	}
}

// TestE2E_PK_PartitionFilter applies a partition predicate on pk_partitioned
// to read only dt='2024-01-02' and asserts exactly 2 rows are returned.
func TestE2E_PK_PartitionFilter(t *testing.T) {
	ctx := context.Background()
	wh := fixtureWarehouse(t)

	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: wh})
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	defer cat.Close()

	tbl, err := cat.GetTable(ctx, "default", "pk_partitioned")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}

	rb := read.NewReadBuilder(tbl)
	pb := rb.NewPredicateBuilder()
	eq, err := pb.Equal("dt", "2024-01-02")
	if err != nil {
		t.Fatalf("Equal: %v", err)
	}
	rb = rb.WithFilter(eq)

	plan, err := rb.NewScan().Plan(ctx)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Splits) != 1 {
		t.Fatalf("partition filter must prune before reading: want 1 split, got %d", len(plan.Splits))
	}

	rdr, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rdr.Release()

	var total int64
	for rdr.Next() {
		total += rdr.Record().NumRows()
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reader.Err: %v", err)
	}
	if total != 2 {
		t.Errorf("partition filter dt='2024-01-02': want 2 rows, got %d", total)
	}
}

func TestE2E_PK_PartitionAndKeyFilter(t *testing.T) {
	ctx := context.Background()
	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: fixtureWarehouse(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	tbl, err := cat.GetTable(ctx, "default", "pk_partitioned")
	if err != nil {
		t.Fatal(err)
	}
	pb := predicate.NewBuilder(tbl.Schema.Fields)
	id, _ := pb.Equal("id", int32(1))
	dt, _ := pb.Equal("dt", "2024-01-01")
	for _, p := range []*predicate.Predicate{id, predicate.And(id, dt)} {
		rb := read.NewReadBuilder(tbl).WithFilter(p)
		plan, err := rb.NewScan().Plan(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := rb.NewRead().ToArrow(ctx, plan.Splits)
		if err != nil {
			t.Fatal(err)
		}
		if result.NumRows() != 1 {
			t.Errorf("want current id=1 row, got %d rows", result.NumRows())
		}
		result.Release()
	}
}

// compile-time check: predicate.Builder is used above.
var _ = (*predicate.Builder)(nil)
