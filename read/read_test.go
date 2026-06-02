package read

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/larssk/paimon-go/fileio"
	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
	"github.com/larssk/paimon-go/snapshot"
)

// ---------------------------------------------------------------------------
// Test doubles for TableRead tests
// ---------------------------------------------------------------------------

// mockFileIO serves pre-registered in-memory file contents keyed by path.
type mockFileIO struct {
	files map[string][]byte
}

func newMockFileIO() *mockFileIO { return &mockFileIO{files: make(map[string][]byte)} }

func (m *mockFileIO) register(path string, data []byte) { m.files[path] = data }

func (m *mockFileIO) Open(_ context.Context, path string) (io.ReadCloser, error) {
	data, ok := m.files[path]
	if !ok {
		return nil, fmt.Errorf("mockFileIO: file not found: %s", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockFileIO) Close() error { return nil }

func (m *mockFileIO) ReadAll(ctx context.Context, path string) ([]byte, error) {
	rc, err := m.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (m *mockFileIO) List(_ context.Context, _ string) ([]fileio.FileStatus, error) {
	return nil, nil
}

func (m *mockFileIO) Exists(_ context.Context, _ string) (bool, error) { return false, nil }

// readTable is a stub Table for TableRead tests.
type readTable struct {
	sch *schema.TableSchema
	io  fileio.FileIO
}

func (r *readTable) LatestSnapshot(_ context.Context) (*snapshot.Snapshot, error) {
	return makeSnap(1), nil
}
func (r *readTable) SnapshotByID(_ context.Context, _ int64) (*snapshot.Snapshot, error) {
	return makeSnap(1), nil
}
func (r *readTable) ListSnapshotIDs(_ context.Context) ([]int64, error) {
	return []int64{1}, nil
}
func (r *readTable) GetSchema() *schema.TableSchema { return r.sch }
func (r *readTable) ManifestDir() string            { return "manifest" }
func (r *readTable) GetIO() fileio.FileIO           { return r.io }
func (r *readTable) DataFilePath(_ *binaryrow.BinaryRow, _ []schema.DataField, _ int, fileName string) string {
	return "data/" + fileName
}

// ---------------------------------------------------------------------------
// Parquet helpers
// ---------------------------------------------------------------------------

// writeParquet writes an arrow.Table to a Parquet file in memory and returns the bytes.
func writeParquet(t *testing.T, tbl arrow.Table) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(tbl.Schema(), &buf, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("pqarrow.NewFileWriter: %v", err)
	}
	if err := writer.WriteTable(tbl, 65536); err != nil {
		t.Fatalf("writer.WriteTable: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}
	return buf.Bytes()
}

// buildArrowTable builds a simple two-column (id INT32, val INT64) Arrow table
// with the provided id and val slices.
func buildArrowTable(t *testing.T, ids []int32, vals []int64) arrow.Table {
	t.Helper()
	alloc := memory.NewGoAllocator()

	idBldr := array.NewInt32Builder(alloc)
	defer idBldr.Release()
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	valBldr := array.NewInt64Builder(alloc)
	defer valBldr.Release()
	valBldr.AppendValues(vals, nil)
	valArr := valBldr.NewArray()
	defer valArr.Release()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	return array.NewTableFromSlice(arrowSchema, [][]arrow.Array{{idArr}, {valArr}})
}

// paimonSchema returns a TableSchema matching the two-column Arrow schema above.
func paimonSchema() *schema.TableSchema {
	return &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT", Nullable: true}},
			{ID: 1, Name: "val", Type: schema.DataType{Type: "BIGINT", Nullable: true}},
		},
	}
}

// makeFileSplit builds a single-file DataSplit that points at the given path in mockFileIO.
func makeFileSplit(fileName string) DataSplit {
	return DataSplit{
		Partition: nil,
		Bucket:    0,
		Files: []manifest.DataFileMeta{
			{FileName: fileName, RowCount: 3},
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestToArrow_EmptySplits verifies that reading zero splits returns an empty
// arrow.Table whose schema matches the table schema.
func TestToArrow_EmptySplits(t *testing.T) {
	sch := paimonSchema()
	tbl := &readTable{sch: sch, io: newMockFileIO()}
	rb := newReadBuilderFromIface(tbl, nil)

	result, err := rb.NewRead().ToArrow(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NumRows() != 0 {
		t.Errorf("want 0 rows, got %d", result.NumRows())
	}
	if result.NumCols() != 2 {
		t.Errorf("want 2 columns, got %d", result.NumCols())
	}
}

// TestToArrow_ReadsRows verifies that a Parquet file served by mockFileIO is
// decoded correctly and the row count and values are preserved.
func TestToArrow_ReadsRows(t *testing.T) {
	ids := []int32{1, 2, 3}
	vals := []int64{10, 20, 30}

	arrowTbl := buildArrowTable(t, ids, vals)
	defer arrowTbl.Release()
	parquetBytes := writeParquet(t, arrowTbl)

	mio := newMockFileIO()
	mio.register("data/rows.parquet", parquetBytes)

	sch := paimonSchema()
	tbl := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(tbl, nil)

	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("rows.parquet")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 3 {
		t.Errorf("want 3 rows, got %d", result.NumRows())
	}

	// Verify first column values.
	col := result.Column(0)
	chunk := col.Data().Chunks()[0]
	idCol, ok := chunk.(*array.Int32)
	if !ok {
		t.Fatalf("column 0: want *array.Int32, got %T", chunk)
	}
	for i, want := range ids {
		if got := idCol.Value(i); got != want {
			t.Errorf("row %d id: want %d, got %d", i, want, got)
		}
	}
}

// TestToArrow_Projection verifies that when a column projection is set, only
// the requested columns appear in the output schema and data.
func TestToArrow_Projection(t *testing.T) {
	ids := []int32{1, 2, 3}
	vals := []int64{10, 20, 30}

	arrowTbl := buildArrowTable(t, ids, vals)
	defer arrowTbl.Release()
	parquetBytes := writeParquet(t, arrowTbl)

	mio := newMockFileIO()
	mio.register("data/proj.parquet", parquetBytes)

	sch := paimonSchema()
	tbl := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(tbl, nil).WithProjection([]string{"id"})

	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("proj.parquet")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	if result.NumCols() != 1 {
		t.Errorf("want 1 column after projection, got %d", result.NumCols())
	}
	if result.Schema().Field(0).Name != "id" {
		t.Errorf("want column name 'id', got %q", result.Schema().Field(0).Name)
	}
	if result.NumRows() != 3 {
		t.Errorf("want 3 rows, got %d", result.NumRows())
	}
}

// TestToArrow_MissingColumnIsNull verifies that a column present in the table
// schema but absent from the Parquet file is returned as a null-filled array.
func TestToArrow_MissingColumnIsNull(t *testing.T) {
	// Write a Parquet file with only the "id" column.
	alloc := memory.NewGoAllocator()
	idBldr := array.NewInt32Builder(alloc)
	defer idBldr.Release()
	idBldr.AppendValues([]int32{7, 8, 9}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	oneColSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)
	oneColTable := array.NewTableFromSlice(oneColSchema, [][]arrow.Array{{idArr}})
	defer oneColTable.Release()
	parquetBytes := writeParquet(t, oneColTable)

	mio := newMockFileIO()
	mio.register("data/missing-col.parquet", parquetBytes)

	// Table schema expects both "id" and "val".
	sch := paimonSchema()
	tbl := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(tbl, nil)

	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("missing-col.parquet")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	if result.NumCols() != 2 {
		t.Errorf("want 2 columns (id + null val), got %d", result.NumCols())
	}
	if result.NumRows() != 3 {
		t.Errorf("want 3 rows, got %d", result.NumRows())
	}

	// The second column ("val") should be entirely null.
	valCol := result.Column(1)
	for _, chunk := range valCol.Data().Chunks() {
		for i := 0; i < chunk.Len(); i++ {
			if chunk.IsValid(i) {
				t.Errorf("val[%d] should be null but is valid", i)
			}
		}
	}
}

// TestToArrow_FilterApplied verifies that a row-level filter predicate removes
// non-matching rows from the output, even when all rows are in the same file
// (i.e. stats-based pruning alone would not help).
func TestToArrow_FilterApplied(t *testing.T) {
	// Three rows: ids 1, 2, 3. Only id==2 should pass the filter.
	arrowTbl := buildArrowTable(t, []int32{1, 2, 3}, []int64{10, 20, 30})
	defer arrowTbl.Release()
	parquetBytes := writeParquet(t, arrowTbl)

	mio := newMockFileIO()
	mio.register("data/filter.parquet", parquetBytes)

	sch := paimonSchema()
	tbl := &readTable{sch: sch, io: mio}

	pb := predicate.NewBuilder(sch.Fields)
	p, err := pb.Equal("id", int32(2))
	if err != nil {
		t.Fatalf("build predicate: %v", err)
	}

	rb := newReadBuilderFromIface(tbl, nil).WithFilter(p)
	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("filter.parquet")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 1 {
		t.Fatalf("want 1 row after filter (id==2), got %d", result.NumRows())
	}

	// Verify the surviving row has id=2 and val=20.
	idChunk := result.Column(0).Data().Chunks()[0].(*array.Int32)
	if idChunk.Value(0) != 2 {
		t.Errorf("want id=2, got %d", idChunk.Value(0))
	}
	valChunk := result.Column(1).Data().Chunks()[0].(*array.Int64)
	if valChunk.Value(0) != 20 {
		t.Errorf("want val=20, got %d", valChunk.Value(0))
	}
}

// TestToArrow_TimestampUnitCast verifies that a Parquet file written with
// timestamp[ms] is transparently cast to timestamp[us] when the table schema
// declares the column as TIMESTAMP(6) (which maps to timestamp[us]).
// This covers the panic observed in production when the file unit differs from
// the schema unit.
func TestToArrow_TimestampUnitCast(t *testing.T) {
	alloc := memory.NewGoAllocator()

	// Build a Parquet file with a timestamp[ms] column.
	msType := arrow.FixedWidthTypes.Timestamp_ms
	fileSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "ts", Type: msType, Nullable: true},
	}, nil)

	idBldr := array.NewInt32Builder(alloc)
	defer idBldr.Release()
	idBldr.AppendValues([]int32{1, 2, 3}, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	tsBldr := array.NewTimestampBuilder(alloc, msType.(*arrow.TimestampType))
	defer tsBldr.Release()
	// Three timestamps in milliseconds: 1000ms, 2000ms, 3000ms
	tsBldr.AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
	tsArr := tsBldr.NewArray()
	defer tsArr.Release()

	fileTbl := array.NewTableFromSlice(fileSchema, [][]arrow.Array{{idArr}, {tsArr}})
	defer fileTbl.Release()
	parquetBytes := writeParquet(t, fileTbl)

	mio := newMockFileIO()
	mio.register("data/ts.parquet", parquetBytes)

	// Table schema declares ts as TIMESTAMP(6) → timestamp[us].
	sch := &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT", Nullable: true}},
			{ID: 1, Name: "ts", Type: schema.DataType{Type: "TIMESTAMP", Precision: 6, Nullable: true}},
		},
	}
	tbl := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(tbl, nil)

	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("ts.parquet")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 3 {
		t.Errorf("want 3 rows, got %d", result.NumRows())
	}

	// The ts column should now be timestamp[us].
	tsCol := result.Column(1)
	chunk := tsCol.Data().Chunks()[0]
	tsTyped, ok := chunk.(*array.Timestamp)
	if !ok {
		t.Fatalf("ts column: want *array.Timestamp, got %T", chunk)
	}
	if tsTyped.DataType().(*arrow.TimestampType).Unit != arrow.Microsecond {
		t.Errorf("want timestamp unit=us, got %v", tsTyped.DataType())
	}
	// 1000ms = 1_000_000us, 2000ms = 2_000_000us, 3000ms = 3_000_000us
	wantUs := []arrow.Timestamp{1_000_000, 2_000_000, 3_000_000}
	for i, want := range wantUs {
		if got := tsTyped.Value(i); got != want {
			t.Errorf("row %d: want ts=%d us, got %d", i, want, got)
		}
	}
}

// TestToArrowReader_DeletionVector verifies that rows marked as deleted in a
// deletion vector are filtered out from the output.
//
// Setup: a Parquet file with 4 rows (ids 0,1,2,3). A deletion vector marks
// rows at positions 1 and 3 as deleted. Expected output: ids 0 and 2.
func TestToArrowReader_DeletionVector(t *testing.T) {
	// Build a Parquet file with 4 rows.
	arrowTbl := buildArrowTable(t,
		[]int32{0, 1, 2, 3},
		[]int64{10, 11, 12, 13},
	)
	defer arrowTbl.Release()
	pqBytes := writeParquet(t, arrowTbl)

	// Build a DV record marking row positions 1 and 3 as deleted.
	dvBytes := buildDVRecord(t, 1, 3)

	// Index file: 1-byte version (0x01) + DV record at offset 1.
	indexFileBytes := append([]byte{0x01}, dvBytes...)

	fio := newMockFileIO()
	fio.register("data/test.parquet", pqBytes)
	fio.register("index/dv-index", indexFileBytes)

	tbl := &readTable{sch: paimonSchema(), io: fio}
	rb := newReadBuilderFromIface(tbl, &stubManifestReader{})

	dvOffset := int64(1) // after the 1-byte version prefix
	split := DataSplit{
		Bucket: 0,
		Files:  []manifest.DataFileMeta{{FileName: "test.parquet", RowCount: 4}},
		DeletionFiles: map[string]manifest.DeletionVectorMeta{
			"test.parquet": {
				IndexFilePath: "index/dv-index",
				Offset:        dvOffset,
				Length:        int32(len(dvBytes)),
				Cardinality:   2,
			},
		},
	}

	reader, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer reader.Release()

	// Collect all output rows.
	var gotIDs []int32
	for reader.Next() {
		rec := reader.RecordBatch()
		col := rec.Column(0).(*array.Int32)
		for i := 0; i < int(rec.NumRows()); i++ {
			gotIDs = append(gotIDs, col.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}

	wantIDs := []int32{0, 2}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("want ids %v, got %v", wantIDs, gotIDs)
	}
	for i, want := range wantIDs {
		if gotIDs[i] != want {
			t.Errorf("row %d: want id=%d, got %d", i, want, gotIDs[i])
		}
	}
}

// buildDVRecord builds a valid BitmapDeletionVector32 record for the given row positions.
func buildDVRecord(t *testing.T, positions ...uint32) []byte {
	t.Helper()
	bm := roaring.BitmapOf(positions...)
	roaringBytes, err := bm.ToBytes()
	if err != nil {
		t.Fatalf("bitmap.ToBytes: %v", err)
	}
	totalLength := int32(4 + len(roaringBytes)) // magic(4) + roaring
	buf := make([]byte, 4+4+len(roaringBytes)+4)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(buf[4:8], 1581511376) // magicV1
	copy(buf[8:], roaringBytes)
	// CRC left as zero (skipped on decode)
	return buf
}

// ---------------------------------------------------------------------------
// Filter + Projection rebasing tests
// ---------------------------------------------------------------------------

// fourColSchema returns a schema with 4 columns: id (INT), name (VARCHAR), value (BIGINT), status (VARCHAR).
func fourColSchema() *schema.TableSchema {
	return &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT", Nullable: true}},
			{ID: 1, Name: "name", Type: schema.DataType{Type: "VARCHAR", Nullable: true}},
			{ID: 2, Name: "value", Type: schema.DataType{Type: "BIGINT", Nullable: true}},
			{ID: 3, Name: "status", Type: schema.DataType{Type: "VARCHAR", Nullable: true}},
		},
	}
}

// buildFourColTable builds a 4-column Arrow table with the given data.
func buildFourColTable(t *testing.T, ids []int32, names []string, values []int64, statuses []string) arrow.Table {
	t.Helper()
	alloc := memory.NewGoAllocator()

	idBldr := array.NewInt32Builder(alloc)
	defer idBldr.Release()
	idBldr.AppendValues(ids, nil)
	idArr := idBldr.NewArray()
	defer idArr.Release()

	nameBldr := array.NewStringBuilder(alloc)
	defer nameBldr.Release()
	nameBldr.AppendValues(names, nil)
	nameArr := nameBldr.NewArray()
	defer nameArr.Release()

	valBldr := array.NewInt64Builder(alloc)
	defer valBldr.Release()
	valBldr.AppendValues(values, nil)
	valArr := valBldr.NewArray()
	defer valArr.Release()

	statusBldr := array.NewStringBuilder(alloc)
	defer statusBldr.Release()
	statusBldr.AppendValues(statuses, nil)
	statusArr := statusBldr.NewArray()
	defer statusArr.Release()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	return array.NewTableFromSlice(arrowSchema, [][]arrow.Array{{idArr}, {nameArr}, {valArr}, {statusArr}})
}

// TestToArrow_FilterRebasedWithProjection verifies that predicates are correctly
// rebased to projected schema indices when WithProjection is used.
//
// Scenario:
//   - Full schema: [id(0), name(1), value(2), status(3)]
//   - Predicate: status == "active" (FieldIdx=3 in full schema)
//   - Projection: [value, status] → projected schema: [value(0), status(1)]
//
// Without rebasing the filter, it would look up FieldIdx=3 in the projected record
// which doesn't exist (only 2 columns), causing wrong results or panics.
// With rebasing, FieldIdx is remapped to 1 (status's position in projected schema).
func TestToArrow_FilterRebasedWithProjection(t *testing.T) {
	// Build a Parquet file with 4 rows:
	// Row 0: id=1, name="alice", value=100, status="active"
	// Row 1: id=2, name="bob",   value=200, status="inactive"
	// Row 2: id=3, name="carol", value=300, status="active"
	// Row 3: id=4, name="dave",  value=400, status="pending"
	arrowTbl := buildFourColTable(t,
		[]int32{1, 2, 3, 4},
		[]string{"alice", "bob", "carol", "dave"},
		[]int64{100, 200, 300, 400},
		[]string{"active", "inactive", "active", "pending"},
	)
	defer arrowTbl.Release()
	parquetBytes := writeParquet(t, arrowTbl)

	mio := newMockFileIO()
	mio.register("data/four-col.parquet", parquetBytes)

	sch := fourColSchema()
	tbl := &readTable{sch: sch, io: mio}

	// Build predicate on "status" which is at FieldIdx=3 in the full schema.
	pb := predicate.NewBuilder(sch.Fields)
	p, err := pb.Equal("status", "active")
	if err != nil {
		t.Fatalf("build predicate: %v", err)
	}

	// Project only [value, status]. In the projected schema:
	// - value is at index 0
	// - status is at index 1
	// The filter must be rebased so it looks up index 1, not index 3.
	rb := newReadBuilderFromIface(tbl, nil).
		WithProjection([]string{"value", "status"}).
		WithFilter(p)

	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("four-col.parquet")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	// Should have 2 rows (id=1 and id=3 have status="active")
	if result.NumRows() != 2 {
		t.Fatalf("want 2 rows after filter (status=='active'), got %d", result.NumRows())
	}

	// Verify we only have 2 columns in the result (projection applied).
	if result.NumCols() != 2 {
		t.Fatalf("want 2 columns after projection, got %d", result.NumCols())
	}

	// Verify column names.
	if result.Schema().Field(0).Name != "value" {
		t.Errorf("want column 0 to be 'value', got %q", result.Schema().Field(0).Name)
	}
	if result.Schema().Field(1).Name != "status" {
		t.Errorf("want column 1 to be 'status', got %q", result.Schema().Field(1).Name)
	}

	// Verify the surviving rows have value=100, value=300 (corresponding to status="active").
	valChunk := result.Column(0).Data().Chunks()[0].(*array.Int64)
	wantValues := []int64{100, 300}
	for i, want := range wantValues {
		if valChunk.Value(i) != want {
			t.Errorf("row %d: want value=%d, got %d", i, want, valChunk.Value(i))
		}
	}

	// Verify status column values are all "active".
	statusChunk := result.Column(1).Data().Chunks()[0].(*array.String)
	for i := 0; i < int(result.NumRows()); i++ {
		if statusChunk.Value(i) != "active" {
			t.Errorf("row %d: want status='active', got %q", i, statusChunk.Value(i))
		}
	}
}
