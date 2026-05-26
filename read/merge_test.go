package read

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/schema"
)

// ---------------------------------------------------------------------------
// Helpers for PK merge tests
// ---------------------------------------------------------------------------

// pkSchema returns a PK TableSchema with fields: id INT (PK), val BIGINT.
// Physical Parquet schema for PK files also includes _SEQUENCE_NUMBER and _VALUE_KIND,
// but the logical output schema only has id and val.
func pkSchema() *schema.TableSchema {
	return &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT", Nullable: false}},
			{ID: 1, Name: "val", Type: schema.DataType{Type: "BIGINT", Nullable: true}},
		},
		PrimaryKeys: []string{"id"},
	}
}

// pkArrowSchema is the physical Arrow schema of a PK Parquet file.
func pkPhysicalArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: colSequenceNumber, Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: colValueKind, Type: arrow.PrimitiveTypes.Int8, Nullable: false},
		{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
}

// buildPKArrowTable builds a PK physical Parquet record with the given rows.
// ids, seqs, kinds, vals must all have the same length.
func buildPKArrowTable(
	t *testing.T,
	ids []int32,
	seqs []int64,
	kinds []int8,
	vals []int64,
) arrow.Table {
	t.Helper()
	alloc := memory.NewGoAllocator()
	n := len(ids)

	idB := array.NewInt32Builder(alloc)
	defer idB.Release()
	idB.AppendValues(ids, nil)
	idArr := idB.NewArray()
	defer idArr.Release()

	seqB := array.NewInt64Builder(alloc)
	defer seqB.Release()
	seqB.AppendValues(seqs, nil)
	seqArr := seqB.NewArray()
	defer seqArr.Release()

	kindB := array.NewInt8Builder(alloc)
	defer kindB.Release()
	kindB.AppendValues(kinds, nil)
	kindArr := kindB.NewArray()
	defer kindArr.Release()

	valB := array.NewInt64Builder(alloc)
	defer valB.Release()
	valid := make([]bool, n)
	for i := range vals {
		valid[i] = true
	}
	valB.AppendValues(vals, valid)
	valArr := valB.NewArray()
	defer valArr.Release()

	sch := pkPhysicalArrowSchema()
	return array.NewTableFromSlice(sch, [][]arrow.Array{{idArr}, {seqArr}, {kindArr}, {valArr}})
}

// makeKeyStats builds a SimpleStats with MinValues and MaxValues as single-INT32 BinaryRows.
func makeKeyStats(t *testing.T, minKey, maxKey int32) manifest.SimpleStats {
	t.Helper()
	return manifest.SimpleStats{
		MinValues: buildIntRow(minKey),
		MaxValues: buildIntRow(maxKey),
	}
}
func makePKSplit(files ...manifest.DataFileMeta) DataSplit {
	return DataSplit{
		Partition:  nil,
		Bucket:     0,
		Files:      files,
		NeedsMerge: true,
	}
}

// collectInt32Col collects all int32 values from column colName across all record batches
// of a RecordReader.
func collectInt32Col(t *testing.T, rr array.RecordReader, colName string) []int32 {
	t.Helper()
	var out []int32
	for rr.Next() {
		rec := rr.RecordBatch()
		idx := rec.Schema().FieldIndices(colName)
		if len(idx) == 0 {
			t.Fatalf("column %q not found in schema %v", colName, rec.Schema())
		}
		col := rec.Column(idx[0]).(*array.Int32)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
	}
	return out
}

func collectInt64Col(t *testing.T, rr array.RecordReader, colName string) []int64 {
	t.Helper()
	var out []int64
	for rr.Next() {
		rec := rr.RecordBatch()
		idx := rec.Schema().FieldIndices(colName)
		if len(idx) == 0 {
			t.Fatalf("column %q not found in schema %v", colName, rec.Schema())
		}
		col := rec.Column(idx[0]).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// intervalPartition tests
// ---------------------------------------------------------------------------

func TestIntervalPartition_EmptyFiles(t *testing.T) {
	keyFields := []schema.DataField{{Name: "id", Type: schema.DataType{Type: "INT"}}}
	sections := intervalPartition(nil, keyFields)
	if len(sections) != 0 {
		t.Errorf("want 0 sections, got %d", len(sections))
	}
}

func TestIntervalPartition_SingleL1File(t *testing.T) {
	keyFields := []schema.DataField{{Name: "id", Type: schema.DataType{Type: "INT"}}}
	files := []manifest.DataFileMeta{
		{FileName: "f1.parquet", Level: 1, KeyStats: makeKeyStats(t, 1, 10)},
	}
	sections := intervalPartition(files, keyFields)
	if len(sections) != 1 {
		t.Fatalf("want 1 section, got %d", len(sections))
	}
	if len(sections[0]) != 1 {
		t.Fatalf("want 1 run in section 0, got %d", len(sections[0]))
	}
	if len(sections[0][0]) != 1 {
		t.Fatalf("want 1 file in run 0, got %d", len(sections[0][0]))
	}
}

func TestIntervalPartition_TwoNonOverlappingL1Files(t *testing.T) {
	// Two L1 files with non-overlapping ranges (1-5, 6-10).
	// Since minKey(f2)=6 > maxKey(f1)=5, the algorithm treats them as separate sections.
	// Each section has exactly one SortedRun with one file.
	keyFields := []schema.DataField{{Name: "id", Type: schema.DataType{Type: "INT"}}}
	files := []manifest.DataFileMeta{
		{FileName: "f1.parquet", Level: 1, KeyStats: makeKeyStats(t, 1, 5)},
		{FileName: "f2.parquet", Level: 1, KeyStats: makeKeyStats(t, 6, 10)},
	}
	sections := intervalPartition(files, keyFields)
	// Total file count across all sections must equal 2.
	total := 0
	for _, section := range sections {
		for _, run := range section {
			total += len(run)
		}
	}
	if total != 2 {
		t.Errorf("want 2 files total, got %d", total)
	}
}

func TestIntervalPartition_L0FileInOwnSection(t *testing.T) {
	keyFields := []schema.DataField{{Name: "id", Type: schema.DataType{Type: "INT"}}}
	files := []manifest.DataFileMeta{
		{FileName: "l0.parquet", Level: 0, KeyStats: makeKeyStats(t, 1, 10)},
		{FileName: "l1.parquet", Level: 1, KeyStats: makeKeyStats(t, 1, 10)},
	}
	sections := intervalPartition(files, keyFields)
	// L1 goes into one section, L0 goes into another.
	if len(sections) != 2 {
		t.Fatalf("want 2 sections (L1 section + L0 section), got %d", len(sections))
	}
}

// ---------------------------------------------------------------------------
// sortMergeReader / full PK pipeline tests
// ---------------------------------------------------------------------------

// TestPKMerge_Deduplicate: two versions of the same key — latest seqnum wins.
func TestPKMerge_Deduplicate(t *testing.T) {
	// File contains key=1 twice: seqnum 1 (val=10) and seqnum 2 (val=99).
	// After dedup only val=99 should be emitted.
	tbl := buildPKArrowTable(t,
		[]int32{1, 1},
		[]int64{1, 2},   // sequence numbers
		[]int8{0, 0},    // both INSERT
		[]int64{10, 99}, // vals
	)
	defer tbl.Release()
	pqBytes := writeParquet(t, tbl)

	mio := newMockFileIO()
	mio.register("data/dedup.parquet", pqBytes)

	sch := pkSchema()
	rt := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(rt, nil)

	split := makePKSplit(manifest.DataFileMeta{
		FileName: "dedup.parquet",
		Level:    0,
		RowCount: 2,
		KeyStats: makeKeyStats(t, 1, 1),
	})

	rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rr.Release()

	vals := collectInt64Col(t, rr, "val")
	if err := rr.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if len(vals) != 1 {
		t.Fatalf("want 1 row after dedup, got %d", len(vals))
	}
	if vals[0] != 99 {
		t.Errorf("want val=99 (latest), got %d", vals[0])
	}
}

// TestPKMerge_DeleteDropped: INSERT followed by DELETE for same key → no output.
func TestPKMerge_DeleteDropped(t *testing.T) {
	tbl := buildPKArrowTable(t,
		[]int32{1, 1},
		[]int64{1, 2},
		[]int8{0, 3}, // INSERT then DELETE
		[]int64{10, 0},
	)
	defer tbl.Release()
	pqBytes := writeParquet(t, tbl)

	mio := newMockFileIO()
	mio.register("data/delete.parquet", pqBytes)

	sch := pkSchema()
	rt := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(rt, nil)

	split := makePKSplit(manifest.DataFileMeta{
		FileName: "delete.parquet",
		Level:    0,
		RowCount: 2,
		KeyStats: makeKeyStats(t, 1, 1),
	})

	rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rr.Release()

	vals := collectInt64Col(t, rr, "val")
	if err := rr.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if len(vals) != 0 {
		t.Errorf("want 0 rows (key deleted), got %d rows with vals %v", len(vals), vals)
	}
}

// TestPKMerge_MultipleKeys: three distinct keys each with two versions across two files.
func TestPKMerge_MultipleKeys(t *testing.T) {
	// File 1: keys 1,2,3 with seqnum=1 (sorted within file — L0 files are sorted by key)
	tbl1 := buildPKArrowTable(t,
		[]int32{1, 2, 3},
		[]int64{1, 1, 1},
		[]int8{0, 0, 0},
		[]int64{10, 20, 30},
	)
	defer tbl1.Release()
	pq1 := writeParquet(t, tbl1)

	// File 2: same keys with seqnum=2 (newer versions)
	tbl2 := buildPKArrowTable(t,
		[]int32{1, 2, 3},
		[]int64{2, 2, 2},
		[]int8{0, 0, 0},
		[]int64{100, 200, 300},
	)
	defer tbl2.Release()
	pq2 := writeParquet(t, tbl2)

	mio := newMockFileIO()
	mio.register("data/multi1.parquet", pq1)
	mio.register("data/multi2.parquet", pq2)

	sch := pkSchema()
	rt := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(rt, nil)

	split := makePKSplit(
		manifest.DataFileMeta{FileName: "multi1.parquet", Level: 0, RowCount: 3, KeyStats: makeKeyStats(t, 1, 3)},
		manifest.DataFileMeta{FileName: "multi2.parquet", Level: 0, RowCount: 3, KeyStats: makeKeyStats(t, 1, 3)},
	)

	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatalf("ToArrow: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 3 {
		t.Fatalf("want 3 rows (one per key), got %d", result.NumRows())
	}
}

// TestPKMerge_UpdateBeforeDropped: UPDATE_BEFORE (kind=1) rows must be dropped.
func TestPKMerge_UpdateBeforeDropped(t *testing.T) {
	// Key=1: UPDATE_BEFORE (kind=1, seqnum=1) then UPDATE_AFTER (kind=2, seqnum=2).
	// Expected: one row with val=99.
	tbl := buildPKArrowTable(t,
		[]int32{1, 1},
		[]int64{1, 2},
		[]int8{1, 2}, // UPDATE_BEFORE then UPDATE_AFTER
		[]int64{10, 99},
	)
	defer tbl.Release()
	pqBytes := writeParquet(t, tbl)

	mio := newMockFileIO()
	mio.register("data/upd.parquet", pqBytes)

	sch := pkSchema()
	rt := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(rt, nil)

	split := makePKSplit(manifest.DataFileMeta{
		FileName: "upd.parquet",
		Level:    0,
		RowCount: 2,
		KeyStats: makeKeyStats(t, 1, 1),
	})

	rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatalf("ToArrowReader: %v", err)
	}
	defer rr.Release()

	vals := collectInt64Col(t, rr, "val")
	if err := rr.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if len(vals) != 1 {
		t.Fatalf("want 1 row, got %d", len(vals))
	}
	if vals[0] != 99 {
		t.Errorf("want val=99, got %d", vals[0])
	}
}
