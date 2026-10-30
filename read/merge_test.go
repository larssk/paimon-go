package read

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/schema"
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

// TestIntervalPartition_OverlappingL0AndL1_SameSection verifies that an L0
// file and an L1 file with overlapping key ranges land in the SAME section,
// matching paimon-python's IntervalPartition (which does not special-case
// level 0 at all — sections are formed purely from key-range overlap).
//
// This is required for correct merge-on-read: a DELETE row for a key in a
// fresh L0 file must be merged against (and allowed to suppress) a stale
// INSERT for the same key in an older L1 file. If L0 and L1 files were kept
// in separate sections, the sort-merge heap would never compare their
// entries, so the stale L1 row would survive unmerged and leak into read
// output even though the key was deleted.
func TestIntervalPartition_OverlappingL0AndL1_SameSection(t *testing.T) {
	keyFields := []schema.DataField{{Name: "id", Type: schema.DataType{Type: "INT"}}}
	files := []manifest.DataFileMeta{
		{FileName: "l0.parquet", Level: 0, KeyStats: makeKeyStats(t, 1, 10)},
		{FileName: "l1.parquet", Level: 1, KeyStats: makeKeyStats(t, 1, 10)},
	}
	sections := intervalPartition(files, keyFields)
	if len(sections) != 1 {
		t.Fatalf("want 1 section (overlapping L0+L1 must merge together), got %d", len(sections))
	}
	total := 0
	for _, run := range sections[0] {
		total += len(run)
	}
	if total != 2 {
		t.Errorf("want 2 files total in the merged section, got %d", total)
	}
}

// TestIntervalPartition_NonOverlappingL0AndL1_SeparateSections verifies that
// when key ranges genuinely do not overlap, files still form separate
// sections regardless of level.
func TestIntervalPartition_NonOverlappingL0AndL1_SeparateSections(t *testing.T) {
	keyFields := []schema.DataField{{Name: "id", Type: schema.DataType{Type: "INT"}}}
	files := []manifest.DataFileMeta{
		{FileName: "l0.parquet", Level: 0, KeyStats: makeKeyStats(t, 1, 5)},
		{FileName: "l1.parquet", Level: 1, KeyStats: makeKeyStats(t, 6, 10)},
	}
	sections := intervalPartition(files, keyFields)
	if len(sections) != 2 {
		t.Fatalf("want 2 sections (non-overlapping ranges), got %d", len(sections))
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

// TestPKMerge_DeleteInL0DropsStaleL1Insert reproduces the reported bug: a
// stale INSERT for a key lives in an older, higher-level (L1) file while a
// later DELETE for the SAME key lives in a fresh L0 file. Before the fix,
// intervalPartition isolated L0 files into their own section, so the L0
// DELETE and the L1 INSERT were never compared against each other by the
// sort-merge heap — the L1 INSERT survived unmerged and leaked into read
// output even though the key had been deleted.
//
// Correct behaviour (matching paimon-python): both files' key ranges overlap,
// so they must land in the same section; the DELETE has the higher sequence
// number and wins, and the merged result for this key must be empty.
func TestPKMerge_DeleteInL0DropsStaleL1Insert(t *testing.T) {
	// L1 file: stale INSERT for key=1, seqnum=1.
	l1Tbl := buildPKArrowTable(t,
		[]int32{1},
		[]int64{1},
		[]int8{0}, // INSERT
		[]int64{1460},
	)
	defer l1Tbl.Release()
	l1Bytes := writeParquet(t, l1Tbl)

	// L0 file: later DELETE for the same key=1, higher seqnum=2.
	l0Tbl := buildPKArrowTable(t,
		[]int32{1},
		[]int64{2},
		[]int8{3}, // DELETE
		[]int64{0},
	)
	defer l0Tbl.Release()
	l0Bytes := writeParquet(t, l0Tbl)

	mio := newMockFileIO()
	mio.register("data/l1.parquet", l1Bytes)
	mio.register("data/l0.parquet", l0Bytes)

	sch := pkSchema()
	rt := &readTable{sch: sch, io: mio}
	rb := newReadBuilderFromIface(rt, nil)

	split := makePKSplit(
		manifest.DataFileMeta{FileName: "l1.parquet", Level: 1, RowCount: 1, KeyStats: makeKeyStats(t, 1, 1)},
		manifest.DataFileMeta{FileName: "l0.parquet", Level: 0, RowCount: 1, KeyStats: makeKeyStats(t, 1, 1)},
	)

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
		t.Errorf("want 0 rows (key deleted by L0 DELETE with higher seqnum), got %d rows with vals %v", len(vals), vals)
	}
}
