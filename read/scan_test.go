package read

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/larssk/paimon-go/fileio"
	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
	"github.com/larssk/paimon-go/snapshot"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// stubTable satisfies the tableReader interface. All behaviour can be controlled via fields.
type stubTable struct {
	snap    *snapshot.Snapshot
	snapErr error
	sch     *schema.TableSchema
}

func (s *stubTable) LatestSnapshot(_ context.Context) (*snapshot.Snapshot, error) {
	return s.snap, s.snapErr
}

func (s *stubTable) SnapshotByID(_ context.Context, id int64) (*snapshot.Snapshot, error) {
	if s.snap != nil && s.snap.ID == id {
		return s.snap, nil
	}
	return nil, fmt.Errorf("snapshot %d not found", id)
}

func (s *stubTable) ListSnapshotIDs(_ context.Context) ([]int64, error) {
	if s.snap == nil {
		return nil, nil
	}
	return []int64{s.snap.ID}, nil
}

func (s *stubTable) GetSchema() *schema.TableSchema { return s.sch }

func (s *stubTable) ManifestDir() string { return "manifest" }

func (s *stubTable) GetIO() fileio.FileIO { return nil }

func (s *stubTable) DataFilePath(_ *binaryrow.BinaryRow, _ []schema.DataField, _ int, fileName string) string {
	return "data/" + fileName
}

// stubManifestReader satisfies the manifestReader interface.
// listResults is returned on ReadList calls; can be configured to fail per-call via listErrs.
type stubManifestReader struct {
	// listResults: successive calls to ReadList return these slices in order.
	// If only one element is provided it is returned for all calls.
	listResults [][]manifest.ManifestFileMeta
	listErrs    []error
	listCalls   int

	entriesResult []manifest.ManifestEntry
	entriesErr    error

	// captureEntryMetas records the metas passed to ReadAllEntries for assertion.
	captureEntryMetas *[]manifest.ManifestFileMeta
}

func (r *stubManifestReader) ReadList(_ context.Context, _ string, _ []schema.DataField) ([]manifest.ManifestFileMeta, error) {
	i := r.listCalls
	r.listCalls++

	var err error
	if i < len(r.listErrs) {
		err = r.listErrs[i]
	}
	if err != nil {
		return nil, err
	}

	if len(r.listResults) == 0 {
		return nil, nil
	}
	if i >= len(r.listResults) {
		return r.listResults[len(r.listResults)-1], nil
	}
	return r.listResults[i], nil
}

func (r *stubManifestReader) ReadAllEntries(_ context.Context, metas []manifest.ManifestFileMeta, _ []schema.DataField, _ []schema.DataField) ([]manifest.ManifestEntry, error) {
	if r.captureEntryMetas != nil {
		*r.captureEntryMetas = metas
	}
	return r.entriesResult, r.entriesErr
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func intSchema() *schema.TableSchema {
	return &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT"}},
			{ID: 1, Name: "val", Type: schema.DataType{Type: "BIGINT"}},
		},
	}
}

// buildIntRow builds a BinaryRow containing a single INT32 value at slot 0.
// Layout mirrors Paimon's wire format (arity prefix + null bits + slots).
func buildIntRow(v int32) *binaryrow.BinaryRow {
	arity := 1
	nullBitsSize := ((arity + 63 + 8) / 64) * 8 // 8 bytes
	data := make([]byte, 4+nullBitsSize+arity*8)
	binary.BigEndian.PutUint32(data[0:4], uint32(arity)) // arity prefix
	// slot 0: INT32 stored little-endian in 8-byte slot
	binary.LittleEndian.PutUint32(data[4+nullBitsSize:], uint32(v))
	row, err := binaryrow.New(data)
	if err != nil {
		panic(err)
	}
	return row
}

// intStats builds a SimpleStats with min=lo, max=hi for a single INT field.
func intStats(lo, hi int32) manifest.SimpleStats {
	return manifest.SimpleStats{
		MinValues: buildIntRow(lo),
		MaxValues: buildIntRow(hi),
	}
}

func makeSnap(id int64) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		ID:                id,
		BaseManifestList:  "manifest-list-base",
		DeltaManifestList: "manifest-list-delta",
	}
}

func makeEntry(bucket int, fileName string) manifest.ManifestEntry {
	return manifest.ManifestEntry{
		Kind:   manifest.EntryAdd,
		Bucket: bucket,
		File:   manifest.DataFileMeta{FileName: fileName},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestPlan_SnapshotID verifies that the snapshot ID from LatestSnapshot flows
// through unchanged into Plan.SnapshotID.
func TestPlan_SnapshotID(t *testing.T) {
	tbl := &stubTable{snap: makeSnap(42), sch: intSchema()}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: nil,
	}

	rb := newReadBuilderFromIface(tbl, mr)
	plan, err := rb.NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.SnapshotID != 42 {
		t.Errorf("want SnapshotID=42, got %d", plan.SnapshotID)
	}
}

// TestPlan_BuildsSplits verifies that entries in different buckets produce
// separate DataSplits.
func TestPlan_BuildsSplits(t *testing.T) {
	tbl := &stubTable{snap: makeSnap(1), sch: intSchema()}
	entries := []manifest.ManifestEntry{
		makeEntry(0, "file-a.parquet"),
		makeEntry(1, "file-b.parquet"),
		makeEntry(1, "file-c.parquet"),
	}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: entries,
	}

	rb := newReadBuilderFromIface(tbl, mr)
	plan, err := rb.NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Splits) != 2 {
		t.Errorf("want 2 splits (one per bucket), got %d", len(plan.Splits))
	}
	// Find the split for bucket 1 and verify it has 2 files.
	var bucket1Files int
	for _, s := range plan.Splits {
		if s.Bucket == 1 {
			bucket1Files = len(s.Files)
		}
	}
	if bucket1Files != 2 {
		t.Errorf("want 2 files in bucket-1 split, got %d", bucket1Files)
	}
}

// TestPlan_PrunesManifestFilesByPartitionStats verifies that manifest files
// whose stats prove the predicate cannot match are NOT passed to ReadAllEntries.
func TestPlan_PrunesManifestFilesByPartitionStats(t *testing.T) {
	sch := intSchema()
	tbl := &stubTable{snap: makeSnap(1), sch: sch}

	// meta1: id in [100, 200] — will NOT satisfy id == 5 → should be pruned.
	meta1 := manifest.ManifestFileMeta{FileName: "m1", NumAddedFiles: 1, PartitionStats: intStats(100, 200)}
	// meta2: id in [1, 10] — will satisfy id == 5 → kept.
	meta2 := manifest.ManifestFileMeta{FileName: "m2", NumAddedFiles: 1, PartitionStats: intStats(1, 10)}
	// meta3: id in [200, 300] — will NOT satisfy id == 5 → pruned.
	meta3 := manifest.ManifestFileMeta{FileName: "m3", NumAddedFiles: 1, PartitionStats: intStats(200, 300)}

	var capturedMetas []manifest.ManifestFileMeta
	mr := &stubManifestReader{
		listResults:       [][]manifest.ManifestFileMeta{{meta1, meta2}, {meta3}},
		entriesResult:     nil,
		captureEntryMetas: &capturedMetas,
	}

	pb := predicate.NewBuilder(sch.Fields)
	p, err := pb.Equal("id", int32(5))
	if err != nil {
		t.Fatalf("build predicate: %v", err)
	}

	rb := newReadBuilderFromIface(tbl, mr).WithFilter(p)
	_, err = rb.NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(capturedMetas) != 1 {
		t.Errorf("want 1 meta passed to ReadAllEntries (only m2), got %d", len(capturedMetas))
	}
	if len(capturedMetas) == 1 && capturedMetas[0].FileName != "m2" {
		t.Errorf("want m2 to survive pruning, got %s", capturedMetas[0].FileName)
	}
}

// TestPlan_PrunesEntriesByValueStats verifies that data-file entries are dropped
// when the predicate can disprove a match from min/max stats.
func TestPlan_PrunesEntriesByValueStats(t *testing.T) {
	sch := intSchema()
	tbl := &stubTable{snap: makeSnap(1), sch: sch}

	// Two entries: one whose id range [100,200] excludes id==5 (pruned),
	// one whose id range [1,10] includes id==5 (kept).
	entries := []manifest.ManifestEntry{
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{
			FileName: "out-of-range.parquet", RowCount: 100,
			ValueStats: intStats(100, 200),
		}},
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{
			FileName: "in-range.parquet", RowCount: 10,
			ValueStats: intStats(1, 10),
		}},
	}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: entries,
	}

	pb := predicate.NewBuilder(sch.Fields)
	p, err := pb.Equal("id", int32(5))
	if err != nil {
		t.Fatalf("build predicate: %v", err)
	}

	rb := newReadBuilderFromIface(tbl, mr).WithFilter(p)
	plan, err := rb.NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var totalFiles int
	for _, s := range plan.Splits {
		totalFiles += len(s.Files)
	}
	if totalFiles != 1 {
		t.Errorf("want 1 file after pruning, got %d", totalFiles)
	}
	if totalFiles == 1 && plan.Splits[0].Files[0].FileName != "in-range.parquet" {
		t.Errorf("want in-range.parquet to remain, got %s", plan.Splits[0].Files[0].FileName)
	}
}

// timestampSchema returns a schema with an INT32 "id" and a TIMESTAMP "ts".
func timestampSchema() *schema.TableSchema {
	return &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT"}},
			{ID: 1, Name: "ts", Type: schema.DataType{Type: "TIMESTAMP", Precision: 3}},
		},
	}
}

// buildTimestampStatsRow builds a BinaryRow with 2 fields: id (INT32) and ts (TIMESTAMP).
// This matches timestampSchema() field order so FieldIdx in predicates aligns correctly.
func buildTimestampStatsRow(id int32, tsMillis int64) *binaryrow.BinaryRow {
	arity := 2
	nullBitsSize := ((arity + 63 + 8) / 64) * 8 // 8 bytes
	data := make([]byte, 4+nullBitsSize+arity*8)
	binary.BigEndian.PutUint32(data[0:4], uint32(arity)) // arity prefix
	// slot 0: INT32 stored little-endian in 8-byte slot
	binary.LittleEndian.PutUint32(data[4+nullBitsSize:], uint32(id))
	// slot 1: int64 stored little-endian in 8-byte slot (TIMESTAMP millis)
	binary.LittleEndian.PutUint64(data[4+nullBitsSize+8:], uint64(tsMillis))
	row, err := binaryrow.New(data)
	if err != nil {
		panic(err)
	}
	return row
}

// timestampStats builds a SimpleStats with min/max for both id and ts fields.
func timestampStats(idLo, idHi int32, tsLoMillis, tsHiMillis int64) manifest.SimpleStats {
	return manifest.SimpleStats{
		MinValues: buildTimestampStatsRow(idLo, tsLoMillis),
		MaxValues: buildTimestampStatsRow(idHi, tsHiMillis),
	}
}

// TestPlan_PrunesEntriesByTimestampStats verifies that data-file entries are dropped
// when a timestamp predicate (using int64 millis) can disprove a match from min/max
// stats (which are decoded as time.Time from BinaryRow).
// This test exercises the fix for timestamp comparison type mismatch.
func TestPlan_PrunesEntriesByTimestampStats(t *testing.T) {
	sch := timestampSchema()
	tbl := &stubTable{snap: makeSnap(1), sch: sch}

	// Cutoff: 1000ms. We want ts > 1000ms.
	// Entry 1: ts in [500, 800] → max=800 < 1000, no rows can match → pruned
	// Entry 2: ts in [900, 1500] → max=1500 > 1000, might have matches → kept
	// Entry 3: ts in [100, 200] → max=200 < 1000, no rows can match → pruned
	entries := []manifest.ManifestEntry{
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{
			FileName: "too-old-1.parquet", RowCount: 100,
			ValueStats: timestampStats(0, 0, 500, 800), // id doesn't matter, ts=[500,800]
		}},
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{
			FileName: "in-range.parquet", RowCount: 10,
			ValueStats: timestampStats(0, 0, 900, 1500), // ts=[900,1500]
		}},
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{
			FileName: "too-old-2.parquet", RowCount: 50,
			ValueStats: timestampStats(0, 0, 100, 200), // ts=[100,200]
		}},
	}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: entries,
	}

	pb := predicate.NewBuilder(sch.Fields)
	// User provides int64 millis as predicate value; BinaryRow decodes stats as time.Time.
	// The fix ensures these are comparable.
	p, err := pb.GreaterThan("ts", int64(1000))
	if err != nil {
		t.Fatalf("build predicate: %v", err)
	}

	rb := newReadBuilderFromIface(tbl, mr).WithFilter(p)
	plan, err := rb.NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var totalFiles int
	for _, s := range plan.Splits {
		totalFiles += len(s.Files)
	}
	if totalFiles != 1 {
		t.Errorf("want 1 file after timestamp pruning, got %d", totalFiles)
	}
	if totalFiles == 1 && plan.Splits[0].Files[0].FileName != "in-range.parquet" {
		t.Errorf("want in-range.parquet to remain, got %s", plan.Splits[0].Files[0].FileName)
	}
}

// TestPlan_SnapshotError verifies that an error from LatestSnapshot is propagated.
func TestPlan_SnapshotError(t *testing.T) {
	sentinel := errors.New("storage unavailable")
	tbl := &stubTable{snapErr: sentinel, sch: intSchema()}
	mr := &stubManifestReader{}

	rb := newReadBuilderFromIface(tbl, mr)
	_, err := rb.NewScan().Plan(context.Background())
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("want sentinel error in chain, got: %v", err)
	}
}

// TestPlan_ManifestListError verifies that an error from ReadList is propagated.
func TestPlan_ManifestListError(t *testing.T) {
	sentinel := errors.New("avro decode failed")
	tbl := &stubTable{snap: makeSnap(1), sch: intSchema()}
	mr := &stubManifestReader{
		listErrs: []error{sentinel},
	}

	rb := newReadBuilderFromIface(tbl, mr)
	_, err := rb.NewScan().Plan(context.Background())
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("want sentinel error in chain, got: %v", err)
	}
}

// TestPlan_NeedsMerge_SetForPKTable verifies that splits for a PK table have NeedsMerge=true.
func TestPlan_NeedsMerge_SetForPKTable(t *testing.T) {
	sch := &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT"}},
			{ID: 1, Name: "val", Type: schema.DataType{Type: "BIGINT"}},
		},
		PrimaryKeys: []string{"id"},
	}
	tbl := &stubTable{snap: makeSnap(1), sch: sch}
	entries := []manifest.ManifestEntry{
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{FileName: "f.parquet", RowCount: 1}},
	}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: entries,
	}

	plan, err := newReadBuilderFromIface(tbl, mr).NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Splits) == 0 {
		t.Fatal("want at least one split")
	}
	for i, s := range plan.Splits {
		if !s.NeedsMerge {
			t.Errorf("split %d: want NeedsMerge=true for PK table, got false", i)
		}
	}
}

// TestPlan_NeedsMerge_FalseForDVEnabledPKTable verifies that a PK table with
// deletion-vectors.enabled=true produces splits with NeedsMerge=false (the DV
// path replaces sort-merge deduplication).
func TestPlan_NeedsMerge_FalseForDVEnabledPKTable(t *testing.T) {
	sch := &schema.TableSchema{
		Fields: []schema.DataField{
			{ID: 0, Name: "id", Type: schema.DataType{Type: "INT"}},
			{ID: 1, Name: "val", Type: schema.DataType{Type: "BIGINT"}},
		},
		PrimaryKeys: []string{"id"},
		Options:     map[string]string{"deletion-vectors.enabled": "true"},
	}
	tbl := &stubTable{snap: makeSnap(1), sch: sch}
	entries := []manifest.ManifestEntry{
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{FileName: "f.parquet", RowCount: 1}},
	}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: entries,
	}

	plan, err := newReadBuilderFromIface(tbl, mr).NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Splits) == 0 {
		t.Fatal("want at least one split")
	}
	for i, s := range plan.Splits {
		if s.NeedsMerge {
			t.Errorf("split %d: want NeedsMerge=false for DV-enabled PK table, got true", i)
		}
	}
}
func TestPlan_NeedsMerge_FalseForAppendTable(t *testing.T) {
	sch := intSchema() // no PrimaryKeys
	tbl := &stubTable{snap: makeSnap(1), sch: sch}
	entries := []manifest.ManifestEntry{
		{Kind: manifest.EntryAdd, Bucket: 0, File: manifest.DataFileMeta{FileName: "f.parquet", RowCount: 1}},
	}
	mr := &stubManifestReader{
		listResults:   [][]manifest.ManifestFileMeta{{}, {}},
		entriesResult: entries,
	}

	plan, err := newReadBuilderFromIface(tbl, mr).NewScan().Plan(context.Background())
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Splits) == 0 {
		t.Fatal("want at least one split")
	}
	for i, s := range plan.Splits {
		if s.NeedsMerge {
			t.Errorf("split %d: want NeedsMerge=false for append-only table, got true", i)
		}
	}
}
