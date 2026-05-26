package read

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/schema"
	"github.com/apache/paimon/paimon-go/snapshot"
)

// ---------------------------------------------------------------------------
// Stream-specific test doubles
// ---------------------------------------------------------------------------

// streamTable extends stubTable with controllable snapshot ID lists and
// per-ID snapshot reads, needed by TableStream.
type streamTable struct {
	stubTable
	// snapIDs is the sequence of ID lists returned on successive ListSnapshotIDs calls.
	// If exhausted the last element is repeated.
	snapIDs    [][]int64
	snapIDCall int
	// snaps holds snapshots keyed by ID for SnapshotByID.
	snaps map[int64]*snapshot.Snapshot
}

func (s *streamTable) ListSnapshotIDs(_ context.Context) ([]int64, error) {
	i := s.snapIDCall
	s.snapIDCall++
	if i >= len(s.snapIDs) {
		i = len(s.snapIDs) - 1
	}
	return s.snapIDs[i], nil
}

func (s *streamTable) SnapshotByID(_ context.Context, id int64) (*snapshot.Snapshot, error) {
	if snap, ok := s.snaps[id]; ok {
		return snap, nil
	}
	return nil, errors.New("snapshot not found")
}

// streamManifestReader is a manifestReader where ReadList returns different
// slices per call, keyed by the filename (= DeltaManifestList of the snapshot).
// ReadAllEntries returns entries for all metas passed in, looked up by FileName.
type streamManifestReader struct {
	// listsByFile maps manifest-list filename → manifest file metas.
	listsByFile map[string][]manifest.ManifestFileMeta
	// entriesByMetaFile maps ManifestFileMeta.FileName → manifest entries.
	entriesByMetaFile map[string][]manifest.ManifestEntry
}

func (r *streamManifestReader) ReadList(_ context.Context, filename string, _ []schema.DataField) ([]manifest.ManifestFileMeta, error) {
	return r.listsByFile[filename], nil
}

func (r *streamManifestReader) ReadAllEntries(_ context.Context, metas []manifest.ManifestFileMeta, _ []schema.DataField, _ []schema.DataField) ([]manifest.ManifestEntry, error) {
	var out []manifest.ManifestEntry
	for _, m := range metas {
		out = append(out, r.entriesByMetaFile[m.FileName]...)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func appendSnap(id int64, deltaName string) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		ID:                id,
		CommitKind:        snapshot.CommitAppend,
		BaseManifestList:  "manifest-list-base",
		DeltaManifestList: deltaName,
	}
}

func compactSnap(id int64, deltaName string) *snapshot.Snapshot {
	return &snapshot.Snapshot{
		ID:                id,
		CommitKind:        snapshot.CommitCompact,
		BaseManifestList:  "manifest-list-base",
		DeltaManifestList: deltaName,
	}
}

func addEntry(bucket int, fileName string) manifest.ManifestEntry {
	return manifest.ManifestEntry{
		Kind:   manifest.EntryAdd,
		Bucket: bucket,
		File:   manifest.DataFileMeta{FileName: fileName, RowCount: 1},
	}
}

// simpleMR builds a streamManifestReader where each delta manifest list
// "delta-N" maps to a single meta "m-delta-N" whose entries are provided.
func simpleMR(snapIDs []int64, deltaSets [][]manifest.ManifestEntry) *streamManifestReader {
	mr := &streamManifestReader{
		listsByFile:       make(map[string][]manifest.ManifestFileMeta),
		entriesByMetaFile: make(map[string][]manifest.ManifestEntry),
	}
	for i, id := range snapIDs {
		metaName := fmt.Sprintf("m-delta-%d", id)
		listName := fmt.Sprintf("delta-%d", id)
		mr.listsByFile[listName] = []manifest.ManifestFileMeta{{FileName: metaName, NumAddedFiles: int64(len(deltaSets[i]))}}
		mr.entriesByMetaFile[metaName] = deltaSets[i]
	}
	return mr
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestTableStream_StartingFromLatest_SkipsExistingData verifies that when
// StartingFromLatest is used, the stream records the current snapshot ID and
// does not return splits for it — only for subsequent new snapshots.
func TestTableStream_StartingFromLatest_SkipsExistingData(t *testing.T) {
	snap5 := appendSnap(5, "delta-5")
	snap6 := appendSnap(6, "delta-6")

	tbl := &streamTable{
		stubTable: stubTable{snap: snap5, sch: intSchema()},
		// First call (init): [5]. Second call (poll): [5,6].
		snapIDs: [][]int64{{5}, {5, 6}},
		snaps:   map[int64]*snapshot.Snapshot{5: snap5, 6: snap6},
	}
	mr := simpleMR(
		[]int64{5, 6},
		[][]manifest.ManifestEntry{
			{},                          // delta-5: not read (skipped as starting point)
			{addEntry(0, "f6.parquet")}, // delta-6: new file
		},
	)

	sb := newStreamReadBuilderFromIface(tbl, mr).
		WithStartingFrom(StartingFromLatest).
		WithPollInterval(0)
	stream := sb.NewStream()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	batch, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch.SnapshotID != 6 {
		t.Errorf("want snapshotID=6, got %d", batch.SnapshotID)
	}
	if len(batch.Splits) != 1 {
		t.Errorf("want 1 split, got %d", len(batch.Splits))
	}
	if stream.LastSnapshotID() != 6 {
		t.Errorf("want lastID=6, got %d", stream.LastSnapshotID())
	}
}

// TestTableStream_StartingFromEarliest_ReplayAll verifies that
// StartingFromEarliest starts from the very first snapshot.
func TestTableStream_StartingFromEarliest_ReplayAll(t *testing.T) {
	snap1 := appendSnap(1, "delta-1")
	snap2 := appendSnap(2, "delta-2")

	tbl := &streamTable{
		stubTable: stubTable{snap: snap2, sch: intSchema()},
		snapIDs:   [][]int64{{1, 2}},
		snaps:     map[int64]*snapshot.Snapshot{1: snap1, 2: snap2},
	}
	mr := simpleMR(
		[]int64{1, 2},
		[][]manifest.ManifestEntry{
			{addEntry(0, "f1.parquet")},
			{addEntry(0, "f2.parquet")},
		},
	)

	sb := newStreamReadBuilderFromIface(tbl, mr).
		WithStartingFrom(StartingFromEarliest).
		WithPollInterval(0)
	stream := sb.NewStream()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	b1, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if b1.SnapshotID != 1 {
		t.Errorf("want snapshotID=1, got %d", b1.SnapshotID)
	}
	if len(b1.Splits) != 1 {
		t.Errorf("snap1: want 1 split, got %d", len(b1.Splits))
	}

	b2, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if b2.SnapshotID != 2 {
		t.Errorf("want snapshotID=2, got %d", b2.SnapshotID)
	}
	if len(b2.Splits) != 1 {
		t.Errorf("snap2: want 1 split, got %d", len(b2.Splits))
	}
}

// TestTableStream_DetectsNewSnapshot verifies that after consuming snapshot 5,
// the next call to Next() returns splits for snapshot 6 when it appears.
func TestTableStream_DetectsNewSnapshot(t *testing.T) {
	snap5 := appendSnap(5, "delta-5")
	snap6 := appendSnap(6, "delta-6")

	tbl := &streamTable{
		stubTable: stubTable{snap: snap5, sch: intSchema()},
		snapIDs:   [][]int64{{5}, {5}, {5, 6}},
		snaps:     map[int64]*snapshot.Snapshot{5: snap5, 6: snap6},
	}
	mr := simpleMR(
		[]int64{5, 6},
		[][]manifest.ManifestEntry{
			{},
			{addEntry(0, "f6.parquet")},
		},
	)

	sb := newStreamReadBuilderFromIface(tbl, mr).
		WithStartingFrom(StartingFromLatest).
		WithPollInterval(0)
	stream := sb.NewStream()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	batch, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch.SnapshotID != 6 {
		t.Errorf("want snapshotID=6, got %d", batch.SnapshotID)
	}
	if len(batch.Splits) == 0 {
		t.Error("want at least 1 split")
	}
}

// TestTableStream_SkipsAlreadySeenSnapshots verifies that snapshots older than
// lastID are never returned again even if they appear in the ID list.
func TestTableStream_SkipsAlreadySeenSnapshots(t *testing.T) {
	snap5 := appendSnap(5, "delta-5")
	snap6 := appendSnap(6, "delta-6")
	snap7 := appendSnap(7, "delta-7")

	tbl := &streamTable{
		stubTable: stubTable{snap: snap5, sch: intSchema()},
		snapIDs:   [][]int64{{5}, {5, 6, 7}},
		snaps:     map[int64]*snapshot.Snapshot{5: snap5, 6: snap6, 7: snap7},
	}
	mr := simpleMR(
		[]int64{5, 6, 7},
		[][]manifest.ManifestEntry{
			{},
			{addEntry(0, "f6.parquet")},
			{addEntry(0, "f7.parquet")},
		},
	)

	sb := newStreamReadBuilderFromIface(tbl, mr).
		WithStartingFrom(StartingFromLatest).
		WithPollInterval(0)
	stream := sb.NewStream()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	b1, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	if b1.SnapshotID != 6 {
		t.Errorf("want snapshotID=6, got %d", b1.SnapshotID)
	}

	b2, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if b2.SnapshotID != 7 {
		t.Errorf("want snapshotID=7, got %d", b2.SnapshotID)
	}
}

// TestTableStream_SkipsCompactionSnapshots verifies that a COMPACT snapshot
// between two APPEND snapshots is silently skipped — its lastID is advanced
// but no batch is returned to the caller, so the caller never sees
// the compacted (rewritten) files as new data.
func TestTableStream_SkipsCompactionSnapshots(t *testing.T) {
	snap5 := appendSnap(5, "delta-5")
	snap6 := compactSnap(6, "delta-6-compact") // compaction — must be skipped
	snap7 := appendSnap(7, "delta-7")

	tbl := &streamTable{
		stubTable: stubTable{snap: snap5, sch: intSchema()},
		// All three snapshots are present from the first poll onwards.
		snapIDs: [][]int64{{5}, {5, 6, 7}},
		snaps:   map[int64]*snapshot.Snapshot{5: snap5, 6: snap6, 7: snap7},
	}
	mr := simpleMR(
		[]int64{7},
		[][]manifest.ManifestEntry{
			{addEntry(0, "f7.parquet")},
		},
	)
	// Also register the compact delta so ReadList doesn't panic on a missing key.
	mr.listsByFile["delta-6-compact"] = []manifest.ManifestFileMeta{{FileName: "m-compact-6"}}
	mr.entriesByMetaFile["m-compact-6"] = []manifest.ManifestEntry{
		// Compacted file that contains ALL historical data — must NOT be emitted.
		addEntry(0, "f-all.parquet"),
	}

	sb := newStreamReadBuilderFromIface(tbl, mr).
		WithStartingFrom(StartingFromLatest).
		WithPollInterval(0)
	stream := sb.NewStream()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The first Next() call must skip snapshot 6 (COMPACT) and return snapshot 7.
	batch, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batch.SnapshotID != 7 {
		t.Errorf("want snapshotID=7 (compaction skipped), got %d", batch.SnapshotID)
	}
	// Verify only f7.parquet is in the splits — not f-all.parquet from the compaction.
	if len(batch.Splits) != 1 || batch.Splits[0].Files[0].FileName != "f7.parquet" {
		t.Errorf("want only f7.parquet, got splits: %+v", batch.Splits)
	}
	// lastID should be 7, meaning both 6 and 7 were advanced through.
	if stream.LastSnapshotID() != 7 {
		t.Errorf("want lastID=7, got %d", stream.LastSnapshotID())
	}
}

// TestTableStream_ContextCancellation verifies that Next() returns
// context.Canceled when the context is cancelled while waiting for new data.
func TestTableStream_ContextCancellation(t *testing.T) {
	snap5 := appendSnap(5, "delta-5")

	tbl := &streamTable{
		stubTable: stubTable{snap: snap5, sch: intSchema()},
		// Always return only snapshot 5 — stream will never see a new one.
		snapIDs: [][]int64{{5}},
		snaps:   map[int64]*snapshot.Snapshot{5: snap5},
	}
	mr := &streamManifestReader{
		listsByFile:       map[string][]manifest.ManifestFileMeta{},
		entriesByMetaFile: map[string][]manifest.ManifestEntry{},
	}

	sb := newStreamReadBuilderFromIface(tbl, mr).
		WithStartingFrom(StartingFromLatest).
		WithPollInterval(10 * time.Millisecond)
	stream := sb.NewStream()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := stream.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", err)
	}
}
