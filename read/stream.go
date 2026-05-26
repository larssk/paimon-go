package read

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/predicate"
	"github.com/apache/paimon/paimon-go/schema"
	"github.com/apache/paimon/paimon-go/snapshot"
	"github.com/apache/paimon/paimon-go/table"
)

// StartingFrom controls where a stream read begins.
type StartingFrom int

const (
	// StartingFromLatest skips all existing data and only emits rows from
	// snapshots that appear after the stream is started.
	StartingFromLatest StartingFrom = iota

	// StartingFromEarliest replays all existing snapshots before emitting new ones.
	StartingFromEarliest
)

const defaultPollInterval = 2 * time.Second

// StreamReadBuilder is the entry point for building a continuous stream read.
type StreamReadBuilder struct {
	tbl            tableReader
	manifestReader manifestReader
	filter         *predicate.Predicate
	projection     []string
	startingFrom   StartingFrom
	pollInterval   time.Duration
}

// NewStreamReadBuilder creates a StreamReadBuilder for the given table.
//
// By default reads start from the latest snapshot ([StartingFromLatest]) with
// no filter and no projection. Use [StreamReadBuilder.WithStartingFrom],
// [StreamReadBuilder.WithFilter], and [StreamReadBuilder.WithProjection] to
// configure before calling [StreamReadBuilder.NewStream].
func NewStreamReadBuilder(tbl *table.FileStoreTable) *StreamReadBuilder {
	return &StreamReadBuilder{
		tbl:            tbl,
		manifestReader: newDefaultManifestReader(tbl.ManifestDir(), tbl.GetIO()),
		pollInterval:   defaultPollInterval,
	}
}

// newStreamReadBuilderFromIface creates a StreamReadBuilder using the tableReader
// interface directly. Used in tests.
func newStreamReadBuilderFromIface(tbl tableReader, mr manifestReader) *StreamReadBuilder {
	return &StreamReadBuilder{
		tbl:            tbl,
		manifestReader: mr,
		pollInterval:   defaultPollInterval,
	}
}

// WithFilter attaches a filter predicate applied at the row level to each
// snapshot batch. Stats-based pruning is also applied during planning.
// Passing nil clears any previously set filter.
func (sb *StreamReadBuilder) WithFilter(p *predicate.Predicate) *StreamReadBuilder {
	sb.filter = p
	return sb
}

// WithProjection limits the columns returned to the named subset.
// Unknown names are silently ignored. Passing nil or empty returns all columns.
func (sb *StreamReadBuilder) WithProjection(cols []string) *StreamReadBuilder {
	sb.projection = cols
	return sb
}

// WithStartingFrom sets where the stream begins (default: StartingFromLatest).
func (sb *StreamReadBuilder) WithStartingFrom(s StartingFrom) *StreamReadBuilder {
	sb.startingFrom = s
	return sb
}

// WithPollInterval sets how often to check for new snapshots (default: 2s).
func (sb *StreamReadBuilder) WithPollInterval(d time.Duration) *StreamReadBuilder {
	sb.pollInterval = d
	return sb
}

// NewStream returns a TableStream that iterates over new snapshots.
// Each call to [TableStream.Next] blocks until a new APPEND snapshot arrives.
func (sb *StreamReadBuilder) NewStream() *TableStream {
	return &TableStream{sb: sb, lastID: -1}
}

// readFields returns the effective read schema.
func (sb *StreamReadBuilder) readFields() []schema.DataField {
	s := sb.tbl.GetSchema()
	if len(sb.projection) == 0 {
		return s.Fields
	}
	var fields []schema.DataField
	for _, name := range sb.projection {
		if f, ok := s.FieldByName(name); ok {
			fields = append(fields, f)
		}
	}
	return fields
}

// --- TableStream ---

// TableStream iterates over snapshots as they arrive, emitting one batch of
// splits per new snapshot. Call Next in a loop; each successful call provides
// splits that can be read with NewRead().ToArrowReader().
type TableStream struct {
	sb     *StreamReadBuilder
	lastID int64 // last snapshot ID consumed; -1 = not yet initialised
}

// StreamBatch is the result of a single TableStream.Next() call.
type StreamBatch struct {
	Splits     []DataSplit
	SnapshotID int64
}

// Next blocks until a new snapshot is available, then returns its splits.
//
// Only APPEND commits produce splits; COMPACT/OVERWRITE/ANALYZE snapshots are
// silently skipped (lastID is advanced but no batch is returned to the caller).
// Returns a non-nil error (including context.Canceled / context.DeadlineExceeded)
// when the context is done.
func (ts *TableStream) Next(ctx context.Context) (*StreamBatch, error) {
	// First call: initialise lastID.
	if ts.lastID < 0 {
		if err := ts.initialise(ctx); err != nil {
			return nil, err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ids, err := ts.sb.tbl.ListSnapshotIDs(ctx)
		if err != nil {
			return nil, fmt.Errorf("stream: list snapshots: %w", err)
		}

		// Find the smallest ID strictly greater than lastID.
		nextID := int64(-1)
		for _, id := range ids {
			if id > ts.lastID {
				nextID = id
				break
			}
		}

		if nextID >= 0 {
			snap, err := ts.sb.tbl.SnapshotByID(ctx, nextID)
			if err != nil {
				return nil, fmt.Errorf("stream: read snapshot %d: %w", nextID, err)
			}
			// Always advance lastID so we don't re-read this snapshot.
			ts.lastID = nextID

			// Skip non-APPEND commits (compaction, overwrite, analyze).
			// A COMPACT snapshot reorganises physical files without adding new
			// logical rows; emitting it would replay existing data.
			if snap.CommitKind != snapshot.CommitAppend {
				continue
			}

			batch, err := ts.readDelta(ctx, snap)
			if err != nil {
				return nil, err
			}
			return batch, nil
		}

		// No new snapshot yet — wait and retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(ts.sb.pollInterval):
		}
	}
}

// LastSnapshotID returns the ID of the last snapshot consumed, or -1 if none yet.
func (ts *TableStream) LastSnapshotID() int64 { return ts.lastID }

// initialise sets lastID based on the StartingFrom setting.
func (ts *TableStream) initialise(ctx context.Context) error {
	switch ts.sb.startingFrom {
	case StartingFromLatest:
		snap, err := ts.sb.tbl.LatestSnapshot(ctx)
		if err != nil {
			return fmt.Errorf("stream: resolve latest snapshot: %w", err)
		}
		ts.lastID = snap.ID
	case StartingFromEarliest:
		ids, err := ts.sb.tbl.ListSnapshotIDs(ctx)
		if err != nil {
			return fmt.Errorf("stream: list snapshots: %w", err)
		}
		if len(ids) == 0 {
			ts.lastID = 0
		} else {
			ts.lastID = ids[0] - 1 // poll loop will find ids[0] as the first "new" snapshot
		}
	}
	return nil
}

// readDelta reads the delta manifest list for an APPEND snapshot and returns
// splits for the newly added data files.
//
// For an APPEND commit the delta manifest list contains only ADD entries for
// genuinely new data files — no rewritten or historical files.
func (ts *TableStream) readDelta(ctx context.Context, snap *snapshot.Snapshot) (*StreamBatch, error) {
	s := ts.sb.tbl.GetSchema()
	partFields := s.PartitionFields()
	valueFields := s.Fields
	mr := ts.sb.manifestReader

	// Read only the delta manifest list for this APPEND commit.
	deltaMetas, err := mr.ReadList(ctx, snap.DeltaManifestList, partFields)
	if err != nil {
		return nil, fmt.Errorf("stream: delta manifest list for snapshot %d: %w", snap.ID, err)
	}

	// Prune manifest files by partition stats if a filter is set.
	if ts.sb.filter != nil {
		pruned := deltaMetas[:0]
		for _, m := range deltaMetas {
			total := m.NumAddedFiles + m.NumDeletedFiles
			if predicate.TestByStats(ts.sb.filter, m.PartitionStats, total) {
				pruned = append(pruned, m)
			}
		}
		deltaMetas = pruned
	}

	entries, err := mr.ReadAllEntries(ctx, deltaMetas, partFields, valueFields)
	if err != nil {
		return nil, fmt.Errorf("stream: read entries for snapshot %d: %w", snap.ID, err)
	}

	// Keep only ADD entries (append-only tables should have no DELETEs in delta,
	// but guard defensively).
	addEntries := entries[:0]
	for _, e := range entries {
		if e.Kind == manifest.EntryAdd {
			addEntries = append(addEntries, e)
		}
	}

	// Prune individual files by value stats if a filter is set.
	if ts.sb.filter != nil {
		pruned := addEntries[:0]
		for _, e := range addEntries {
			p := predicate.WithProjection(ts.sb.filter, s.Fields)
			if predicate.TestByStats(p, e.File.ValueStats, e.File.RowCount) {
				pruned = append(pruned, e)
			}
		}
		addEntries = pruned
	}

	splits := buildStreamSplits(addEntries)
	return &StreamBatch{Splits: splits, SnapshotID: snap.ID}, nil
}

func buildStreamSplits(entries []manifest.ManifestEntry) []DataSplit {
	type key struct {
		partPath string
		bucket   int
	}
	groups := make(map[key][]manifest.ManifestEntry)
	for _, e := range entries {
		k := key{partPath: partitionKey(e), bucket: e.Bucket}
		groups[k] = append(groups[k], e)
	}
	splits := make([]DataSplit, 0, len(groups))
	for _, group := range groups {
		files := make([]manifest.DataFileMeta, 0, len(group))
		for _, e := range group {
			files = append(files, e.File)
		}
		splits = append(splits, DataSplit{
			Partition: &group[0],
			Bucket:    group[0].Bucket,
			Files:     files,
		})
	}
	return splits
}

// --- TableStreamReader ---

// TableStreamReader is a convenience wrapper that implements array.RecordReader
// across an unbounded stream of snapshots. It blocks inside Next() waiting for
// new data, and respects context cancellation.
type TableStreamReader struct {
	ctx    context.Context
	stream *TableStream
	read   *StreamReadBuilder

	current    arrow.RecordBatch
	currentErr error
	innerRdr   array.RecordReader
	curSnapID  int64
}

// NewStreamReader creates a TableStreamReader from a StreamReadBuilder.
// The returned reader implements [array.RecordReader] and can be used
// wherever a standard Arrow record reader is expected. It blocks inside
// [TableStreamReader.Next] until a new snapshot batch arrives or ctx is
// cancelled.
func NewStreamReader(ctx context.Context, sb *StreamReadBuilder) *TableStreamReader {
	return &TableStreamReader{
		ctx:    ctx,
		stream: sb.NewStream(),
		read:   sb,
	}
}

// Schema returns the Arrow schema of the record batches produced by this reader.
func (r *TableStreamReader) Schema() *arrow.Schema {
	fields := r.read.readFields()
	s, err := schema.ToArrowSchema(fields)
	if err != nil {
		panic(fmt.Sprintf("stream: build Arrow schema: %v", err))
	}
	return s
}

// Retain is a no-op; TableStreamReader does not use reference counting.
func (r *TableStreamReader) Retain()  {}

// Release releases the current record batch and any open inner reader.
// Safe to call multiple times. Call when you are done with the reader.
func (r *TableStreamReader) Release() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.innerRdr != nil {
		r.innerRdr.Release()
		r.innerRdr = nil
	}
}

// RecordBatch returns the current record batch. Valid only after Next returns true.
func (r *TableStreamReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is an alias for RecordBatch, present for interface compatibility.
func (r *TableStreamReader) Record() arrow.RecordBatch { return r.current }

// Err returns the first error encountered, or nil if the reader stopped because
// the context was cancelled. Always check Err after Next returns false.
func (r *TableStreamReader) Err() error { return r.currentErr }

// SnapshotID returns the snapshot ID of the current record batch.
// Returns -1 before the first successful Next call.
func (r *TableStreamReader) SnapshotID() int64 { return r.curSnapID }

// Next advances to the next record batch, blocking until new data arrives.
// Returns false when the context is cancelled or an error occurs; check
// [TableStreamReader.Err] to distinguish the two cases.
func (r *TableStreamReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	for {
		// Drain the current inner reader first.
		if r.innerRdr != nil {
			if r.innerRdr.Next() {
				r.current = r.innerRdr.RecordBatch()
				r.current.Retain()
				return true
			}
			if err := r.innerRdr.Err(); err != nil {
				r.currentErr = err
				return false
			}
			r.innerRdr.Release()
			r.innerRdr = nil
		}

		// Fetch the next snapshot batch (blocks until one is available).
		batch, err := r.stream.Next(r.ctx)
		if err != nil {
			r.currentErr = err
			return false
		}
		r.curSnapID = batch.SnapshotID

		if len(batch.Splits) == 0 {
			continue
		}

		// Build a TableRead for this batch using the same settings.
		rb := newReadBuilderFromIface(r.read.tbl, r.read.manifestReader)
		rb.filter = r.read.filter
		rb.projection = r.read.projection

		rdr, err := rb.NewRead().ToArrowReader(r.ctx, batch.Splits)
		if err != nil {
			r.currentErr = err
			return false
		}
		r.innerRdr = rdr
	}
}
