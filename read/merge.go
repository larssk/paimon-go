package read

// merge.go — PK table merge-on-read pipeline.
//
// Pipeline (mirrors paimon-python MergeFileSplitRead):
//
//	For each (partition, bucket) split flagged NeedsMerge:
//	  1. intervalPartition groups files into non-overlapping sections.
//	  2. Within each section, a min-heap sort-merges parallel SortedRuns.
//	  3. DeduplicateMergeFunction keeps the highest-seqnum KV per primary key.
//	  4. Rows with RowKind DELETE (3) or UPDATE_BEFORE (1) are dropped.
//	  5. Only value fields (no _SEQUENCE_NUMBER / _VALUE_KIND) are emitted.

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

// PK Parquet physical column names.
const (
	colSequenceNumber = "_SEQUENCE_NUMBER"
	colValueKind      = "_VALUE_KIND"
)

// kvRow is a single decoded row from a PK Parquet file.
type kvRow struct {
	key    *binaryrow.BinaryRow // decoded primary key (for comparison only — sourced from KeyStats)
	seqNum int64
	kind   binaryrow.RowKind
	// rowIdx and batch are the source for the value columns.
	batch  arrow.RecordBatch
	rowIdx int
}

// ---- intervalPartition ----

// intervalPartition groups files into non-overlapping sections.
// Each section is a slice of SortedRuns; within each section, all runs have
// non-overlapping key ranges (guaranteed for L1+ files).
// L0 files have overlapping ranges and are each placed in their own SortedRun
// within a single shared section (so they all get merged together).
//
// Returns: []section where section = []sortedRun = []DataFileMeta
func intervalPartition(files []manifest.DataFileMeta, keyFields []schema.DataField) [][][]manifest.DataFileMeta {
	if len(files) == 0 {
		return nil
	}

	// Separate L0 (overlapping) from higher-level (sorted) files.
	var l0 []manifest.DataFileMeta
	var lN []manifest.DataFileMeta
	for _, f := range files {
		if f.Level == 0 {
			l0 = append(l0, f)
		} else {
			lN = append(lN, f)
		}
	}

	// Sort lN files by their min key.
	sort.Slice(lN, func(i, j int) bool {
		return binaryrow.Compare(lN[i].KeyStats.MinValues, lN[j].KeyStats.MinValues, keyFields) < 0
	})

	// Build sections from lN files using the greedy interval-partition algorithm
	// (same as paimon-python IntervalPartition._partition_section).
	// A section boundary occurs whenever a file's minKey > current section bound.
	var sections [][][]manifest.DataFileMeta

	if len(lN) > 0 {
		// Each section is a list of SortedRuns.
		// A SortedRun is a list of files that are guaranteed non-overlapping.
		// We use a heap of "runs" ordered by the max key of their last file.
		var currentSection []manifest.DataFileMeta
		sectionBound := lN[0].KeyStats.MaxValues // max key seen in current section

		for _, f := range lN {
			cmp := binaryrow.Compare(f.KeyStats.MinValues, sectionBound, keyFields)
			if cmp > 0 {
				// New section: flush current section.
				if len(currentSection) > 0 {
					secs := partitionSection(currentSection, keyFields)
					sections = append(sections, secs)
				}
				currentSection = nil
				sectionBound = f.KeyStats.MaxValues
			} else {
				// Extend section bound if this file's max is larger.
				if binaryrow.Compare(f.KeyStats.MaxValues, sectionBound, keyFields) > 0 {
					sectionBound = f.KeyStats.MaxValues
				}
			}
			currentSection = append(currentSection, f)
		}
		if len(currentSection) > 0 {
			secs := partitionSection(currentSection, keyFields)
			sections = append(sections, secs)
		}
	}

	// L0 files: each goes into its own SortedRun inside one shared section.
	// (They overlap, so they all need to be merged together in one section.)
	if len(l0) > 0 {
		l0Section := make([][]manifest.DataFileMeta, len(l0))
		for i, f := range l0 {
			l0Section[i] = []manifest.DataFileMeta{f}
		}
		sections = append(sections, l0Section)
	}

	return sections
}

// heapRun tracks the current max-key boundary for a run being built.
type heapRun struct {
	files    []manifest.DataFileMeta
	maxKey   *binaryrow.BinaryRow
	keyFields []schema.DataField
}

type heapRunSlice []*heapRun

func (h heapRunSlice) Len() int      { return len(h) }
func (h heapRunSlice) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h heapRunSlice) Less(i, j int) bool {
	return binaryrow.Compare(h[i].maxKey, h[j].maxKey, h[i].keyFields) < 0
}
func (h *heapRunSlice) Push(x interface{}) { *h = append(*h, x.(*heapRun)) }
func (h *heapRunSlice) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// partitionSection packs a list of pre-sorted (by minKey) files into the
// minimum number of non-overlapping SortedRuns using a min-heap on maxKey.
func partitionSection(files []manifest.DataFileMeta, keyFields []schema.DataField) [][]manifest.DataFileMeta {
	h := &heapRunSlice{}
	heap.Init(h)

	for _, f := range files {
		f := f // capture
		var run *heapRun
		if h.Len() > 0 {
			top := (*h)[0]
			// Can this file extend the top run without overlapping?
			if binaryrow.Compare(f.KeyStats.MinValues, top.maxKey, keyFields) > 0 {
				run = heap.Pop(h).(*heapRun)
			}
		}
		if run == nil {
			run = &heapRun{keyFields: keyFields}
		}
		run.files = append(run.files, f)
		run.maxKey = f.KeyStats.MaxValues
		heap.Push(h, run)
	}

	result := make([][]manifest.DataFileMeta, h.Len())
	for i := range result {
		result[i] = heap.Pop(h).(*heapRun).files
	}
	return result
}

// ---- sortMergeReader ----

// sortMergeReader implements array.RecordReader for a single PK DataSplit.
// It applies interval-partition, then sort-merges within each section,
// and emits deduplicated value rows (RowKind INSERT or UPDATE_AFTER only).
type sortMergeReader struct {
	ctx         context.Context
	tbl         tableReader
	split       DataSplit
	keyFields   []schema.DataField
	readFields  []schema.DataField // output fields (no PK metadata columns)
	arrowSchema *arrow.Schema
	alloc       memory.Allocator
	filter      *predicate.Predicate // nil = no filter

	// State machine: sections → per-section heap merge
	sections     [][][]manifest.DataFileMeta // from intervalPartition
	sectionIdx   int

	// Current section's heap-merge state
	merger *sectionMerger

	// Output buffer: pending merged rows to emit
	pending []mergedRow
	pendIdx int

	current    arrow.RecordBatch
	currentErr error
}

// mergedRow holds a winning KV after deduplication.
type mergedRow struct {
	batch  arrow.RecordBatch
	rowIdx int
}

func newSortMergeReader(
	ctx context.Context,
	tbl tableReader,
	split DataSplit,
	keyFields []schema.DataField,
	readFields []schema.DataField,
	arrowSchema *arrow.Schema,
	alloc memory.Allocator,
	filter *predicate.Predicate,
) (*sortMergeReader, error) {
	s := tbl.GetSchema()
	sections := intervalPartition(split.Files, s.PrimaryKeyFields())
	return &sortMergeReader{
		ctx:         ctx,
		tbl:         tbl,
		split:       split,
		keyFields:   keyFields,
		readFields:  readFields,
		arrowSchema: arrowSchema,
		alloc:       alloc,
		sections:    sections,
		filter:      filter,
	}, nil
}

func (r *sortMergeReader) Schema() *arrow.Schema { return r.arrowSchema }
func (r *sortMergeReader) Retain()               {}
func (r *sortMergeReader) Release() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.merger != nil {
		r.merger.close()
		r.merger = nil
	}
}
func (r *sortMergeReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *sortMergeReader) Record() arrow.RecordBatch      { return r.current }
func (r *sortMergeReader) Err() error                     { return r.currentErr }

func (r *sortMergeReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	for {
		// Drain pending merged rows first.
		if r.pendIdx < len(r.pending) {
			mr := r.pending[r.pendIdx]
			r.pendIdx++
			rec, err := r.buildOutputRecord(mr)
			if err != nil {
				r.currentErr = err
				return false
			}
			if rec == nil {
				continue // filtered out
			}
			r.current = rec
			return true
		}

		// Advance to the next section if needed.
		if r.merger == nil || r.merger.done() {
			if r.merger != nil {
				r.merger.close()
				r.merger = nil
			}
			if r.sectionIdx >= len(r.sections) {
				return false
			}
			section := r.sections[r.sectionIdx]
			r.sectionIdx++
			m, err := r.openSection(section)
			if err != nil {
				r.currentErr = err
				return false
			}
			r.merger = m
		}

		// Pull next batch of merged rows from the current section.
		rows, err := r.merger.next()
		if err != nil {
			r.currentErr = err
			return false
		}
		if len(rows) == 0 {
			// Section exhausted.
			r.merger.close()
			r.merger = nil
			continue
		}
		r.pending = rows
		r.pendIdx = 0
	}
}

// buildOutputRecord extracts the value columns for one merged row into an
// Arrow record batch matching readFields / arrowSchema.
func (r *sortMergeReader) buildOutputRecord(mr mergedRow) (arrow.RecordBatch, error) {
	rec := mr.batch
	defer rec.Release() // release the retained batch now that we've extracted the row
	rowIdx := mr.rowIdx
	fileSchema := rec.Schema()

	cols := make([]arrow.Array, len(r.readFields))
	effectiveFields := make([]arrow.Field, len(r.readFields))

	for i, f := range r.readFields {
		wantField, err := schema.ToArrowField(f)
		if err != nil {
			return nil, err
		}
		colIndices := fileSchema.FieldIndices(f.Name)
		if len(colIndices) > 0 {
			src := rec.Column(colIndices[0])
			// Build a single-row array.
			col := extractRow(src, rowIdx, r.alloc)
			cols[i] = col
			fileField := fileSchema.Field(colIndices[0])
			if arrowTypesCompatible(fileField.Type, wantField.Type) {
				effectiveFields[i] = fileField
				effectiveFields[i].Name = f.Name
			} else {
				effectiveFields[i] = wantField
			}
		} else {
			cols[i] = array.MakeArrayOfNull(r.alloc, wantField.Type, 1)
			effectiveFields[i] = wantField
		}
	}
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()

	effectiveSchema := arrow.NewSchema(effectiveFields, nil)
	out := array.NewRecord(effectiveSchema, cols, 1)
	// Apply row-level filter against the output record.
	if r.filter != nil && !predicate.EvalRow(r.filter, out, 0) {
		out.Release()
		return nil, nil
	}
	return out, nil
}

// openSection creates a sectionMerger for the given list of SortedRuns.
func (r *sortMergeReader) openSection(section [][]manifest.DataFileMeta) (*sectionMerger, error) {
	runs := make([]*runReader, 0, len(section))
	for _, runFiles := range section {
		rr, err := r.newRunReader(runFiles)
		if err != nil {
			for _, existing := range runs {
				existing.close()
			}
			return nil, err
		}
		runs = append(runs, rr)
	}
	return newSectionMerger(runs, r.keyFields), nil
}

// newRunReader opens all files in a SortedRun and returns a runReader.
func (r *sortMergeReader) newRunReader(files []manifest.DataFileMeta) (*runReader, error) {
	readers := make([]*fileKVReader, 0, len(files))
	for _, fm := range files {
		fr, err := r.openKVReader(fm)
		if err != nil {
			for _, existing := range readers {
				existing.close()
			}
			return nil, err
		}
		readers = append(readers, fr)
	}
	return &runReader{readers: readers}, nil
}

// openKVReader opens one Parquet file and returns a fileKVReader.
func (r *sortMergeReader) openKVReader(fm manifest.DataFileMeta) (*fileKVReader, error) {
	tbl := r.tbl
	var filePath string
	if fm.ExternalPath != nil {
		filePath = *fm.ExternalPath
	} else {
		var partRow *binaryrow.BinaryRow
		if r.split.Partition != nil {
			partRow = r.split.Partition.Partition
		}
		filePath = tbl.DataFilePath(
			partRow,
			tbl.GetSchema().PartitionFields(),
			r.split.Bucket,
			fm.FileName,
		)
	}

	rc, err := tbl.GetIO().Open(r.ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("merge: open %s: %w", filePath, err)
	}
	ras, err := toReaderAtSeeker(rc)
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("merge: buffer %s: %w", filePath, err)
	}
	rc.Close() // data is now in memory

	pqReader, err := file.NewParquetReader(ras)
	if err != nil {
		return nil, fmt.Errorf("merge: open parquet %s: %w", filePath, err)
	}
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		BatchSize: 65536,
	}, r.alloc)
	if err != nil {
		return nil, fmt.Errorf("merge: arrow reader %s: %w", filePath, err)
	}
	// Read all columns (we need _SEQUENCE_NUMBER, _VALUE_KIND plus all value cols).
	rdr, err := arrowReader.GetRecordReader(r.ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("merge: record reader %s: %w", filePath, err)
	}
	return &fileKVReader{rdr: rdr, keyStats: fm.KeyStats}, nil
}

// ---- fileKVReader: reads one Parquet file, emitting heapEntry values ----

type fileKVReader struct {
	rdr      array.RecordReader
	keyStats manifest.SimpleStats
	// Current batch state.
	batch    arrow.RecordBatch
	batchRow int
	seqCol   int // index of _SEQUENCE_NUMBER col in batch schema, or -1
	kindCol  int // index of _VALUE_KIND col in batch schema, or -1
}

func (f *fileKVReader) close() {
	if f.batch != nil {
		f.batch.Release()
		f.batch = nil
	}
	if f.rdr != nil {
		f.rdr.Release()
		f.rdr = nil
	}
}

// advance loads the next row, returning (seqNum, kind, batch, rowIdx, ok).
func (f *fileKVReader) advance() (int64, binaryrow.RowKind, arrow.RecordBatch, int, bool) {
	for {
		if f.batch != nil && f.batchRow < int(f.batch.NumRows()) {
			row := f.batchRow
			f.batchRow++
			seq := f.readSeqNum(row)
			kind := f.readKind(row)
			return seq, kind, f.batch, row, true
		}
		// Load next batch.
		if f.batch != nil {
			f.batch.Release()
			f.batch = nil
		}
		if !f.rdr.Next() {
			return 0, 0, nil, 0, false
		}
		b := f.rdr.RecordBatch()
		b.Retain()
		f.batch = b
		f.batchRow = 0
		// Resolve column indices once per batch (schema is constant across batches).
		s := b.Schema()
		f.seqCol = -1
		f.kindCol = -1
		for ci := 0; ci < s.NumFields(); ci++ {
			switch s.Field(ci).Name {
			case colSequenceNumber:
				f.seqCol = ci
			case colValueKind:
				f.kindCol = ci
			}
		}
	}
}

func (f *fileKVReader) readSeqNum(row int) int64 {
	if f.batch == nil || f.seqCol < 0 {
		return 0
	}
	col := f.batch.Column(f.seqCol)
	if col.IsNull(row) {
		return 0
	}
	if c, ok := col.(*array.Int64); ok {
		return c.Value(row)
	}
	return 0
}

func (f *fileKVReader) readKind(row int) binaryrow.RowKind {
	if f.batch == nil || f.kindCol < 0 {
		return binaryrow.Insert
	}
	col := f.batch.Column(f.kindCol)
	if col.IsNull(row) {
		return binaryrow.Insert
	}
	if c, ok := col.(*array.Int8); ok {
		return binaryrow.RowKind(c.Value(row))
	}
	return binaryrow.Insert
}

// ---- runReader: concatenates fileKVReaders for one SortedRun ----

type runReader struct {
	readers []*fileKVReader
	idx     int
}

func (r *runReader) close() {
	for _, fr := range r.readers {
		fr.close()
	}
}

// next returns the next row from the run (across all files in order).
func (r *runReader) next() (int64, binaryrow.RowKind, arrow.RecordBatch, int, bool) {
	for r.idx < len(r.readers) {
		seq, kind, batch, row, ok := r.readers[r.idx].advance()
		if ok {
			return seq, kind, batch, row, true
		}
		r.idx++
	}
	return 0, 0, nil, 0, false
}

// ---- heapEntry / mergeHeap ----

// heapEntry is one element in the sort-merge heap: the current row from one SortedRun.
type heapEntry struct {
	// key is derived from KeyStats for comparison; for runtime rows we use
	// the actual primary-key columns from the Arrow batch.
	keyBatch  arrow.RecordBatch // the batch containing the row
	keyRow    int               // row index within keyBatch
	keyFields []schema.DataField
	keyColIdx []int // Arrow column indices for each PK field in keyBatch

	seqNum int64
	kind   binaryrow.RowKind
	batch  arrow.RecordBatch // same as keyBatch (value columns are here too)
	rowIdx int

	run *runReader
}

type mergeHeap []*heapEntry

func (h mergeHeap) Len() int      { return len(h) }
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h mergeHeap) Less(i, j int) bool {
	c := compareArrowKey(h[i], h[j])
	if c != 0 {
		return c < 0
	}
	// Tie-break by seqNum ascending (lowest seqNum = "smaller" → last add() wins).
	return h[i].seqNum < h[j].seqNum
}
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(*heapEntry)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// compareArrowKey compares two heap entries by their primary key columns.
func compareArrowKey(a, b *heapEntry) int {
	for i, kf := range a.keyFields {
		ai := a.keyColIdx[i]
		bi := b.keyColIdx[i]
		var aNull, bNull bool
		var aVal, bVal interface{}
		if ai < 0 {
			aNull = true
		} else {
			aCol := a.keyBatch.Column(ai)
			aNull = aCol.IsNull(a.keyRow)
			if !aNull {
				aVal = arrowScalar(aCol, a.keyRow)
			}
		}
		if bi < 0 {
			bNull = true
		} else {
			bCol := b.keyBatch.Column(bi)
			bNull = bCol.IsNull(b.keyRow)
			if !bNull {
				bVal = arrowScalar(bCol, b.keyRow)
			}
		}
		c := compareScalar(aVal, bVal, aNull, bNull, kf.Type)
		if c != 0 {
			return c
		}
	}
	return 0
}

// arrowScalar extracts a scalar value from an Arrow column at row i.
func arrowScalar(col arrow.Array, i int) interface{} {
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(i)
	case *array.Int8:
		return c.Value(i)
	case *array.Int16:
		return c.Value(i)
	case *array.Int32:
		return c.Value(i)
	case *array.Int64:
		return c.Value(i)
	case *array.Float32:
		return c.Value(i)
	case *array.Float64:
		return c.Value(i)
	case *array.String:
		return c.Value(i)
	case *array.LargeString:
		return c.Value(i)
	case *array.Binary:
		return c.Value(i)
	case *array.Date32:
		return int32(c.Value(i))
	case *array.Timestamp:
		return int64(c.Value(i))
	default:
		return nil
	}
}

// compareScalar compares two scalars of the same PK field type.
func compareScalar(a, b interface{}, aNil, bNil bool, _ schema.DataType) int {
	if aNil && bNil {
		return 0
	}
	if aNil {
		return -1
	}
	if bNil {
		return 1
	}
	switch av := a.(type) {
	case bool:
		bv := b.(bool)
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	case int8:
		bv := b.(int8)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int16:
		bv := b.(int16)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int32:
		bv := b.(int32)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int64:
		bv := b.(int64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		bv := b.(float32)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv := b.(float64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv := b.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case []byte:
		bv := b.([]byte)
		for i := 0; i < len(av) && i < len(bv); i++ {
			if av[i] < bv[i] {
				return -1
			}
			if av[i] > bv[i] {
				return 1
			}
		}
		if len(av) < len(bv) {
			return -1
		}
		if len(av) > len(bv) {
			return 1
		}
		return 0
	}
	return 0
}

// pkColIndices finds the Arrow column indices for each PK field in a batch schema.
func pkColIndices(batchSchema *arrow.Schema, keyFields []schema.DataField) []int {
	idx := make([]int, len(keyFields))
	for i, kf := range keyFields {
		found := batchSchema.FieldIndices(kf.Name)
		if len(found) > 0 {
			idx[i] = found[0]
		} else {
			idx[i] = -1
		}
	}
	return idx
}

// ---- sectionMerger: heap-merge across SortedRuns in one section ----

type sectionMerger struct {
	h         *mergeHeap
	keyFields []schema.DataField
	exhausted bool
}

func newSectionMerger(runs []*runReader, keyFields []schema.DataField) *sectionMerger {
	h := &mergeHeap{}
	heap.Init(h)
	m := &sectionMerger{h: h, keyFields: keyFields}

	// Seed the heap with the first row from each run.
	for _, run := range runs {
		run := run // capture
		seq, kind, batch, row, ok := run.next()
		if !ok {
			continue
		}
		batch.Retain() // heapEntry owns a reference
		colIdx := pkColIndices(batch.Schema(), keyFields)
		entry := &heapEntry{
			keyBatch:  batch,
			keyRow:    row,
			keyFields: keyFields,
			keyColIdx: colIdx,
			seqNum:    seq,
			kind:      kind,
			batch:     batch,
			rowIdx:    row,
			run:       run,
		}
		heap.Push(h, entry)
	}
	return m
}

func (m *sectionMerger) done() bool { return m.exhausted }

func (m *sectionMerger) close() {
	// runs are closed by sortMergeReader
}

// next pops all entries with the same minimum key, runs DeduplicateMergeFunction,
// and returns up to one merged row (or empty if the result is a delete).
func (m *sectionMerger) next() ([]mergedRow, error) {
	if m.h.Len() == 0 {
		m.exhausted = true
		return nil, nil
	}

	// Collect all entries with the same key as the heap minimum.
	var group []*heapEntry

	for m.h.Len() > 0 {
		top := (*m.h)[0]
		if len(group) > 0 && compareArrowKey(group[0], top) != 0 {
			break
		}
		e := heap.Pop(m.h).(*heapEntry)
		group = append(group, e)

		// Advance the run and push the next row back.
		seq, kind, batch, row, ok := e.run.next()
		if ok {
			batch.Retain() // next heapEntry owns a reference
			colIdx := pkColIndices(batch.Schema(), m.keyFields)
			next := &heapEntry{
				keyBatch:  batch,
				keyRow:    row,
				keyFields: m.keyFields,
				keyColIdx: colIdx,
				seqNum:    seq,
				kind:      kind,
				batch:     batch,
				rowIdx:    row,
				run:       e.run,
			}
			heap.Push(m.h, next)
		}
	}

	// DeduplicateMergeFunction: group is sorted by (key, seqNum) ascending;
	// last element has the highest seqNum → it wins.
	winner := group[len(group)-1]

	// Release all non-winners (they own a retain).
	for i := 0; i < len(group)-1; i++ {
		group[i].batch.Release()
	}

	// Drop DELETE (3) and UPDATE_BEFORE (1).
	if winner.kind == binaryrow.Delete || winner.kind == binaryrow.UpdateBefore {
		winner.batch.Release() // release the winner too since we won't use it
		return []mergedRow{}, nil // empty = "no output for this key"
	}

	// Pass ownership of winner.batch to mergedRow; caller must release after use.
	return []mergedRow{{batch: winner.batch, rowIdx: winner.rowIdx}}, nil
}

// ---- extractRow: single-row array extraction ----

// extractRow copies row i from src into a new length-1 array.
func extractRow(src arrow.Array, i int, alloc memory.Allocator) arrow.Array {
	switch c := src.(type) {
	case *array.Boolean:
		b := array.NewBooleanBuilder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Int8:
		b := array.NewInt8Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Int16:
		b := array.NewInt16Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Int32:
		b := array.NewInt32Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Int64:
		b := array.NewInt64Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Float32:
		b := array.NewFloat32Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Float64:
		b := array.NewFloat64Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.String:
		b := array.NewStringBuilder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.LargeString:
		b := array.NewLargeStringBuilder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Binary:
		b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Date32:
		b := array.NewDate32Builder(alloc)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	case *array.Timestamp:
		dt := src.DataType().(*arrow.TimestampType)
		b := array.NewTimestampBuilder(alloc, dt)
		if c.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(c.Value(i))
		}
		return b.NewArray()
	default:
		return array.MakeArrayOfNull(alloc, src.DataType(), 1)
	}
}

// Ensure io is imported (used via toReaderAtSeeker which takes io.ReadCloser).
var _ io.Reader
