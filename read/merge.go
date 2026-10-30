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

	"github.com/larssk/paimon-go/fileio"
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
// non-overlapping key ranges.
//
// This mirrors paimon-python's IntervalPartition.partition() (interval_partition.py):
// ALL files — regardless of level — are sorted together by (minKey, maxKey)
// and partitioned into sections purely by key-range overlap. There is no
// special-casing for level 0: L0 files typically have wide, overlapping key
// ranges and therefore naturally end up sharing a section with any L1+ files
// whose ranges overlap, which is required for correct merge-on-read — e.g. a
// DELETE row in a fresh L0 file must be able to merge against (and suppress)
// a stale INSERT row for the same key in an older L1+ file. Isolating L0
// into its own section (as an earlier version of this function did) breaks
// that merge and lets stale rows leak through undeleted.
//
// Returns: []section where section = []sortedRun = []DataFileMeta
func intervalPartition(files []manifest.DataFileMeta, keyFields []schema.DataField) [][][]manifest.DataFileMeta {
	if len(files) == 0 {
		return nil
	}
	for _, f := range files {
		bounds := f.KeyBounds()
		if bounds.MinValues == nil || bounds.MaxValues == nil || bounds.MinValues.Arity() != len(keyFields) || bounds.MaxValues.Arity() != len(keyFields) {
			// An unknown range may overlap every other file. Keep all files in
			// one section, with a separate run per file, rather than accidentally
			// separating versions of a key and exposing stale rows.
			runs := make([][]manifest.DataFileMeta, len(files))
			for i, file := range files {
				runs[i] = []manifest.DataFileMeta{file}
			}
			return [][][]manifest.DataFileMeta{runs}
		}
	}

	sorted := make([]manifest.DataFileMeta, len(files))
	copy(sorted, files)

	// Sort by (minKey, maxKey) ascending — matches paimon-python's _compare_files.
	sort.Slice(sorted, func(i, j int) bool {
		cmp := binaryrow.Compare(sorted[i].KeyBounds().MinValues, sorted[j].KeyBounds().MinValues, keyFields)
		if cmp != 0 {
			return cmp < 0
		}
		return binaryrow.Compare(sorted[i].KeyBounds().MaxValues, sorted[j].KeyBounds().MaxValues, keyFields) < 0
	})

	// Build sections using the greedy interval-partition algorithm (same as
	// paimon-python IntervalPartition.partition()). A section boundary occurs
	// whenever a file's minKey > current section bound.
	var sections [][][]manifest.DataFileMeta
	var currentSection []manifest.DataFileMeta
	var sectionBound *binaryrow.BinaryRow

	for _, f := range sorted {
		if len(currentSection) > 0 && binaryrow.Compare(f.KeyBounds().MinValues, sectionBound, keyFields) > 0 {
			sections = append(sections, partitionSection(currentSection, keyFields))
			currentSection = nil
			sectionBound = nil
		}
		currentSection = append(currentSection, f)
		if sectionBound == nil || binaryrow.Compare(f.KeyBounds().MaxValues, sectionBound, keyFields) > 0 {
			sectionBound = f.KeyBounds().MaxValues
		}
	}
	if len(currentSection) > 0 {
		sections = append(sections, partitionSection(currentSection, keyFields))
	}

	return sections
}

// heapRun tracks the current max-key boundary for a run being built.
type heapRun struct {
	files     []manifest.DataFileMeta
	maxKey    *binaryrow.BinaryRow
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
			if binaryrow.Compare(f.KeyBounds().MinValues, top.maxKey, keyFields) > 0 {
				run = heap.Pop(h).(*heapRun)
			}
		}
		if run == nil {
			run = &heapRun{keyFields: keyFields}
		}
		run.files = append(run.files, f)
		run.maxKey = f.KeyBounds().MaxValues
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
	evalFields  []schema.DataField
	evalSchema  *arrow.Schema
	evalFilter  *predicate.Predicate

	// State machine: sections → per-section heap merge
	sections   [][][]manifest.DataFileMeta // from intervalPartition
	sectionIdx int

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
	sections := intervalPartition(split.Files, storageKeyFields(s))
	evalFields := filterFields(filter, s.Fields)
	evalSchema, err := schema.ToArrowSchema(evalFields)
	if err != nil {
		return nil, err
	}
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
		evalFields:  evalFields,
		evalSchema:  evalSchema,
		evalFilter:  predicate.WithProjection(filter, evalFields),
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
	for _, row := range r.pending[r.pendIdx:] {
		row.batch.Release()
	}
	r.pending = nil
	r.pendIdx = 0
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
			// A deleted key produces no row, but does not exhaust the section.
			if !r.merger.done() {
				continue
			}
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
	if r.filter != nil {
		row := rec.NewSlice(int64(rowIdx), int64(rowIdx+1))
		eval, err := projectRecord(r.ctx, row, r.evalFields, r.evalSchema, r.alloc)
		row.Release()
		if err != nil {
			return nil, err
		}
		matches := predicate.EvalRow(r.evalFilter, eval, 0)
		eval.Release()
		if !matches {
			return nil, nil
		}
	}

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
	return newSectionMerger(runs, r.keyFields)
}

// newRunReader defers file opens until the run advances to each file.
func (r *sortMergeReader) newRunReader(files []manifest.DataFileMeta) (*runReader, error) {
	return &runReader{files: files, open: r.openKVReader}, nil
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

	rc, err := fileio.OpenRandomAccess(r.ctx, tbl.GetIO(), filePath)
	if err != nil {
		return nil, fmt.Errorf("merge: open %s: %w", filePath, err)
	}
	pqReader, err := file.NewParquetReader(rc)
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("merge: open parquet %s: %w", filePath, err)
	}
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		BatchSize: 65536,
	}, r.alloc)
	if err != nil {
		pqReader.Close()
		return nil, fmt.Errorf("merge: arrow reader %s: %w", filePath, err)
	}
	fields := append([]schema.DataField(nil), r.readFields...)
	fields = append(fields, r.keyFields...)
	fields = append(fields, schema.DataField{Name: colSequenceNumber}, schema.DataField{Name: colValueKind})
	fields = append(fields, r.evalFields...)
	rdr, err := arrowReader.GetRecordReader(r.ctx, parquetColumnIndices(pqReader, fields), nil)
	if err != nil {
		pqReader.Close()
		return nil, fmt.Errorf("merge: record reader %s: %w", filePath, err)
	}
	return &fileKVReader{rdr: rdr, closer: pqReader, keyStats: fm.KeyStats}, nil
}

// ---- fileKVReader: reads one Parquet file, emitting heapEntry values ----

type fileKVReader struct {
	rdr      array.RecordReader
	closer   io.Closer
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
	if f.closer != nil {
		f.closer.Close()
		f.closer = nil
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
	files   []manifest.DataFileMeta
	open    func(manifest.DataFileMeta) (*fileKVReader, error)
	current *fileKVReader
	idx     int
	err     error
}

func (r *runReader) close() {
	if r.current != nil {
		r.current.close()
		r.current = nil
	}
	r.idx = len(r.files)
}

// next returns the next row from the run (across all files in order).
func (r *runReader) next() (int64, binaryrow.RowKind, arrow.RecordBatch, int, bool) {
	for r.idx < len(r.files) && r.err == nil {
		if r.current == nil {
			r.current, r.err = r.open(r.files[r.idx])
			if r.err != nil {
				return 0, 0, nil, 0, false
			}
		}
		seq, kind, batch, row, ok := r.current.advance()
		if ok {
			return seq, kind, batch, row, true
		}
		r.err = r.current.rdr.Err()
		r.current.close()
		r.current = nil
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
	runs      []*runReader
	keyFields []schema.DataField
	exhausted bool
}

func newSectionMerger(runs []*runReader, keyFields []schema.DataField) (*sectionMerger, error) {
	h := &mergeHeap{}
	heap.Init(h)
	m := &sectionMerger{h: h, runs: runs, keyFields: keyFields}

	// Seed the heap with the first row from each run.
	for _, run := range runs {
		run := run // capture
		seq, kind, batch, row, ok := run.next()
		if !ok {
			if run.err != nil {
				m.close()
				return nil, run.err
			}
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
	return m, nil
}

func (m *sectionMerger) done() bool { return m.exhausted }

func (m *sectionMerger) close() {
	for m.h.Len() > 0 {
		heap.Pop(m.h).(*heapEntry).batch.Release()
	}
	for _, run := range m.runs {
		run.close()
	}
	m.exhausted = true
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
		if e.run.err != nil {
			for _, entry := range group {
				entry.batch.Release()
			}
			m.close()
			return nil, e.run.err
		}
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
		winner.batch.Release()    // release the winner too since we won't use it
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
