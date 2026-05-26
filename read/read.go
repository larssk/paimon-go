package read

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/internal/deletionvector"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)


// TableRead executes the read of splits and produces Arrow data.
type TableRead struct {
	rb *ReadBuilder
}

// ToArrowReader returns an array.RecordReader that streams record batches from all splits.
// The caller is responsible for calling Release() on each record and Release() on the reader.
func (tr *TableRead) ToArrowReader(ctx context.Context, splits []DataSplit) (array.RecordReader, error) {
	readFields := tr.rb.readFields()
	arrowSchema, err := schema.ToArrowSchema(readFields)
	if err != nil {
		return nil, fmt.Errorf("read: build arrow schema: %w", err)
	}

	return &multiSplitReader{
		ctx:         ctx,
		tr:          tr,
		splits:      splits,
		readFields:  readFields,
		arrowSchema: arrowSchema,
		alloc:       memory.NewGoAllocator(),
	}, nil
}

// multiSplitReader iterates over splits, dispatching each to either the append
// path (splitRecordReader) or the PK merge path (sortMergeReader).
type multiSplitReader struct {
	ctx         context.Context
	tr          *TableRead
	splits      []DataSplit
	readFields  []schema.DataField
	arrowSchema *arrow.Schema
	alloc       memory.Allocator

	splitIdx int
	current  array.RecordReader // active sub-reader for the current split

	currentBatch arrow.RecordBatch
	currentErr   error
}

func (m *multiSplitReader) Schema() *arrow.Schema { return m.arrowSchema }
func (m *multiSplitReader) Retain()               {}
func (m *multiSplitReader) Release() {
	if m.currentBatch != nil {
		m.currentBatch.Release()
		m.currentBatch = nil
	}
	if m.current != nil {
		m.current.Release()
		m.current = nil
	}
}
func (m *multiSplitReader) RecordBatch() arrow.RecordBatch { return m.currentBatch }
func (m *multiSplitReader) Record() arrow.RecordBatch      { return m.currentBatch }
func (m *multiSplitReader) Err() error                     { return m.currentErr }

func (m *multiSplitReader) Next() bool {
	if m.currentBatch != nil {
		m.currentBatch.Release()
		m.currentBatch = nil
	}
	for {
		if m.current != nil {
			if m.current.Next() {
				m.currentBatch = m.current.RecordBatch()
				m.currentBatch.Retain()
				return true
			}
			if err := m.current.Err(); err != nil {
				m.currentErr = err
				return false
			}
			m.current.Release()
			m.current = nil
		}
		if m.splitIdx >= len(m.splits) {
			return false
		}
		split := m.splits[m.splitIdx]
		m.splitIdx++
		rdr, err := m.openSplit(split)
		if err != nil {
			m.currentErr = err
			return false
		}
		m.current = rdr
	}
}

func (m *multiSplitReader) openSplit(split DataSplit) (array.RecordReader, error) {
	if split.NeedsMerge {
		s := m.tr.rb.tbl.GetSchema()
		return newSortMergeReader(
			m.ctx,
			m.tr.rb.tbl,
			split,
			s.PrimaryKeyFields(),
			m.readFields,
			m.arrowSchema,
			m.alloc,
			m.tr.rb.filter,
		)
	}
	// Append-only path: wrap in a single-split splitRecordReader.
	return &splitRecordReader{
		ctx:         m.ctx,
		tr:          m.tr,
		splits:      []DataSplit{split},
		readFields:  m.readFields,
		arrowSchema: m.arrowSchema,
		alloc:       m.alloc,
	}, nil
}

// ToArrow reads all splits and materialises the result as a single arrow.Table
// held entirely in memory.
//
// For large tables or streaming workloads prefer [TableRead.ToArrowReader],
// which streams record batches one at a time without accumulating all data.
//
// The caller must call Release() on the returned table when done to free memory.
func (tr *TableRead) ToArrow(ctx context.Context, splits []DataSplit) (arrow.Table, error) {
	rr, err := tr.ToArrowReader(ctx, splits)
	if err != nil {
		return nil, err
	}
	defer rr.Release()

	var records []arrow.RecordBatch
	for rr.Next() {
		rec := rr.RecordBatch()
		rec.Retain()
		records = append(records, rec)
	}
	if err := rr.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		s := rr.Schema()
		return array.NewTableFromRecords(s, nil), nil
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	return array.NewTableFromRecords(rr.Schema(), records), nil
}

// splitRecordReader implements array.RecordReader over a list of DataSplits.
type splitRecordReader struct {
	ctx         context.Context
	tr          *TableRead
	splits      []DataSplit
	readFields  []schema.DataField
	arrowSchema *arrow.Schema
	alloc       memory.Allocator

	splitIdx int
	fileIdx  int

	current    arrow.RecordBatch
	currentErr error
	fileRdr    *pqarrow.FileReader
	rdrClose   io.Closer
	rowRdr     array.RecordReader

	// Deletion vector state — reset per file in openFile.
	dvBitmap     *roaring.Bitmap // nil if no DV for this file
	fileRowOffset int64          // absolute row position of the start of the current batch
}

func (r *splitRecordReader) Schema() *arrow.Schema { return r.arrowSchema }

func (r *splitRecordReader) Retain()  {}
func (r *splitRecordReader) Release() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.closeCurrentFile()
}

func (r *splitRecordReader) RecordBatch() arrow.RecordBatch { return r.current }

// Record is deprecated but required by the interface; delegates to RecordBatch.
func (r *splitRecordReader) Record() arrow.RecordBatch { return r.current }
func (r *splitRecordReader) Err() error                { return r.currentErr }

func (r *splitRecordReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	for {
		// Try to get the next batch from the current file reader.
		if r.rowRdr != nil {
			if r.rowRdr.Next() {
				rec := r.rowRdr.RecordBatch()
				// Project to read schema (handles missing columns as nulls).
				projected, err := r.projectRecord(r.ctx, rec)
				if err != nil {
					r.currentErr = err
					return false
				}

				// Deletion vector filtering: remove rows whose absolute position
				// within the file is set in the DV bitmap.
				batchRows := projected.NumRows()
				if r.dvBitmap != nil {
					projected = applyDeletionVector(projected, r.dvBitmap, r.fileRowOffset, r.alloc)
				}
				r.fileRowOffset += batchRows

				if projected == nil {
					continue // all rows deleted; advance to next batch
				}

				// Row-level filter: discard non-matching rows.
				if r.tr.rb.filter != nil {
					filtered := filterBatch(r.tr.rb.filter, projected, r.alloc)
					projected.Release()
					if filtered == nil {
						continue // all rows filtered out; advance to next batch
					}
					projected = filtered
				}
				r.current = projected
				return true
			}
			r.closeCurrentFile()
		}

		// Advance to the next file.
		if r.splitIdx >= len(r.splits) {
			return false
		}
		split := r.splits[r.splitIdx]
		if r.fileIdx >= len(split.Files) {
			r.splitIdx++
			r.fileIdx = 0
			continue
		}
		fm := split.Files[r.fileIdx]
		r.fileIdx++

		if err := r.openFile(split, fm); err != nil {
			r.currentErr = err
			return false
		}
	}
}

func (r *splitRecordReader) openFile(split DataSplit, fm manifest.DataFileMeta) error {
	tbl := r.tr.rb.tbl
	var filePath string
	if fm.ExternalPath != nil {
		filePath = *fm.ExternalPath
	} else {
		var partRow *binaryrow.BinaryRow
		if split.Partition != nil {
			partRow = split.Partition.Partition
		}
		filePath = tbl.DataFilePath(
			partRow,
			tbl.GetSchema().PartitionFields(),
			split.Bucket,
			fm.FileName,
		)
	}

	if !isParquet(filePath) {
		return fmt.Errorf("read: unsupported file format for %q (only parquet supported in v1)", filePath)
	}

	rc, err := tbl.GetIO().Open(r.ctx, filePath)
	if err != nil {
		return fmt.Errorf("read: open %s: %w", filePath, err)
	}

	// pqarrow requires a parquet.ReaderAtSeeker. Wrap the ReadCloser.
	ras, err := toReaderAtSeeker(rc)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: wrap reader for %s: %w", filePath, err)
	}
	r.rdrClose = rc

	pqReader, err := file.NewParquetReader(ras)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: open parquet %s: %w", filePath, err)
	}

	// Build column indices for projection.
	colIndices := r.parquetColumnIndices(pqReader)

	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		BatchSize: 65536,
	}, r.alloc)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: create arrow reader for %s: %w", filePath, err)
	}

	rowRdr, err := arrowReader.GetRecordReader(r.ctx, colIndices, nil)
	if err != nil {
		rc.Close()
		return fmt.Errorf("read: get record reader for %s: %w", filePath, err)
	}

	r.fileRdr = arrowReader
	r.rowRdr = rowRdr
	r.dvBitmap = nil
	r.fileRowOffset = 0

	// Decode deletion vector for this file if one exists.
	if dvMeta, ok := split.DeletionFiles[fm.FileName]; ok {
		indexData, err := tbl.GetIO().ReadAll(r.ctx, dvMeta.IndexFilePath)
		if err != nil {
			log.Printf("read: load deletion vector index for %s: %v", fm.FileName, err)
		} else {
			start := dvMeta.Offset
			end := start + int64(dvMeta.Length)
			if end > int64(len(indexData)) {
				end = int64(len(indexData))
			}
			bm, err := deletionvector.Decode(indexData[start:end])
			if err != nil {
				if _, ok := err.(*deletionvector.ErrUnsupportedMagic); ok {
					log.Printf("read: skipping unsupported deletion vector for %s: %v", fm.FileName, err)
				} else {
					log.Printf("read: decode deletion vector for %s: %v", fm.FileName, err)
				}
			} else {
				r.dvBitmap = bm
			}
		}
	}
	return nil
}

// parquetColumnIndices maps the read fields to Parquet column indices.
// Fields not present in the file are included as -1 (handled in projectRecord).
func (r *splitRecordReader) parquetColumnIndices(pqReader *file.Reader) []int {
	parquetSchema := pqReader.MetaData().Schema
	nameToCol := make(map[string]int, parquetSchema.NumColumns())
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		nameToCol[parquetSchema.Column(i).Name()] = i
	}
	indices := make([]int, 0, len(r.readFields))
	for _, f := range r.readFields {
		if ci, ok := nameToCol[f.Name]; ok {
			indices = append(indices, ci)
		}
	}
	return indices
}

// projectRecord re-orders and/or pads a record to match the read schema exactly.
// Columns present in the file but not the read schema are dropped.
// Columns in the read schema but missing from the file are filled with null arrays.
// When a column's physical type is compatible but not identical (e.g. timestamp with
// vs without timezone), the column data is reused and the schema field is overridden
// to match what the file actually contains, avoiding Arrow type-mismatch panics.
func (r *splitRecordReader) projectRecord(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if rec.Schema().Equal(r.arrowSchema) {
		rec.Retain()
		return rec, nil
	}

	fileSchema := rec.Schema()
	cols := make([]arrow.Array, len(r.readFields))
	// Build the effective schema: use the file's field type when it differs from
	// the read schema but is physically compatible (same base type + unit).
	effectiveFields := make([]arrow.Field, len(r.readFields))
	n := rec.NumRows()

	for i, f := range r.readFields {
		wantField, err := schema.ToArrowField(f)
		if err != nil {
			return nil, err
		}
		colIdx := fileSchema.FieldIndices(f.Name)
		if len(colIdx) > 0 {
			col := rec.Column(colIdx[0])
			col.Retain()
			cols[i] = col
			// Use the file's actual field type to avoid type-mismatch panics.
			fileField := fileSchema.Field(colIdx[0])
			if arrowTypesCompatible(fileField.Type, wantField.Type) {
				// Same physical representation — use file type, keep our name.
				effectiveFields[i] = fileField
				effectiveFields[i].Name = f.Name
			} else if fileField.Type.ID() == wantField.Type.ID() {
				// Same logical type family but different parameters
				// (e.g., timestamp[ms] vs timestamp[us]).
				// Cast the column to the target type.
				casted, castErr := compute.CastToType(ctx, col, wantField.Type)
				if castErr == nil {
					col.Release()
					cols[i] = casted
					effectiveFields[i] = wantField
				} else {
					// Cast failed — use file type as-is (best effort, may fail later).
					effectiveFields[i] = fileField
					effectiveFields[i].Name = f.Name
				}
			} else {
				effectiveFields[i] = wantField
			}
		} else {
			// Field missing from file: synthesize a null array.
			nullArr := array.MakeArrayOfNull(r.alloc, wantField.Type, int(n))
			cols[i] = nullArr
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
	result := array.NewRecord(effectiveSchema, cols, n)
	return result, nil
}

// arrowTypesCompatible returns true when two Arrow types have the same physical
// representation and can be safely used interchangeably (e.g. timestamp[us] vs
// timestamp[us, tz=UTC] — same unit, different timezone annotation).
func arrowTypesCompatible(a, b arrow.DataType) bool {
	if a.ID() != b.ID() {
		return false
	}
	switch at := a.(type) {
	case *arrow.TimestampType:
		bt, ok := b.(*arrow.TimestampType)
		return ok && at.Unit == bt.Unit
	}
	return arrow.TypeEqual(a, b)
}

func (r *splitRecordReader) closeCurrentFile() {
	if r.rowRdr != nil {
		r.rowRdr.Release()
		r.rowRdr = nil
	}
	r.fileRdr = nil
	if r.rdrClose != nil {
		r.rdrClose.Close()
		r.rdrClose = nil
	}
}

// isParquet returns true if the file name indicates a Parquet file.
func isParquet(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".parquet") ||
		strings.HasSuffix(lower, ".snappy.parquet") ||
		strings.HasSuffix(lower, ".gz.parquet") ||
		strings.HasSuffix(lower, ".zstd.parquet") ||
		strings.HasSuffix(lower, ".lz4.parquet") ||
		// Paimon sometimes uses just ".parquet" or "-{uuid}.parquet"
		strings.Contains(lower, ".parquet")
}

// toReaderAtSeeker wraps an io.ReadCloser into a parquet ReaderAtSeeker by reading all bytes.
// For streaming GCS / local files we buffer into memory; for large files a seekable wrapper
// would be preferable, but this keeps the implementation simple for v1.
type memReaderAt struct {
	data []byte
	pos  int64
}

func toReaderAtSeeker(rc io.ReadCloser) (*memReaderAt, error) {
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return &memReaderAt{data: data}, nil
}

func (m *memReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memReaderAt) Read(p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *memReaderAt) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = m.pos + offset
	case io.SeekEnd:
		abs = int64(len(m.data)) + offset
	default:
		return 0, fmt.Errorf("memReaderAt: invalid whence %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("memReaderAt: negative seek")
	}
	m.pos = abs
	return abs, nil
}

// applyDeletionVector filters a record batch by removing rows whose absolute
// position within the file (fileRowOffset + batch row index) is set in bm.
// Returns nil if all rows are deleted. Returns rec unchanged (retained) if no
// rows are deleted. Otherwise returns a new compacted batch.
func applyDeletionVector(rec arrow.RecordBatch, bm *roaring.Bitmap, fileRowOffset int64, alloc memory.Allocator) arrow.RecordBatch {
	n := int(rec.NumRows())
	if n == 0 {
		return nil
	}

	sel := make([]bool, n)
	matchCount := 0
	for i := 0; i < n; i++ {
		pos := uint32(fileRowOffset) + uint32(i)
		if !bm.Contains(pos) {
			sel[i] = true
			matchCount++
		}
	}
	if matchCount == 0 {
		rec.Release()
		return nil
	}
	if matchCount == n {
		return rec // no rows deleted in this batch
	}

	// Compact all columns.
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		cols[i] = compactColumn(rec.Column(i), sel, matchCount, alloc)
	}
	rec.Release()
	out := array.NewRecord(rec.Schema(), cols, int64(matchCount))
	for _, c := range cols {
		c.Release()
	}
	return out
}

// filterBatch returns a new RecordBatch containing only the rows for which p
// evaluates to true. Returns nil if no rows match. If all rows match the
// original batch is retained and returned unchanged (no copy).
func filterBatch(p *predicate.Predicate, rec arrow.RecordBatch, alloc memory.Allocator) arrow.RecordBatch {
	n := int(rec.NumRows())
	if n == 0 {
		return nil
	}

	// Build selection mask.
	sel := make([]bool, n)
	matchCount := 0
	for i := 0; i < n; i++ {
		if predicate.EvalRow(p, rec, i) {
			sel[i] = true
			matchCount++
		}
	}
	if matchCount == 0 {
		return nil
	}
	if matchCount == n {
		rec.Retain()
		return rec
	}

	// Compact: copy selected rows into new arrays.
	s := rec.Schema()
	cols := make([]arrow.Array, rec.NumCols())
	for ci := 0; ci < int(rec.NumCols()); ci++ {
		src := rec.Column(ci)
		cols[ci] = compactColumn(src, sel, matchCount, alloc)
	}
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	return array.NewRecord(s, cols, int64(matchCount))
}

// compactColumn copies the rows indicated by sel into a new array.
func compactColumn(src arrow.Array, sel []bool, matchCount int, alloc memory.Allocator) arrow.Array {
	switch c := src.(type) {
	case *array.Boolean:
		b := array.NewBooleanBuilder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Int8:
		b := array.NewInt8Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Int16:
		b := array.NewInt16Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Int32:
		b := array.NewInt32Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Int64:
		b := array.NewInt64Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Float32:
		b := array.NewFloat32Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Float64:
		b := array.NewFloat64Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.String:
		b := array.NewStringBuilder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.LargeString:
		b := array.NewLargeStringBuilder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Timestamp:
		dt := src.DataType().(*arrow.TimestampType)
		b := array.NewTimestampBuilder(alloc, dt)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	case *array.Date32:
		b := array.NewDate32Builder(alloc)
		b.Reserve(matchCount)
		for i, ok := range sel {
			if ok {
				if c.IsNull(i) {
					b.AppendNull()
				} else {
					b.Append(c.Value(i))
				}
			}
		}
		return b.NewArray()
	default:
		// Unsupported type: return a null array of the same length as a safe fallback.
		return array.MakeArrayOfNull(alloc, src.DataType(), matchCount)
	}
}
