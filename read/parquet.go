package read

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

type rowGroupSpan struct {
	index        int
	offset, rows int64
}

// selectRowGroups consults only the footer; it never reads column data. PK
// callers must pass only key predicates so updates/tombstones survive merging.
func selectRowGroups(pq *file.Reader, fileSchema *arrow.Schema, fields []schema.DataField, filter *predicate.Predicate) []rowGroupSpan {
	if filter == nil {
		groups := make([]rowGroupSpan, 0, pq.NumRowGroups())
		var offset int64
		for i := 0; i < pq.NumRowGroups(); i++ {
			rows := pq.MetaData().RowGroup(i).NumRows()
			if rows > 0 {
				groups = append(groups, rowGroupSpan{index: i, offset: offset, rows: rows})
			}
			offset += rows
		}
		return groups
	}
	columns := make(map[string]int)
	for i := 0; i < pq.MetaData().Schema.NumColumns(); i++ {
		col := pq.MetaData().Schema.Column(i)
		if len(col.ColumnPath()) == 1 && col.MaxRepetitionLevel() == 0 {
			columns[col.Name()] = i
		}
	}
	types := make(map[string]arrow.DataType)
	for _, f := range filterFields(filter, fields) {
		expected, err := schema.ToArrowField(f)
		indices := fileSchema.FieldIndices(f.Name)
		if err == nil && len(indices) == 1 {
			actual := fileSchema.Field(indices[0]).Type
			if arrowTypesCompatible(actual, expected.Type) {
				types[f.Name] = actual
			}
		}
	}
	groups := make([]rowGroupSpan, 0, pq.NumRowGroups())
	var offset int64
	for i := 0; i < pq.NumRowGroups(); i++ {
		rg := pq.MetaData().RowGroup(i)
		cache := make(map[string]predicate.ColumnStats)
		matches := predicate.TestByColumnStats(filter, func(p *predicate.Predicate) predicate.ColumnStats {
			if stats, ok := cache[p.FieldName]; ok {
				return stats
			}
			stats := predicate.ColumnStats{RowCount: rg.NumRows()}
			if index, ok := columns[p.FieldName]; ok && types[p.FieldName] != nil {
				if chunk, err := rg.ColumnChunk(index); err == nil {
					if raw, err := chunk.Statistics(); err == nil && raw != nil {
						stats = parquetColumnStats(raw, types[p.FieldName], rg.NumRows())
					}
				}
			}
			cache[p.FieldName] = stats
			return stats
		})
		if matches && rg.NumRows() > 0 {
			groups = append(groups, rowGroupSpan{index: i, offset: offset, rows: rg.NumRows()})
		}
		offset += rg.NumRows()
	}
	return groups
}

func parquetColumnStats(raw metadata.TypedStatistics, typ arrow.DataType, rows int64) predicate.ColumnStats {
	stats := predicate.ColumnStats{RowCount: rows}
	if raw.HasNullCount() && raw.NullCount() >= 0 && raw.NullCount() <= rows {
		n := raw.NullCount()
		stats.NullCount = &n
	}
	if !raw.HasMinMax() {
		return stats
	}
	// Do not reinterpret unsigned, decimal, timestamp, or nested statistics as
	// their physical integer/byte types. Floating bounds are also kept unknown:
	// writers can omit NaNs from min/max, and the footer has no NaN count with
	// which to prove that a predicate cannot match. Null counts remain useful.
	switch typ.ID() {
	case arrow.BOOL:
		if s, ok := raw.(*metadata.BooleanStatistics); ok {
			stats.Min, stats.Max = s.Min(), s.Max()
		}
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.DATE32:
		if s, ok := raw.(*metadata.Int32Statistics); ok {
			stats.Min, stats.Max = s.Min(), s.Max()
		}
	case arrow.INT64:
		if s, ok := raw.(*metadata.Int64Statistics); ok {
			stats.Min, stats.Max = s.Min(), s.Max()
		}
	case arrow.STRING, arrow.LARGE_STRING:
		if s, ok := raw.(*metadata.ByteArrayStatistics); ok {
			stats.Min, stats.Max = string(s.Min()), string(s.Max())
		}
	}
	return stats
}

// parquetRecordReader splits decoded batches at original row-group boundaries.
// The underlying Arrow reader may concatenate non-adjacent selected groups in
// one batch; RowOffset must still identify the original physical file position
// so deletion vectors never shift onto the wrong rows.
type parquetRecordReader struct {
	refs      atomic.Int64
	base      array.RecordReader
	spans     []rowGroupSpan
	span      int
	spanRow   int64
	source    arrow.RecordBatch // borrowed from base
	sourceRow int64
	current   arrow.RecordBatch
	rowOffset int64
	err       error
}

func newParquetRecordReader(ctx context.Context, pq *file.Reader, fr *pqarrow.FileReader, columns []int, fields []schema.DataField, filter *predicate.Predicate) (*parquetRecordReader, error) {
	var sc *arrow.Schema
	if filter != nil {
		var err error
		sc, err = fr.Schema()
		if err != nil {
			return nil, err
		}
	}
	spans := selectRowGroups(pq, sc, fields, filter)
	indices := make([]int, len(spans)) // non-nil empty means no groups, not all
	for i, span := range spans {
		indices[i] = span.index
	}
	base, err := fr.GetRecordReader(ctx, columns, indices)
	if err != nil {
		return nil, err
	}
	r := &parquetRecordReader{base: base, spans: spans}
	r.refs.Store(1)
	return r, nil
}

func (r *parquetRecordReader) Schema() *arrow.Schema          { return r.base.Schema() }
func (r *parquetRecordReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *parquetRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *parquetRecordReader) RowOffset() int64               { return r.rowOffset }
func (r *parquetRecordReader) Err() error                     { return r.err }
func (r *parquetRecordReader) Retain()                        { r.refs.Add(1) }
func (r *parquetRecordReader) Release() {
	if r.refs.Add(-1) != 0 {
		return
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	r.source = nil
	r.base.Release()
}

func (r *parquetRecordReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.err != nil {
		return false
	}
	for r.source == nil || r.sourceRow == r.source.NumRows() {
		if !r.base.Next() {
			r.source = nil
			r.err = r.base.Err()
			return false
		}
		r.source = r.base.RecordBatch()
		r.sourceRow = 0
	}
	for r.span < len(r.spans) && r.spanRow == r.spans[r.span].rows {
		r.span++
		r.spanRow = 0
	}
	if r.span >= len(r.spans) {
		r.err = fmt.Errorf("read: decoded more rows than selected row-group metadata")
		return false
	}
	span := r.spans[r.span]
	n := min(r.source.NumRows()-r.sourceRow, span.rows-r.spanRow)
	r.rowOffset = span.offset + r.spanRow
	r.current = r.source.NewSlice(r.sourceRow, r.sourceRow+n)
	r.sourceRow += n
	r.spanRow += n
	return true
}
