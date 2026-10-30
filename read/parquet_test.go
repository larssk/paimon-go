package read

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

func writeRowGroups(t testing.TB, data arrow.Table, rows int64, props ...parquet.WriterProperty) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(data.Schema(), &buf, parquet.NewWriterProperties(props...), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteTable(data, rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openTestParquet(t testing.TB, data []byte) *file.Reader {
	t.Helper()
	pq, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pq.Close() })
	return pq
}

func assertGroupsNotRead(t *testing.T, data []byte, reads []readRange, skipped ...int) {
	t.Helper()
	pq := openTestParquet(t, data)
	for _, rg := range skipped {
		group := pq.MetaData().RowGroup(rg)
		for i := 0; i < group.NumColumns(); i++ {
			chunk, err := group.ColumnChunk(i)
			if err != nil {
				t.Fatal(err)
			}
			start := chunk.DataPageOffset()
			if chunk.HasDictionaryPage() {
				start = min(start, chunk.DictionaryPageOffset())
			}
			end := start + chunk.TotalCompressedSize()
			for _, read := range reads {
				if read.offset < end && read.offset+int64(read.length) > start {
					t.Fatalf("read [%d,%d) touched skipped row group %d column %d [%d,%d)", read.offset, read.offset+int64(read.length), rg, i, start, end)
				}
			}
		}
	}
}

func TestParquetRowGroupPruning_OnlyMatchingGroupsRead(t *testing.T) {
	const n, groupSize = 4096, 256
	ids, vals := make([]int32, n), make([]int64, n)
	seqs, kinds := make([]int64, n), make([]int8, n)
	for i := range ids {
		ids[i], vals[i], seqs[i] = int32(i), int64(i)*10, 1
	}
	for _, pk := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "primary_key"}[pk], func(t *testing.T) {
			s := paimonSchema()
			data := buildArrowTable(t, ids, vals)
			if pk {
				data.Release()
				data = buildPKArrowTable(t, ids, seqs, kinds, vals)
				s = pkSchema()
			}
			defer data.Release()
			content := writeRowGroups(t, data, groupSize)
			fio := newTrackedFileIO()
			fio.register("data/groups.parquet", content)
			p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(2050))
			rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, nil).WithFilter(p)
			split := makeFileSplit("groups.parquet")
			split.NeedsMerge = pk
			split.Files[0].KeyStats = intStats(0, n-1)
			result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{split})
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()
			if result.NumRows() != 1 || result.Column(0).Data().Chunks()[0].(*array.Int32).Value(0) != 2050 {
				t.Fatalf("incorrect filtered rows: %v", result)
			}
			var skipped []int
			for i := 0; i < n/groupSize; i++ {
				if i != 2050/groupSize {
					skipped = append(skipped, i)
				}
			}
			assertGroupsNotRead(t, content, fio.ranges, skipped...)
			t.Logf("complete lookup read %d/%d bytes", fio.bytesRead, len(content))
		})
	}
}

func TestParquetRowGroupPruning_LogicalPredicatesAndMissingStats(t *testing.T) {
	data := buildArrowTable(t, []int32{0, 1, 10, 11, 20, 21}, []int64{0, 1, 10, 11, 20, 21})
	defer data.Release()
	pb := predicate.NewBuilder(paimonSchema().Fields)
	id, _ := pb.Equal("id", int32(10))
	unknown, _ := predicate.NewBuilder(append(paimonSchema().Fields, schema.DataField{Name: "missing", Type: schema.DataType{Type: "INT"}})).Equal("missing", int32(1))
	for _, tc := range []struct {
		name  string
		p     *predicate.Predicate
		props []parquet.WriterProperty
		want  []int
	}{
		{"equal", id, nil, []int{1}},
		{"and_unknown", predicate.And(id, unknown), nil, []int{1}},
		{"or_unknown", predicate.Or(id, unknown), nil, []int{0, 1, 2}},
		{"not", predicate.Not(id), nil, []int{0, 1, 2}},
		{"no_stats", id, []parquet.WriterProperty{parquet.WithStats(false)}, []int{0, 1, 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pq := openTestParquet(t, writeRowGroups(t, data, 2, tc.props...))
			fr, err := pqarrow.NewFileReader(pq, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
			if err != nil {
				t.Fatal(err)
			}
			sc, _ := fr.Schema()
			var got []int
			for _, span := range selectRowGroups(pq, sc, paimonSchema().Fields, tc.p) {
				got = append(got, span.index)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("want row groups %v, got %v", tc.want, got)
			}
		})
	}
}

func TestParquetRowGroupPruning_NoMatchReadsFooterOnly(t *testing.T) {
	data := buildArrowTable(t, []int32{1, 2, 3, 4}, []int64{10, 20, 30, 40})
	defer data.Release()
	content := writeRowGroups(t, data, 2)
	fio := newTrackedFileIO()
	fio.register("data/groups.parquet", content)
	p, _ := predicate.NewBuilder(paimonSchema().Fields).Equal("id", int32(100))
	rb := newReadBuilderFromIface(&readTable{sch: paimonSchema(), io: fio}, nil).WithFilter(p)
	result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makeFileSplit("groups.parquet")})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if result.NumRows() != 0 {
		t.Fatalf("want no rows, got %d", result.NumRows())
	}
	assertGroupsNotRead(t, content, fio.ranges, 0, 1)
}

func TestParquetRowGroupPruning_DeletionVectorsKeepPhysicalOffsets(t *testing.T) {
	ids, vals := make([]int32, 12), make([]int64, 12)
	for i := range ids {
		ids[i], vals[i] = int32(i), int64(i)
	}
	data := buildArrowTable(t, ids, vals)
	defer data.Release()
	content := writeRowGroups(t, data, 3)
	fio := newTrackedFileIO()
	fio.register("data/groups.parquet", content)
	dv := buildDVRecord(t, 3, 10)
	fio.register("index/dv", dv)
	pb := predicate.NewBuilder(paimonSchema().Fields)
	lo, _ := pb.GreaterOrEqual("id", int32(3))
	hi, _ := pb.LessOrEqual("id", int32(5))
	last, _ := pb.Equal("id", int32(10))
	p := predicate.Or(predicate.And(lo, hi), last)
	rb := newReadBuilderFromIface(&readTable{sch: paimonSchema(), io: fio}, nil).WithFilter(p)
	split := makeFileSplit("groups.parquet")
	split.DeletionFiles = map[string]manifest.DeletionVectorMeta{"groups.parquet": {IndexFilePath: "index/dv", Length: int32(len(dv))}}
	rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	got := collectInt32Col(t, rr, "id")
	if rr.Err() != nil {
		t.Fatal(rr.Err())
	}
	if !reflect.DeepEqual(got, []int32{4, 5}) {
		t.Fatalf("deletion vectors shifted across skipped row groups: %v", got)
	}
	assertGroupsNotRead(t, content, fio.ranges, 0, 2)
}

func TestParquetRowGroupPruning_DeletionVectorsAcrossLargeBatches(t *testing.T) {
	const groupSize, n = 70000, 210000
	ids, vals := make([]int32, n), make([]int64, n)
	for i := range ids {
		ids[i], vals[i] = int32(i), int64(i)
	}
	data := buildArrowTable(t, ids, vals)
	defer data.Release()
	content := writeRowGroups(t, data, groupSize)
	fio := newTrackedFileIO()
	fio.register("data/groups.parquet", content)
	deleted := []uint32{0, 65536, 140000, 205536}
	dv := buildDVRecord(t, deleted...)
	fio.register("index/dv", dv)
	pb := predicate.NewBuilder(paimonSchema().Fields)
	first, _ := pb.LessThan("id", int32(groupSize))
	last, _ := pb.GreaterOrEqual("id", int32(groupSize*2))
	rb := newReadBuilderFromIface(&readTable{sch: paimonSchema(), io: fio}, nil).WithFilter(predicate.Or(first, last))
	split := makeFileSplit("groups.parquet")
	split.DeletionFiles = map[string]manifest.DeletionVectorMeta{"groups.parquet": {IndexFilePath: "index/dv", Length: int32(len(dv))}}
	rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	got := collectInt32Col(t, rr, "id")
	if err := rr.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != groupSize*2-len(deleted) {
		t.Fatalf("wrong row count: %d", len(got))
	}
	for _, id := range got {
		for _, deleted := range deleted {
			if id == int32(deleted) {
				t.Fatalf("deleted physical row %d leaked", id)
			}
		}
	}
	assertGroupsNotRead(t, content, fio.ranges, 1)
}

func TestParquetRowGroupPruning_LogicalScalarTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()
	for _, tc := range []struct {
		name       string
		typ        arrow.DataType
		paimonType string
		values     []interface{}
		literal    interface{}
	}{
		{"string", arrow.BinaryTypes.String, "STRING", []interface{}{"a", "b", "x", "y"}, "x"},
		{"date", arrow.FixedWidthTypes.Date32, "DATE", []interface{}{arrow.Date32(1), arrow.Date32(2), arrow.Date32(20000), arrow.Date32(20001)}, int32(20000)},
		{"boolean", arrow.FixedWidthTypes.Boolean, "BOOLEAN", []interface{}{false, false, true, true}, true},
		{"float", arrow.PrimitiveTypes.Float64, "DOUBLE", []interface{}{float64(1), float64(2), float64(10), float64(11)}, float64(10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewBuilder(alloc, tc.typ)
			defer builder.Release()
			for _, v := range tc.values {
				switch b := builder.(type) {
				case *array.StringBuilder:
					b.Append(v.(string))
				case *array.Date32Builder:
					b.Append(v.(arrow.Date32))
				case *array.BooleanBuilder:
					b.Append(v.(bool))
				case *array.Float64Builder:
					b.Append(v.(float64))
				}
			}
			arr := builder.NewArray()
			defer arr.Release()
			sc := arrow.NewSchema([]arrow.Field{{Name: "key", Type: tc.typ, Nullable: true}}, nil)
			data := array.NewTableFromSlice(sc, [][]arrow.Array{{arr}})
			defer data.Release()
			fields := []schema.DataField{{Name: "key", Type: schema.DataType{Type: tc.paimonType, Nullable: true}}}
			filter, _ := predicate.NewBuilder(fields).Equal("key", tc.literal)
			pq := openTestParquet(t, writeRowGroups(t, data, 2))
			groups := selectRowGroups(pq, sc, fields, filter)
			if tc.name == "float" {
				if len(groups) != 2 {
					t.Fatalf("floating statistics cannot exclude NaN matches: %+v", groups)
				}
			} else if len(groups) != 1 || groups[0].index != 1 {
				t.Fatalf("wrong logical-type pruning: %+v", groups)
			}
		})
	}
}

func BenchmarkPKPointLookupRowGroupPruning(b *testing.B) {
	const n = 65536
	ids, vals := make([]int32, n), make([]int64, n)
	seqs, kinds := make([]int64, n), make([]int8, n)
	for i := range ids {
		ids[i], vals[i], seqs[i] = int32(i), int64(i), 1
	}
	data := buildPKArrowTable(b, ids, seqs, kinds, vals)
	defer data.Release()
	content := writeRowGroups(b, data, 1024)
	s := pkSchema()
	point, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(n/2))
	for _, pruning := range []bool{false, true} {
		b.Run(map[bool]string{false: "without_pruning", true: "with_pruning"}[pruning], func(b *testing.B) {
			filter := point
			if !pruning {
				filter = predicate.Not(predicate.Not(point))
			} // equivalent row filter; NOT cannot prune stats
			b.ReportAllocs()
			var readBytes int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fio := newTrackedFileIO()
				fio.register("data/lookup.parquet", content)
				rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, nil).WithFilter(filter)
				result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makePKSplit(manifest.DataFileMeta{FileName: "lookup.parquet"})})
				if err != nil {
					b.Fatal(err)
				}
				if result.NumRows() != 1 {
					b.Fatalf("want one row, got %d", result.NumRows())
				}
				result.Release()
				readBytes += int64(fio.bytesRead)
			}
			b.ReportMetric(float64(readBytes)/float64(b.N), "read-bytes/op")
		})
	}
}

func TestParquetRowGroupPruning_DoesNotResurrectStalePKRows(t *testing.T) {
	for _, kind := range []int8{0, 3} {
		t.Run(map[int8]string{0: "update", 3: "delete"}[kind], func(t *testing.T) {
			old := buildPKArrowTable(t, []int32{1, 2}, []int64{1, 1}, []int8{0, 0}, []int64{10, 11})
			defer old.Release()
			newer := buildPKArrowTable(t, []int32{1, 2}, []int64{2, 2}, []int8{kind, 0}, []int64{20, 21})
			defer newer.Release()
			fio := newTrackedFileIO()
			fio.register("data/old.parquet", writeRowGroups(t, old, 1))
			fio.register("data/new.parquet", writeRowGroups(t, newer, 1))
			s := pkSchema()
			pb := predicate.NewBuilder(s.Fields)
			id, _ := pb.Equal("id", int32(1))
			val, _ := pb.Equal("val", int64(10))
			rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, nil).WithFilter(predicate.And(id, val))
			result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{makePKSplit(
				manifest.DataFileMeta{FileName: "old.parquet", KeyStats: intStats(1, 2)},
				manifest.DataFileMeta{FileName: "new.parquet", KeyStats: intStats(1, 2)},
			)})
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()
			if result.NumRows() != 0 {
				t.Fatalf("mutable row-group pruning exposed an obsolete row: %d rows", result.NumRows())
			}
		})
	}
}
