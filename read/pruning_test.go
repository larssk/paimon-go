package read

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

func statsRow(values ...int64) *binaryrow.BinaryRow {
	n := len(values)
	nulls := binaryrow.NullBitsSize(n)
	data := make([]byte, 4+nulls+8*n)
	binary.BigEndian.PutUint32(data, uint32(n))
	for i, v := range values {
		binary.LittleEndian.PutUint64(data[4+nulls+8*i:], uint64(v))
	}
	r, err := binaryrow.New(data)
	if err != nil {
		panic(err)
	}
	return r
}

func rowStats(values ...int64) manifest.SimpleStats {
	r := statsRow(values...)
	return manifest.SimpleStats{MinValues: r, MaxValues: r}
}

func partitionedSchema() *schema.TableSchema {
	s := intSchema()
	s.Fields = append(s.Fields, schema.DataField{ID: 2, Name: "dt", Type: schema.DataType{Type: "INT"}})
	s.PartitionKeys = []string{"dt"}
	return s
}

func TestPruneManifests_RebindsPartitionPredicates(t *testing.T) {
	s := partitionedSchema()
	pb := predicate.NewBuilder(s.Fields)
	dt, _ := pb.Equal("dt", int32(5))
	id, _ := pb.Equal("id", int32(5))
	for _, tc := range []struct {
		name string
		p    *predicate.Predicate
		want int
	}{
		{"partition", dt, 1},
		{"nonpartition", id, 2},
		{"and", predicate.And(id, dt), 1},
		{"or", predicate.Or(id, dt), 2},
		{"not_partial_and", predicate.Not(predicate.And(id, dt)), 2},
		{"or_partial_and", predicate.Or(predicate.And(id, dt), id), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metas := []manifest.ManifestFileMeta{
				{FileName: "match", NumAddedFiles: 1, PartitionStats: intStats(5, 5)},
				{FileName: "outside", NumAddedFiles: 1, PartitionStats: intStats(100, 100)},
			}
			if got := pruneManifests(metas, tc.p, s); len(got) != tc.want {
				t.Fatalf("want %d manifests, got %d", tc.want, len(got))
			}
		})
	}
}

func TestPruneDataFiles_ExactPartitionWithoutValueStats(t *testing.T) {
	s := partitionedSchema()
	pb := predicate.NewBuilder(s.Fields)
	lo, _ := pb.GreaterOrEqual("dt", int32(5))
	hi, _ := pb.LessOrEqual("dt", int32(6))
	entries := []manifest.ManifestEntry{
		{Partition: buildIntRow(4), File: manifest.DataFileMeta{FileName: "old"}},
		{Partition: buildIntRow(5), File: manifest.DataFileMeta{FileName: "match"}},
		{Partition: buildIntRow(7), File: manifest.DataFileMeta{FileName: "new"}},
		{File: manifest.DataFileMeta{FileName: "unknown"}},
	}
	got := pruneDataFiles(entries, predicate.And(lo, hi), s)
	if len(got) != 2 || got[0].File.FileName != "match" || got[1].File.FileName != "unknown" {
		t.Fatalf("want matching and unknown partitions, got %+v", got)
	}
}

func TestPartitionPruning_DateUsesEpochDays(t *testing.T) {
	s := partitionedSchema()
	s.Fields[2].Type.Type = "DATE"
	pb := predicate.NewBuilder(s.Fields)
	lo, _ := pb.GreaterOrEqual("dt", int32(20000))
	hi, _ := pb.LessOrEqual("dt", int32(20001))
	p := predicate.And(lo, hi)
	metas := []manifest.ManifestFileMeta{
		{FileName: "outside", NumAddedFiles: 1, PartitionStats: intStats(19000, 19001)},
		{FileName: "match", NumAddedFiles: 1, PartitionStats: intStats(20000, 20001)},
	}
	if got := pruneManifests(metas, p, s); len(got) != 1 || got[0].FileName != "match" {
		t.Fatalf("DATE manifest pruning used incompatible values: %+v", got)
	}
	entries := []manifest.ManifestEntry{
		{Partition: buildIntRow(19000)},
		{Partition: buildIntRow(20001)},
	}
	got := pruneDataFiles(entries, p, s)
	if len(got) != 1 {
		t.Fatalf("want one DATE partition, got %d", len(got))
	}
	day, _ := got[0].Partition.GetInt32(0)
	if day != 20001 {
		t.Fatalf("wrong DATE partition: %d", day)
	}
}

func TestPruneDataFiles_KeyRange(t *testing.T) {
	s := pkSchema()
	p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(5))
	entries := []manifest.ManifestEntry{
		{File: manifest.DataFileMeta{FileName: "outside", KeyStats: intStats(100, 200)}},
		{File: manifest.DataFileMeta{FileName: "match", KeyStats: intStats(1, 10)}},
		{File: manifest.DataFileMeta{FileName: "unknown"}},
	}
	got := pruneDataFiles(entries, p, s)
	if len(got) != 2 || got[0].File.FileName != "match" || got[1].File.FileName != "unknown" {
		t.Fatalf("want matching and unknown key ranges, got %+v", got)
	}
}

func TestPlanPruning_UnselectedFilesAreNeverOpened(t *testing.T) {
	fio := newTrackedFileIO()
	data := buildPKArrowTable(t, []int32{5}, []int64{1}, []int8{0}, []int64{50})
	defer data.Release()
	fio.register("data/match.parquet", writeParquet(t, data))
	s := pkSchema()
	p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(5))
	mr := &stubManifestReader{entriesResult: []manifest.ManifestEntry{
		{File: manifest.DataFileMeta{FileName: "match.parquet", MinKey: statsRow(5), MaxKey: statsRow(5)}},
		{File: manifest.DataFileMeta{FileName: "outside.parquet", MinKey: statsRow(100), MaxKey: statsRow(200)}},
	}}
	rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, mr).WithFilter(p)
	plan, err := rb.NewScan().Plan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, err := rb.NewRead().ToArrow(context.Background(), plan.Splits)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if result.NumRows() != 1 || fio.opens["data/match.parquet"] != 1 || fio.opens["data/outside.parquet"] != 0 {
		t.Fatalf("pruning did not precede data reads: rows=%d opens=%v", result.NumRows(), fio.opens)
	}
}

func TestPruneDataFiles_TrimmedKeyOrder(t *testing.T) {
	s := partitionedSchema()
	s.PrimaryKeys = []string{"dt", "id"}
	p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(5))
	entries := []manifest.ManifestEntry{{File: manifest.DataFileMeta{KeyStats: intStats(100, 200)}}}
	if got := pruneDataFiles(entries, p, s); len(got) != 0 {
		t.Fatalf("partition key must not shift stored id bounds: got %d files", len(got))
	}
}

func TestPruneDataFiles_LexicographicSuffixIsNotColumnStats(t *testing.T) {
	s := pkSchema()
	s.PrimaryKeys = []string{"id", "val"}
	pb := predicate.NewBuilder(s.Fields)
	suffix, _ := pb.Equal("val", int64(50))
	prefix, _ := pb.Equal("id", int32(2))
	boundary, _ := pb.Equal("id", int32(1))
	for _, tc := range []struct {
		name     string
		min, max *binaryrow.BinaryRow
		p        *predicate.Predicate
		want     int
	}{
		{"suffix_unbounded", statsRow(1, 100), statsRow(3, 0), suffix, 1},
		{"interior_tuple", statsRow(1, 100), statsRow(3, 0), predicate.And(prefix, suffix), 1},
		{"point_below_minimum_tuple", statsRow(1, 100), statsRow(3, 0), predicate.And(boundary, suffix), 0},
		{"fixed_prefix_suffix_outside", statsRow(2, 100), statsRow(2, 200), suffix, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entries := []manifest.ManifestEntry{{File: manifest.DataFileMeta{MinKey: tc.min, MaxKey: tc.max}}}
			if got := pruneDataFiles(entries, tc.p, s); len(got) != tc.want {
				t.Fatalf("want %d files, got %d", tc.want, len(got))
			}
		})
	}
}

func TestPruneDataFiles_SparseValueStatsAndHistoricalSchemas(t *testing.T) {
	s := intSchema()
	p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(5))
	entries := []manifest.ManifestEntry{
		{File: manifest.DataFileMeta{FileName: "reordered-match", ValueStatsCols: []string{"val", "id"}, ValueStats: rowStats(100, 5)}},
		{File: manifest.DataFileMeta{FileName: "reordered-outside", ValueStatsCols: []string{"val", "id"}, ValueStats: rowStats(5, 100)}},
		{File: manifest.DataFileMeta{FileName: "untracked", ValueStatsCols: []string{"val"}, ValueStats: rowStats(100)}},
		{File: manifest.DataFileMeta{FileName: "old-schema", SchemaID: 1, ValueStats: rowStats(100, 100)}},
	}
	got := pruneDataFiles(entries, p, s)
	if len(got) != 3 || got[0].File.FileName != "reordered-match" || got[1].File.FileName != "untracked" || got[2].File.FileName != "old-schema" {
		t.Fatalf("incorrect sparse/historical stats pruning: %+v", got)
	}
}

func TestPKPruning_DoesNotResurrectOlderVersions(t *testing.T) {
	for _, kind := range []int8{0, 3} {
		t.Run(map[int8]string{0: "update", 3: "delete"}[kind], func(t *testing.T) {
			old := buildPKArrowTable(t, []int32{1}, []int64{1}, []int8{0}, []int64{10})
			defer old.Release()
			newer := buildPKArrowTable(t, []int32{1}, []int64{2}, []int8{kind}, []int64{20})
			defer newer.Release()
			fio := newMockFileIO()
			fio.register("data/old.parquet", writeParquet(t, old))
			fio.register("data/new.parquet", writeParquet(t, newer))
			s := pkSchema()
			p, _ := predicate.NewBuilder(s.Fields).Equal("val", int64(10))
			mr := &stubManifestReader{entriesResult: []manifest.ManifestEntry{
				{File: manifest.DataFileMeta{FileName: "old.parquet", Level: 1, RowCount: 1, KeyStats: intStats(1, 1), ValueStats: rowStats(1, 10)}},
				{File: manifest.DataFileMeta{FileName: "new.parquet", RowCount: 1, KeyStats: intStats(1, 1), ValueStats: rowStats(1, 20)}},
			}}
			rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, mr).WithFilter(p)
			plan, err := rb.NewScan().Plan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Splits) != 1 || len(plan.Splits[0].Files) != 2 {
				t.Fatalf("must keep both versions for merge, got %+v", plan.Splits)
			}
			result, err := rb.NewRead().ToArrow(context.Background(), plan.Splits)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()
			if result.NumRows() != 0 {
				t.Fatalf("obsolete row leaked through: %d rows", result.NumRows())
			}
		})
	}
}

func TestStreamPruning_UsesPartitionSchemaAndExactPartitions(t *testing.T) {
	s := partitionedSchema()
	p, _ := predicate.NewBuilder(s.Fields).Equal("dt", int32(5))
	mr := &streamManifestReader{
		listsByFile: map[string][]manifest.ManifestFileMeta{"delta": {
			{FileName: "outside", NumAddedFiles: 1, PartitionStats: intStats(100, 100)},
			{FileName: "mixed", NumAddedFiles: 2, PartitionStats: intStats(5, 6)},
		}},
		entriesByMetaFile: map[string][]manifest.ManifestEntry{
			"outside": {{Partition: buildIntRow(100), File: manifest.DataFileMeta{FileName: "outside"}}},
			"mixed": {
				{Partition: buildIntRow(5), File: manifest.DataFileMeta{FileName: "match"}},
				{Partition: buildIntRow(6), File: manifest.DataFileMeta{FileName: "other"}},
			},
		},
	}
	sb := newStreamReadBuilderFromIface(&stubTable{sch: s}, mr).WithFilter(p)
	batch, err := sb.NewStream().readDelta(context.Background(), appendSnap(1, "delta"))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Splits) != 1 || len(batch.Splits[0].Files) != 1 || batch.Splits[0].Files[0].FileName != "match" {
		t.Fatalf("want only matching partition, got %+v", batch.Splits)
	}
}
