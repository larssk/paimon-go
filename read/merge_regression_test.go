package read

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/schema"
)

func TestPKMerge_DeleteDoesNotEndSection(t *testing.T) {
	old := buildPKArrowTable(t, []int32{1, 2}, []int64{1, 1}, []int8{0, 0}, []int64{10, 20})
	defer old.Release()
	newer := buildPKArrowTable(t, []int32{1, 2}, []int64{2, 2}, []int8{3, 0}, []int64{0, 21})
	defer newer.Release()
	fio := newTrackedFileIO()
	fio.register("data/old.parquet", writeParquet(t, old))
	fio.register("data/new.parquet", writeParquet(t, newer))
	rb := newReadBuilderFromIface(&readTable{sch: pkSchema(), io: fio}, nil)
	split := makePKSplit(
		manifest.DataFileMeta{FileName: "old.parquet", Level: 1, KeyStats: intStats(1, 2)},
		manifest.DataFileMeta{FileName: "new.parquet", KeyStats: intStats(1, 2)},
	)
	rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
	if err != nil {
		t.Fatal(err)
	}
	defer rr.Release()
	vals := collectInt64Col(t, rr, "val")
	if err := rr.Err(); err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0] != 21 {
		t.Fatalf("delete must not discard following keys: got %v", vals)
	}
	for _, name := range []string{"old", "new"} {
		if fio.closes["data/"+name+".parquet"] != 1 {
			t.Errorf("file %s not closed exactly once: %v", name, fio.closes)
		}
	}
}

func TestIntervalPartition_MissingBoundsCannotSeparateVersions(t *testing.T) {
	s := pkSchema()
	sections := intervalPartition([]manifest.DataFileMeta{
		{FileName: "unknown.parquet"},
		{FileName: "known.parquet", KeyStats: intStats(1, 10)},
	}, s.PrimaryKeyFields())
	if len(sections) != 1 || len(sections[0]) != 2 {
		t.Fatalf("unknown bounds must overlap all files in separate runs: %+v", sections)
	}
}

func TestPKMerge_EarlyReleaseFreesHeapAndReaders(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	fio := newTrackedFileIO()
	for _, name := range []string{"a", "b"} {
		data := buildPKArrowTable(t, []int32{1, 2, 3}, []int64{1, 1, 1}, []int8{0, 0, 0}, []int64{10, 20, 30})
		fio.register("data/"+name+".parquet", writeParquet(t, data))
		data.Release()
	}
	s := pkSchema()
	arrowSchema, err := schema.ToArrowSchema(s.Fields)
	if err != nil {
		t.Fatal(err)
	}
	r, err := newSortMergeReader(context.Background(), &readTable{sch: s, io: fio}, makePKSplit(
		manifest.DataFileMeta{FileName: "a.parquet", KeyStats: intStats(1, 3)},
		manifest.DataFileMeta{FileName: "b.parquet", KeyStats: intStats(1, 3)},
	), s.PrimaryKeyFields(), s.Fields, arrowSchema, alloc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Next() {
		t.Fatalf("first row: %v", r.Err())
	}
	r.Release()
	r.Release() // cleanup is idempotent
	if fio.closes["data/a.parquet"] != 1 || fio.closes["data/b.parquet"] != 1 {
		t.Fatalf("early release leaked readers: %v", fio.closes)
	}
}

func TestPKMerge_LazyOpenFailureIsReportedAndClosesReaders(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	fio := newTrackedFileIO()
	data := buildPKArrowTable(t, []int32{1}, []int64{1}, []int8{0}, []int64{10})
	fio.register("data/first.parquet", writeParquet(t, data))
	data.Release()
	r := &sortMergeReader{ctx: context.Background(), tbl: &readTable{sch: pkSchema(), io: fio}, readFields: pkSchema().Fields, keyFields: pkSchema().PrimaryKeyFields(), alloc: alloc}
	m, err := r.openSection([][]manifest.DataFileMeta{{{FileName: "first.parquet"}, {FileName: "missing.parquet"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer m.close()
	if _, err := m.next(); err == nil {
		t.Fatal("late file-open error was swallowed")
	}
	if fio.closes["data/first.parquet"] != 1 {
		t.Fatalf("file leaked on late-open failure: %v", fio.closes)
	}
}
