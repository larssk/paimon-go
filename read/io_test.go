package read

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/larssk/paimon-go/fileio"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
)

type trackedFileIO struct {
	*mockFileIO
	opens, closes   map[string]int
	bytesRead       int
	sequentialReads int
	ranges          []readRange
}

type readRange struct {
	path   string
	offset int64
	length int
}

func newTrackedFileIO() *trackedFileIO {
	return &trackedFileIO{mockFileIO: newMockFileIO(), opens: make(map[string]int), closes: make(map[string]int)}
}

func (f *trackedFileIO) OpenRandomAccess(_ context.Context, path string) (fileio.RandomAccessFile, error) {
	f.opens[path]++
	if _, ok := f.files[path]; !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return &trackedFile{Reader: bytes.NewReader(f.files[path]), owner: f, path: path}, nil
}

type trackedFile struct {
	*bytes.Reader
	owner *trackedFileIO
	path  string
}

func (f *trackedFile) Read(p []byte) (int, error) {
	f.owner.sequentialReads++
	n, err := f.Reader.Read(p)
	f.owner.bytesRead += n
	return n, err
}

func (f *trackedFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.Reader.ReadAt(p, off)
	f.owner.bytesRead += n
	f.owner.ranges = append(f.owner.ranges, readRange{path: f.path, offset: off, length: n})
	return n, err
}

func (f *trackedFile) Close() error {
	f.owner.closes[f.path]++
	return nil
}

func TestReaders_RandomAccessDoesNotBufferWholeFile(t *testing.T) {
	const n = 150000 // more than two row groups
	ids, vals := make([]int32, n), make([]int64, n)
	seqs, kinds := make([]int64, n), make([]int8, n)
	for i := range ids {
		ids[i], vals[i], seqs[i] = int32(i), int64(i), 1
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
			fio := newTrackedFileIO()
			content := writeParquet(t, data)
			fio.register("data/large.parquet", content)
			rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, nil).WithProjection([]string{"id"})
			split := makeFileSplit("large.parquet")
			split.NeedsMerge = pk
			split.Files[0].KeyStats = intStats(0, n-1)
			rr, err := rb.NewRead().ToArrowReader(context.Background(), []DataSplit{split})
			if err != nil {
				t.Fatal(err)
			}
			if !rr.Next() {
				t.Fatalf("first batch: %v", rr.Err())
			}
			if fio.sequentialReads != 0 || fio.bytesRead >= len(content) {
				t.Errorf("whole file buffered: %d sequential reads, %d/%d bytes", fio.sequentialReads, fio.bytesRead, len(content))
			}
			t.Logf("first result read %d/%d bytes", fio.bytesRead, len(content))
			rr.Release()
			if fio.closes["data/large.parquet"] != 1 {
				t.Fatalf("file not closed exactly once: %+v", fio.closes)
			}
		})
	}
}

func TestPKRunReader_LazyOpenAndClose(t *testing.T) {
	fio := newTrackedFileIO()
	for _, name := range []string{"first", "second"} {
		data := buildPKArrowTable(t, []int32{1, 2}, []int64{1, 1}, []int8{0, 0}, []int64{10, 20})
		fio.register("data/"+name+".parquet", writeParquet(t, data))
		data.Release()
	}
	r := &sortMergeReader{ctx: context.Background(), tbl: &readTable{sch: pkSchema(), io: fio}, readFields: pkSchema().Fields, keyFields: pkSchema().PrimaryKeyFields()}
	// openKVReader needs an allocator, supplied by the normal outer reader.
	r.alloc = memory.NewGoAllocator()
	run, err := r.newRunReader([]manifest.DataFileMeta{{FileName: "first.parquet"}, {FileName: "second.parquet"}})
	if err != nil {
		t.Fatal(err)
	}
	defer run.close()
	if len(fio.opens) != 0 {
		t.Fatal("run construction opened files")
	}
	run.next()
	if fio.opens["data/first.parquet"] != 1 || fio.opens["data/second.parquet"] != 0 {
		t.Fatalf("expected only first file open, got %+v", fio.opens)
	}
	run.next()
	run.next()
	if fio.closes["data/first.parquet"] != 1 || fio.opens["data/second.parquet"] != 1 {
		t.Fatalf("advance must close first before opening second: opens=%v closes=%v", fio.opens, fio.closes)
	}
	run.close()
	if fio.closes["data/second.parquet"] != 1 {
		t.Fatalf("early close leaked second file: %+v", fio.closes)
	}
}

func TestReaders_FilterColumnNotProjected(t *testing.T) {
	for _, pk := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "primary_key"}[pk], func(t *testing.T) {
			s := paimonSchema()
			data := buildArrowTable(t, []int32{1, 2, 3}, []int64{10, 20, 30})
			if pk {
				data.Release()
				data = buildPKArrowTable(t, []int32{1, 2, 3}, []int64{1, 1, 1}, []int8{0, 0, 0}, []int64{10, 20, 30})
				s = pkSchema()
			}
			defer data.Release()
			fio := newTrackedFileIO()
			fio.register("data/rows.parquet", writeParquet(t, data))
			p, _ := predicate.NewBuilder(s.Fields).Equal("val", int64(20))
			rb := newReadBuilderFromIface(&readTable{sch: s, io: fio}, nil).WithFilter(p).WithProjection([]string{"id"})
			split := makeFileSplit("rows.parquet")
			split.NeedsMerge = pk
			split.Files[0].KeyStats = intStats(1, 3)
			result, err := rb.NewRead().ToArrow(context.Background(), []DataSplit{split})
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()
			if result.NumCols() != 1 || result.NumRows() != 1 || result.Column(0).Data().Chunks()[0].(*array.Int32).Value(0) != 2 {
				t.Fatalf("filter/projection produced incorrect result: %v", result)
			}
		})
	}
}
