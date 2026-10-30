package read

import (
	"context"
	"math/rand"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

func fixedBucketSchema() *schema.TableSchema {
	s := pkSchema()
	s.Options = map[string]string{"bucket": "64"}
	return s
}

func TestBucketPruning_NeverRejectsMatchingRows(t *testing.T) {
	s := fixedBucketSchema()
	pb := predicate.NewBuilder(s.Fields)
	random := rand.New(rand.NewSource(42))
	var expression func(int) *predicate.Predicate
	expression = func(depth int) *predicate.Predicate {
		if depth > 0 {
			switch random.Intn(5) {
			case 0:
				return predicate.And(expression(depth-1), expression(depth-1))
			case 1:
				return predicate.Or(expression(depth-1), expression(depth-1))
			case 2:
				return predicate.Not(expression(depth - 1))
			}
		}
		var p *predicate.Predicate
		switch random.Intn(4) {
		case 0:
			p, _ = pb.Equal("id", int32(random.Intn(8)))
		case 1:
			p, _ = pb.In("id", int32(random.Intn(8)), int32(random.Intn(8)))
		case 2:
			p, _ = pb.Equal("val", int64(random.Intn(8)))
		case 3:
			p, _ = pb.GreaterThan("id", int32(random.Intn(8)))
		}
		return p
	}
	ids, vals := make([]int32, 64), make([]int64, 64)
	for i := range ids {
		ids[i], vals[i] = int32(i/8), int64(i%8)
	}
	data := buildArrowTable(t, ids, vals)
	defer data.Release()
	reader := array.NewTableReader(data, 64)
	defer reader.Release()
	if !reader.Next() {
		t.Fatal("missing test rows")
	}
	rec := reader.RecordBatch()
	for i := 0; i < 300; i++ {
		filter := expression(3)
		pruner := newBucketPruner(s, filter)
		for row := 0; row < 64; row++ {
			if !predicate.EvalRow(filter, rec, row) {
				continue
			}
			// Independently verified Java hashes are covered by binaryrow tests;
			// this checks that logical predicate extraction never strengthens it.
			key, _ := pb.Equal("id", ids[row])
			actual := newBucketPruner(s, key)
			for bucket := 0; bucket < 64; bucket++ {
				if actual.mayContain(bucket, 64) && !pruner.mayContain(bucket, 64) {
					t.Fatalf("expression %d rejected matching row id=%d val=%d bucket=%d", i, ids[row], vals[row], bucket)
				}
			}
		}
	}
}

func TestBucketPruning_FinitePredicates(t *testing.T) {
	s := fixedBucketSchema()
	pb := predicate.NewBuilder(s.Fields)
	zero, _ := pb.Equal("id", int32(0))
	one, _ := pb.Equal("id", int32(1))
	value, _ := pb.Equal("val", int64(10))
	in, _ := pb.In("id", int32(0), int32(1))
	for _, tc := range []struct {
		name string
		p    *predicate.Predicate
		want []int
	}{
		{"equal", zero, []int{27}},
		{"in", in, []int{27, 30}},
		{"or", predicate.Or(zero, one), []int{27, 30}},
		{"and_value", predicate.And(zero, value), []int{27}},
		{"or_value", predicate.Or(zero, value), nil},
		{"not", predicate.Not(zero), nil},
		{"value_only", value, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newBucketPruner(s, tc.p)
			if tc.want == nil {
				if p != nil {
					t.Fatal("ambiguous predicate enabled bucket pruning")
				}
				return
			}
			var got []int
			for bucket := 0; bucket < 64; bucket++ {
				if p.mayContain(bucket, 64) {
					got = append(got, bucket)
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("want buckets %v, got %v", tc.want, got)
			}
		})
	}
}

func TestBucketPruning_UsesEntryCountsAndKeepsUnknownLayouts(t *testing.T) {
	s := fixedBucketSchema()
	filter, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(0))
	entries := []manifest.ManifestEntry{
		{Bucket: 27, TotalBuckets: 64, File: manifest.DataFileMeta{FileName: "current"}},
		{Bucket: 11, TotalBuckets: 16, File: manifest.DataFileMeta{FileName: "historical-count"}},
		{Bucket: 3, TotalBuckets: 8, File: manifest.DataFileMeta{FileName: "older-count"}},
		{Bucket: 1, TotalBuckets: 64, File: manifest.DataFileMeta{FileName: "outside"}},
		{Bucket: 1, TotalBuckets: 16, File: manifest.DataFileMeta{FileName: "outside-old"}},
		{Bucket: 1, File: manifest.DataFileMeta{FileName: "unknown-count"}},
		{Bucket: -1, TotalBuckets: 64, File: manifest.DataFileMeta{FileName: "pending"}},
		{Bucket: 1, TotalBuckets: 64, File: manifest.DataFileMeta{FileName: "old-schema", SchemaID: 1}},
	}
	got := pruneDataFiles(entries, filter, s)
	var names []string
	for _, e := range got {
		names = append(names, e.File.FileName)
	}
	want := []string{"current", "historical-count", "older-count", "unknown-count", "pending", "old-schema"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("want %v, got %v", want, names)
	}
	for _, options := range []map[string]string{
		{"bucket": "-1"},
		{"bucket": "-2"},
		{"bucket": "64", "bucket-function.type": "mod"},
		{"bucket": "64", "bucket-function.provider": "custom"},
		{"bucket": "64", "bucket-key": "val"}, // mutable key in a PK table
		{"bucket": "64", "bucket-key": "missing"},
	} {
		s.Options = options
		if p := newBucketPruner(s, filter); p != nil {
			t.Errorf("unsafe layout enabled pruning: %v", options)
		}
	}
}

func TestBucketPruning_CompositeAndExplicitKeys(t *testing.T) {
	s := fixedBucketSchema()
	s.Fields = []schema.DataField{
		{Name: "profile", Type: schema.DataType{Type: "STRING"}},
		{Name: "asset", Type: schema.DataType{Type: "STRING"}},
		{Name: "id", Type: schema.DataType{Type: "INT"}},
		{Name: "val", Type: schema.DataType{Type: "BIGINT"}},
		{Name: "active", Type: schema.DataType{Type: "BOOLEAN"}},
	}
	s.PrimaryKeys = []string{"profile", "asset", "id", "val", "active"}
	pb := predicate.NewBuilder(s.Fields)
	a, _ := pb.Equal("profile", "profile-123")
	b, _ := pb.Equal("asset", "asset-987")
	c, _ := pb.Equal("id", int32(42))
	d, _ := pb.Equal("val", int64(-7))
	e, _ := pb.Equal("active", true)
	p := newBucketPruner(s, predicate.And(a, b, c, d, e))
	if p == nil || !reflect.DeepEqual(p.hashes, []int32{419515100}) {
		t.Fatalf("composite hash differs from Java writer: %+v", p)
	}
	if p := newBucketPruner(s, predicate.And(a, b)); p != nil {
		t.Fatal("incomplete bucket key enabled pruning")
	}
	s.Options["bucket-key"] = "id"
	p = newBucketPruner(s, c)
	if p == nil || len(p.hashes) != 1 {
		t.Fatal("explicit bucket-key ignored")
	}
	// An append-only table can also opt into a fixed explicit bucket-key.
	s.PrimaryKeys = nil
	if p := newBucketPruner(s, c); p == nil {
		t.Fatal("append-only explicit bucket-key ignored")
	}
}

func TestBucketPruning_PartitionFieldsAreNotHashed(t *testing.T) {
	s := partitionedSchema()
	s.PrimaryKeys = []string{"dt", "id"}
	s.Options = map[string]string{"bucket": "64"}
	p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(0))
	pruner := newBucketPruner(s, p)
	if pruner == nil || !pruner.mayContain(27, 64) {
		t.Fatal("partition field shifted the bucket key")
	}
}

func TestBucketPruning_TooManyCombinationsFallBack(t *testing.T) {
	s := fixedBucketSchema()
	values := make([]interface{}, maxBucketKeyCombinations+1)
	for i := range values {
		values[i] = int32(i)
	}
	p, _ := predicate.NewBuilder(s.Fields).In("id", values...)
	if p := newBucketPruner(s, p); p != nil {
		t.Fatal("unbounded bucket-key expansion")
	}
}

func TestBucketPruning_UnselectedBucketsNeverOpen(t *testing.T) {
	s := fixedBucketSchema()
	fio := newTrackedFileIO()
	data := buildPKArrowTable(t, []int32{1}, []int64{1}, []int8{0}, []int64{10})
	defer data.Release()
	fio.register("data/match.parquet", writeParquet(t, data))
	p, _ := predicate.NewBuilder(s.Fields).Equal("id", int32(1))
	mr := &stubManifestReader{entriesResult: []manifest.ManifestEntry{
		{Bucket: 30, TotalBuckets: 64, File: manifest.DataFileMeta{FileName: "match.parquet"}},
		{Bucket: 0, TotalBuckets: 64, File: manifest.DataFileMeta{FileName: "outside.parquet"}},
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
	if result.NumRows() != 1 || fio.opens["data/outside.parquet"] != 0 {
		t.Fatalf("incorrect bucket read: rows=%d opens=%v", result.NumRows(), fio.opens)
	}
}
