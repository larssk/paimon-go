package pathutil_test

import (
	"testing"

	"github.com/apache/paimon/paimon-go/internal/pathutil"
)

func TestJoin_Local(t *testing.T) {
	got := pathutil.Join("/data/warehouse", "mydb.db", "mytable")
	want := "/data/warehouse/mydb.db/mytable"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestJoin_GCS_PreservesDoubleSlash(t *testing.T) {
	got := pathutil.Join("gs://my-bucket/warehouse", "mydb.db", "mytable")
	want := "gs://my-bucket/warehouse/mydb.db/mytable"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestJoin_GCS_TrailingSlash(t *testing.T) {
	got := pathutil.Join("gs://my-bucket/warehouse/", "snapshot", "1")
	want := "gs://my-bucket/warehouse/snapshot/1"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestBase_Local(t *testing.T) {
	got := pathutil.Base("/data/warehouse/snapshot/7")
	want := "7"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestBase_GCS(t *testing.T) {
	got := pathutil.Base("gs://my-bucket/warehouse/snapshot/7")
	want := "7"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
