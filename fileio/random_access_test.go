package fileio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func TestOpenRandomAccess_LocalFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, []byte("0123456789"), 0600); err != nil {
		t.Fatal(err)
	}
	fio, err := New(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer fio.Close()
	r, err := OpenRandomAccess(context.Background(), fio, path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, ok := r.(*os.File); !ok {
		t.Fatalf("local file was buffered instead of reused: %T", r)
	}
	p := make([]byte, 3)
	if n, err := r.ReadAt(p, 4); err != nil || n != 3 || string(p) != "456" {
		t.Fatalf("ReadAt: n=%d data=%q err=%v", n, p, err)
	}
}

type legacyFileIO struct {
	FileIO
	reader io.ReadCloser
}

func (l *legacyFileIO) Open(context.Context, string) (io.ReadCloser, error) { return l.reader, nil }

type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error { c.closed = true; return nil }

func TestOpenRandomAccess_LegacyFallback(t *testing.T) {
	rc := &closeTracker{Reader: bytes.NewBufferString("0123456789")}
	r, err := OpenRandomAccess(context.Background(), &legacyFileIO{reader: rc}, "data")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !rc.closed {
		t.Fatal("buffered input was not closed")
	}
	if _, err := r.Seek(-3, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(r)
	if err != nil || string(p) != "789" {
		t.Fatalf("fallback seek/read: data=%q err=%v", p, err)
	}
}

func TestOpenRandomAccess_LargeLegacyFileSpoolsToDisk(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	data := bytes.Repeat([]byte("0123456789"), maxBufferedFileSize)
	rc := &closeTracker{Reader: bytes.NewReader(data)}
	r, err := OpenRandomAccess(context.Background(), &legacyFileIO{reader: rc}, "data")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	f, ok := r.(*spooledFile)
	if !ok {
		t.Fatalf("large non-seekable file was buffered in memory: %T", r)
	}
	if !rc.closed {
		t.Fatal("source stream was not closed")
	}
	name := f.Name()
	buf := make([]byte, 10)
	if n, err := r.ReadAt(buf, maxBufferedFileSize+3); err != nil || n != 10 || !bytes.Equal(buf, data[maxBufferedFileSize+3:maxBufferedFileSize+13]) {
		t.Fatalf("spooled ReadAt: n=%d data=%q err=%v", n, buf, err)
	}
	if _, err := r.Seek(-10, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if n, err := io.ReadFull(r, buf); err != nil || n != 10 || !bytes.Equal(buf, data[len(data)-10:]) {
		t.Fatalf("spooled seek/read: n=%d data=%q err=%v", n, buf, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("temporary file leaked: %v", err)
	}
}

type failingStream struct {
	remaining int
	err       error
}

func (r *failingStream) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, r.err
	}
	n := min(len(p), r.remaining)
	clear(p[:n])
	r.remaining -= n
	return n, nil
}

func TestOpenRandomAccess_SpoolFailureCleansUp(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	sentinel := errors.New("stream failed")
	rc := &closeTracker{Reader: &failingStream{remaining: maxBufferedFileSize + 10000, err: sentinel}}
	if _, err := OpenRandomAccess(context.Background(), &legacyFileIO{reader: rc}, "data"); !errors.Is(err, sentinel) {
		t.Fatalf("stream error lost: %v", err)
	}
	if !rc.closed {
		t.Fatal("failed source stream leaked")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files leaked: %v (err=%v)", files, err)
	}
}

func TestOpenRandomAccess_CanceledLegacyReadClosesSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rc := &closeTracker{Reader: bytes.NewReader([]byte("data"))}
	if _, err := OpenRandomAccess(ctx, &legacyFileIO{reader: rc}, "data"); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation lost: %v", err)
	}
	if !rc.closed {
		t.Fatal("canceled source stream leaked")
	}
}

type cancelOnRead struct{ cancel context.CancelFunc }

func (r cancelOnRead) Read(p []byte) (int, error) {
	r.cancel()
	p[0] = 0
	return 1, nil
}

func TestOpenRandomAccess_CanceledSpoolCleansUp(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc := &closeTracker{Reader: io.MultiReader(
		bytes.NewReader(make([]byte, maxBufferedFileSize+1)),
		cancelOnRead{cancel: cancel},
	)}
	if _, err := OpenRandomAccess(ctx, &legacyFileIO{reader: rc}, "data"); !errors.Is(err, context.Canceled) {
		t.Fatalf("spool cancellation lost: %v", err)
	}
	if !rc.closed {
		t.Fatal("canceled spool source leaked")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("canceled spool leaked files: %v (err=%v)", files, err)
	}
}

func TestRangeReaderAt_BoundsAndConcurrentReads(t *testing.T) {
	data := []byte("0123456789")
	var mu sync.Mutex
	var readers []*closeTracker
	r := &rangeReaderAt{size: int64(len(data)), open: func(off, n int64) (io.ReadCloser, error) {
		mu.Lock()
		defer mu.Unlock()
		if off < 0 || off+n > int64(len(data)) {
			return nil, fmt.Errorf("invalid range [%d,%d)", off, off+n)
		}
		rc := &closeTracker{Reader: bytes.NewReader(data[off : off+n])}
		readers = append(readers, rc)
		return rc, nil
	}}
	var wg sync.WaitGroup
	for i := 0; i < len(data); i++ {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			p := make([]byte, 1)
			if n, err := r.ReadAt(p, int64(off)); n != 1 || err != nil || p[0] != data[off] {
				t.Errorf("offset %d: n=%d data=%q err=%v", off, n, p, err)
			}
		}(i)
	}
	wg.Wait()
	p := make([]byte, 3)
	if n, err := r.ReadAt(p, 9); n != 1 || !errors.Is(err, io.EOF) || p[0] != '9' {
		t.Fatalf("short read: n=%d data=%q err=%v", n, p, err)
	}
	if n, err := r.ReadAt(p, 10); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("EOF: n=%d err=%v", n, err)
	}
	if _, err := r.ReadAt(p, -1); err == nil {
		t.Fatal("negative offset accepted")
	}
	for _, rc := range readers {
		if !rc.closed {
			t.Fatal("range response was not closed")
		}
	}
}

func TestGCSRandomAccess_UsesPinnedRangeRequests(t *testing.T) {
	data := []byte("0123456789")
	var ranges int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("alt") != "media" {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"bucket":"bucket","name":"object","size":"10","generation":"7"}`)
			return
		}
		ranges++
		if got := r.URL.Query().Get("generation"); got != "7" {
			t.Errorf("unpinned object generation: %q", got)
		}
		var lo, hi int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &lo, &hi); err != nil || lo < 0 || hi >= len(data) {
			t.Errorf("invalid range request: %q", r.Header.Get("Range"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(hi-lo+1))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[lo : hi+1])
	}))
	defer server.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL), option.WithoutAuthentication(), storage.WithJSONReads())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	fio := &gcsFileIO{client: client}
	r, err := OpenRandomAccess(context.Background(), fio, "gs://bucket/object")
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 3)
	if n, err := r.ReadAt(p, 3); n != 3 || err != nil || string(p) != "345" {
		t.Fatalf("range read: n=%d data=%q err=%v", n, p, err)
	}
	if ranges != 1 {
		t.Fatalf("want one range request, got %d", ranges)
	}
	r.Close()
	if _, err := r.ReadAt(p, 0); err == nil {
		t.Fatal("closed remote reader must not issue further reads")
	}
}
