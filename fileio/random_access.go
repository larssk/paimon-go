package fileio

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// RandomAccessFile is a seekable data-file reader. ReadAt may be called
// concurrently; Read and Seek share a cursor and must not be used concurrently.
type RandomAccessFile interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

// RandomAccessFileIO is an optional FileIO capability for remote range reads.
// Existing FileIO implementations remain compatible without implementing it.
type RandomAccessFileIO interface {
	OpenRandomAccess(context.Context, string) (RandomAccessFile, error)
}

// OpenRandomAccess prefers range reads or an already seekable Open result.
// Only legacy, non-seekable backends fall back to buffering the entire file.
func OpenRandomAccess(ctx context.Context, fio FileIO, path string) (RandomAccessFile, error) {
	if fio, ok := fio.(RandomAccessFileIO); ok {
		return fio.OpenRandomAccess(ctx, path)
	}
	rc, err := fio.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	if r, ok := rc.(RandomAccessFile); ok {
		return r, nil
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return &bufferedFile{Reader: bytes.NewReader(data)}, nil
}

type bufferedFile struct{ *bytes.Reader }

func (*bufferedFile) Close() error { return nil }

func (g *gcsFileIO) OpenRandomAccess(ctx context.Context, path string) (RandomAccessFile, error) {
	bucket, name, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}
	obj := g.client.Bucket(bucket).Object(name)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	// Pin the generation so independent range requests cannot mix versions.
	obj = obj.Generation(attrs.Generation)
	ctx, cancel := context.WithCancel(ctx)
	r := &rangeReaderAt{
		size: attrs.Size,
		open: func(offset, length int64) (io.ReadCloser, error) {
			return obj.NewRangeReader(ctx, offset, length)
		},
	}
	return &rangeFile{SectionReader: io.NewSectionReader(r, 0, attrs.Size), cancel: cancel}, nil
}

type rangeFile struct {
	*io.SectionReader
	cancel context.CancelFunc
}

func (f *rangeFile) Close() error {
	f.cancel()
	return nil
}

type rangeReaderAt struct {
	size int64
	open func(offset, length int64) (io.ReadCloser, error)
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("fileio: negative read offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	length := min(int64(len(p)), r.size-off)
	rc, err := r.open(off, length)
	if err != nil {
		return 0, err
	}
	n, err := io.ReadFull(rc, p[:int(length)])
	closeErr := rc.Close()
	if err != nil {
		return n, err
	}
	if closeErr != nil {
		return n, closeErr
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
