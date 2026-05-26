// Package fileio provides the FileIO abstraction for reading Paimon table files.
package fileio

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// FileStatus describes a file or directory entry.
type FileStatus struct {
	Path    string
	Size    int64
	ModTime time.Time
	IsDir   bool
}

// FileIO is the storage abstraction used by all Paimon readers.
type FileIO interface {
	// Open returns a ReadCloser for the given path.
	Open(ctx context.Context, path string) (io.ReadCloser, error)

	// ReadAll reads the full contents of path into memory.
	ReadAll(ctx context.Context, path string) ([]byte, error)

	// List returns the immediate children of dir.
	List(ctx context.Context, dir string) ([]FileStatus, error)

	// Exists returns true if path exists.
	Exists(ctx context.Context, path string) (bool, error)

	// Close releases any resources held by the FileIO.
	Close() error
}

// New creates a FileIO appropriate for the given path.
// Paths starting with "gs://" use GCS; everything else uses local filesystem.
func New(ctx context.Context, path string, opts ...Option) (FileIO, error) {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	if strings.HasPrefix(path, "gs://") {
		return newGCSFileIO(ctx, cfg)
	}
	return &localFileIO{}, nil
}

// Option configures a FileIO instance.
type Option func(*config)

type config struct {
	credentialsFile string
	credentialsJSON []byte
}

// WithCredentialsFile sets a service-account key file for GCS auth.
func WithCredentialsFile(path string) Option {
	return func(c *config) { c.credentialsFile = path }
}

// WithCredentialsJSON sets raw service-account JSON for GCS auth.
func WithCredentialsJSON(json []byte) Option {
	return func(c *config) { c.credentialsJSON = json }
}

// --- Local filesystem implementation ---

type localFileIO struct{}

func (l *localFileIO) Open(_ context.Context, path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (l *localFileIO) ReadAll(_ context.Context, path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (l *localFileIO) List(_ context.Context, dir string) ([]FileStatus, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := make([]FileStatus, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		result = append(result, FileStatus{
			Path:    filepath.Join(dir, e.Name()),
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   e.IsDir(),
		})
	}
	return result, nil
}

func (l *localFileIO) Exists(_ context.Context, path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (l *localFileIO) Close() error { return nil }

// --- GCS implementation ---

type gcsFileIO struct {
	client *storage.Client
}

func newGCSFileIO(ctx context.Context, cfg *config) (*gcsFileIO, error) {
	var clientOpts []option.ClientOption
	if cfg.credentialsFile != "" {
		clientOpts = append(clientOpts, option.WithCredentialsFile(cfg.credentialsFile))
	} else if len(cfg.credentialsJSON) > 0 {
		clientOpts = append(clientOpts, option.WithCredentialsJSON(cfg.credentialsJSON))
	}
	client, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("fileio: create GCS client: %w", err)
	}
	return &gcsFileIO{client: client}, nil
}

// parsGCSPath splits "gs://bucket/path/to/object" into (bucket, object).
func parseGCSPath(path string) (bucket, object string, err error) {
	s := strings.TrimPrefix(path, "gs://")
	idx := strings.IndexByte(s, '/')
	if idx < 0 {
		return s, "", nil
	}
	return s[:idx], s[idx+1:], nil
}

func (g *gcsFileIO) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	bucket, obj, err := parseGCSPath(path)
	if err != nil {
		return nil, err
	}
	return g.client.Bucket(bucket).Object(obj).NewReader(ctx)
}

func (g *gcsFileIO) ReadAll(ctx context.Context, path string) ([]byte, error) {
	rc, err := g.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (g *gcsFileIO) List(ctx context.Context, dir string) ([]FileStatus, error) {
	bucket, prefix, err := parseGCSPath(dir)
	if err != nil {
		return nil, err
	}
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	query := &storage.Query{Prefix: prefix, Delimiter: "/"}
	it := g.client.Bucket(bucket).Objects(ctx, query)

	var result []FileStatus
	seen := map[string]bool{}
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fileio: list GCS %s: %w", dir, err)
		}
		// attrs.Prefix is non-empty for "directories" (common prefixes)
		if attrs.Prefix != "" {
			p := "gs://" + bucket + "/" + attrs.Prefix
			if !seen[p] {
				seen[p] = true
				result = append(result, FileStatus{Path: p, IsDir: true})
			}
		} else {
			p := "gs://" + bucket + "/" + attrs.Name
			result = append(result, FileStatus{
				Path:    p,
				Size:    attrs.Size,
				ModTime: attrs.Updated,
				IsDir:   false,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result, nil
}

func (g *gcsFileIO) Exists(ctx context.Context, path string) (bool, error) {
	bucket, obj, err := parseGCSPath(path)
	if err != nil {
		return false, err
	}
	_, err = g.client.Bucket(bucket).Object(obj).Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (g *gcsFileIO) Close() error {
	return g.client.Close()
}
