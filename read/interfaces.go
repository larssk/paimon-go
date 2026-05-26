package read

import (
	"context"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/binaryrow"
	"github.com/apache/paimon/paimon-go/manifest"
	"github.com/apache/paimon/paimon-go/schema"
	"github.com/apache/paimon/paimon-go/snapshot"
)

// tableReader is the internal interface that ReadBuilder and TableScan depend on.
// *table.FileStoreTable satisfies this interface.
// It is unexported because callers never need to implement it — they always pass
// a *table.FileStoreTable to NewReadBuilder or NewStreamReadBuilder.
type tableReader interface {
	LatestSnapshot(ctx context.Context) (*snapshot.Snapshot, error)
	SnapshotByID(ctx context.Context, id int64) (*snapshot.Snapshot, error)
	ListSnapshotIDs(ctx context.Context) ([]int64, error)
	GetSchema() *schema.TableSchema
	ManifestDir() string
	GetIO() fileio.FileIO
	DataFilePath(partition *binaryrow.BinaryRow, partFields []schema.DataField, bucket int, fileName string) string
}

// manifestReader reads manifest-list and manifest-entry Avro files.
// It is unexported because the real implementation is wired inside NewReadBuilder
// and callers never need to provide their own.
type manifestReader interface {
	ReadList(ctx context.Context, filename string, partFields []schema.DataField) ([]manifest.ManifestFileMeta, error)
	ReadAllEntries(ctx context.Context, metas []manifest.ManifestFileMeta, partFields []schema.DataField, valueFields []schema.DataField) ([]manifest.ManifestEntry, error)
}

// defaultManifestReader is the real implementation that delegates to the manifest package.
type defaultManifestReader struct {
	dir string
	io  fileio.FileIO
}

func newDefaultManifestReader(dir string, io fileio.FileIO) manifestReader {
	return &defaultManifestReader{dir: dir, io: io}
}

func (r *defaultManifestReader) ReadList(ctx context.Context, filename string, partFields []schema.DataField) ([]manifest.ManifestFileMeta, error) {
	return manifest.ReadManifestList(ctx, r.io, r.dir, filename, partFields)
}

func (r *defaultManifestReader) ReadAllEntries(ctx context.Context, metas []manifest.ManifestFileMeta, partFields []schema.DataField, valueFields []schema.DataField) ([]manifest.ManifestEntry, error) {
	return manifest.ReadAllEntries(ctx, r.io, r.dir, metas, partFields, valueFields)
}
