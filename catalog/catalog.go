// Package catalog provides Catalog implementations for discovering Paimon tables.
package catalog

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/pathutil"
	"github.com/apache/paimon/paimon-go/table"
)

// Catalog is the interface for accessing Paimon tables.
type Catalog interface {
	// GetTable returns a FileStoreTable for the given database and table name.
	GetTable(ctx context.Context, database, tableName string) (*table.FileStoreTable, error)

	// ListDatabases returns all database names in the catalog.
	ListDatabases(ctx context.Context) ([]string, error)

	// ListTables returns all table names in the given database.
	ListTables(ctx context.Context, database string) ([]string, error)

	// Close releases resources held by the catalog.
	Close() error
}

// Options holds configuration for creating a Catalog.
type Options struct {
	// Warehouse is the root path of the Paimon warehouse.
	// For GCS: "gs://bucket/path/to/warehouse"
	// For local: "/path/to/warehouse"
	Warehouse string

	// Metastore selects the catalog implementation. Currently only "filesystem" is supported.
	Metastore string

	// FileIOOptions are passed through to fileio.New.
	FileIOOptions []fileio.Option
}

// New creates a Catalog from the provided options.
//
// opts.Metastore selects the catalog implementation. The only supported value
// is "filesystem" (case-insensitive); it is also the default when the field is
// empty. Any other value returns an error.
//
// opts.Warehouse must be set. For GCS use a "gs://bucket/path" URI; for local
// storage use an absolute path.
func New(ctx context.Context, opts Options) (Catalog, error) {
	ms := strings.ToLower(opts.Metastore)
	if ms == "" {
		ms = "filesystem"
	}
	switch ms {
	case "filesystem":
		fio, err := fileio.New(ctx, opts.Warehouse, opts.FileIOOptions...)
		if err != nil {
			return nil, fmt.Errorf("catalog: create fileio: %w", err)
		}
		return &FileSystemCatalog{warehouse: opts.Warehouse, io: fio}, nil
	default:
		return nil, fmt.Errorf("catalog: unsupported metastore %q (only \"filesystem\" supported)", ms)
	}
}

// FileSystemCatalog is a path-based catalog that resolves tables from a warehouse directory.
// Database layout: <warehouse>/<db>.db/<table>
type FileSystemCatalog struct {
	warehouse string
	io        fileio.FileIO
}

func (c *FileSystemCatalog) dbPath(database string) string {
	return pathutil.Join(c.warehouse, database+".db")
}

func (c *FileSystemCatalog) tablePath(database, tableName string) string {
	return pathutil.Join(c.dbPath(database), tableName)
}

// GetTable opens a FileStoreTable at <warehouse>/<database>.db/<tableName>.
// Rather than checking existence via a single object lookup (which fails on GCS
// because table paths are key prefixes, not objects), we open the table directly
// and let the schema read produce a clear error if the path does not exist.
func (c *FileSystemCatalog) GetTable(ctx context.Context, database, tableName string) (*table.FileStoreTable, error) {
	tp := c.tablePath(database, tableName)
	return table.NewFileStoreTable(ctx, tp, c.io)
}

// ListDatabases lists all <name>.db directories under the warehouse.
func (c *FileSystemCatalog) ListDatabases(ctx context.Context) ([]string, error) {
	entries, err := c.io.List(ctx, c.warehouse)
	if err != nil {
		return nil, fmt.Errorf("catalog: list warehouse %s: %w", c.warehouse, err)
	}
	var dbs []string
	for _, e := range entries {
		if !e.IsDir {
			continue
		}
		name := pathutil.Base(e.Path)
		if strings.HasSuffix(name, ".db") {
			dbs = append(dbs, strings.TrimSuffix(name, ".db"))
		}
	}
	return dbs, nil
}

// ListTables lists all table directories under <warehouse>/<database>.db/.
func (c *FileSystemCatalog) ListTables(ctx context.Context, database string) ([]string, error) {
	dbPath := c.dbPath(database)
	entries, err := c.io.List(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("catalog: list database %s: %w", database, err)
	}
	var tables []string
	for _, e := range entries {
		if e.IsDir {
			tables = append(tables, pathutil.Base(e.Path))
		}
	}
	return tables, nil
}

// Close releases the underlying FileIO.
func (c *FileSystemCatalog) Close() error {
	return c.io.Close()
}
