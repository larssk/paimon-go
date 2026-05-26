// Package paimon provides a minimalistic Go reader for Apache Paimon tables.
//
// The typical read pipeline is: open a Catalog → look up a table → build a
// scan plan → read Arrow record batches. All operations are context-aware and
// can be cancelled at any point.
//
// # Batch read
//
// Read all rows from a local table as Arrow record batches:
//
//	import (
//	    "github.com/apache/paimon/paimon-go"
//	    "github.com/apache/paimon/paimon-go/read"
//	)
//
//	cat, err := paimon.NewCatalog(ctx, paimon.Options{Warehouse: "/data/warehouse"})
//	tbl, err := cat.GetTable(ctx, "mydb", "orders")
//
//	rb := read.NewReadBuilder(tbl)
//	plan, err := rb.NewScan().Plan(ctx)
//
//	reader, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
//	defer reader.Release()
//	for reader.Next() {
//	    rec := reader.Record()
//	    // process rec — an arrow.RecordBatch with one batch of rows
//	    rec.Release()
//	}
//	if err := reader.Err(); err != nil { /* handle */ }
//
// To materialise all splits into a single in-memory arrow.Table instead:
//
//	tbl, err := rb.NewRead().ToArrow(ctx, plan.Splits)
//	defer tbl.Release()
//
// # Batch read with projection and filter
//
// Limit columns and push down a row filter to skip irrelevant files and rows:
//
//	rb := read.NewReadBuilder(tbl).
//	    WithProjection([]string{"event_time", "user_id", "amount"})
//
//	pb := rb.NewPredicateBuilder()
//	gt, err := pb.GreaterThan("amount", float64(100))
//
//	rb = rb.WithFilter(gt)
//
//	plan, err := rb.NewScan().Plan(ctx)
//	reader, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
//	// ... iterate as above
//
// Combine multiple conditions with And / Or / Not:
//
//	import "github.com/apache/paimon/paimon-go/predicate"
//
//	gt, _  := pb.GreaterThan("amount", float64(100))
//	eq, _  := pb.Equal("status", "PAID")
//	both   := predicate.And(gt, eq)
//	rb      = rb.WithFilter(both)
//
// # Stream read
//
// Continuously consume new APPEND snapshots as they land. Next blocks until a
// new snapshot is available or the context is cancelled.
//
//	sb := read.NewStreamReadBuilder(tbl).
//	    WithProjection([]string{"event_time", "user_id"}).
//	    WithStartingFrom(read.StartingFromLatest)
//
//	stream := sb.NewStream()
//	for {
//	    batch, err := stream.Next(ctx)
//	    if err != nil { break } // context cancelled or I/O error
//
//	    reader, err := sb.NewRead().ToArrowReader(ctx, batch.Splits)
//	    // ... iterate record batches, then release
//	    reader.Release()
//	}
//
// Use [read.StartingFromEarliest] to replay all existing snapshots before
// tailing new ones.
//
// # GCS storage
//
// Pass a service-account credential to read from Google Cloud Storage:
//
//	cat, err := paimon.NewCatalog(ctx, paimon.Options{
//	    Warehouse:     "gs://my-bucket/warehouse",
//	    FileIOOptions: []paimon.FileIOOption{
//	        paimon.WithCredentialsFile("/path/to/sa.json"),
//	    },
//	})
//
// Or supply the JSON key directly:
//
//	cat, err := paimon.NewCatalog(ctx, paimon.Options{
//	    Warehouse:     "gs://my-bucket/warehouse",
//	    FileIOOptions: []paimon.FileIOOption{
//	        paimon.WithCredentialsJSON([]byte(`{"type":"service_account",...}`)),
//	    },
//	})
//
// # See also
//
//   - [read.NewReadBuilder] — batch read entry point
//   - [read.NewStreamReadBuilder] — streaming read entry point
//   - [predicate.Builder] — filter predicate construction
package paimon

import (
	"context"

	"github.com/apache/paimon/paimon-go/catalog"
	"github.com/apache/paimon/paimon-go/fileio"
)

// Options configures a Paimon catalog.
type Options = catalog.Options

// NewCatalog opens a Paimon filesystem catalog at the warehouse path in opts.
// It is the main entry point for all table reads.
//
// For local paths set opts.Warehouse to an absolute filesystem path.
// For GCS set it to a "gs://bucket/path" URI and supply credentials via
// opts.FileIOOptions (see [WithCredentialsFile] and [WithCredentialsJSON]).
//
// See the package-level documentation for full usage examples.
func NewCatalog(ctx context.Context, opts Options) (catalog.Catalog, error) {
	return catalog.New(ctx, opts)
}

// Re-export fileio.Option so callers don't need to import fileio directly.
type FileIOOption = fileio.Option

// WithCredentialsFile is a convenience re-export.
var WithCredentialsFile = fileio.WithCredentialsFile

// WithCredentialsJSON is a convenience re-export.
var WithCredentialsJSON = fileio.WithCredentialsJSON
