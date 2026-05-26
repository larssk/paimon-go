# paimon-go

A minimalistic, pure-Go reader for [Apache Paimon](https://paimon.apache.org/) tables. No JVM required.

## Status

Early development — read-only, Parquet data files only.
Supports both append-only and primary-key tables (deduplicate merge engine).

## What is included

### Storage backends

| Backend | Notes |
|---|---|
| Local filesystem | `os.*` — always available |
| Google Cloud Storage | via `cloud.google.com/go/storage`; ADC or explicit service-account key |

### Catalog

- **Filesystem catalog** — resolves tables from a warehouse directory using the convention `<warehouse>/<database>.db/<table>`
- `ListDatabases`, `ListTables`, `GetTable`

### Metadata reading

- **Snapshot resolution** — reads `snapshot/snapshot-<id>` JSON files; always picks the latest snapshot
- **Schema parsing** — reads `schema/schema-<id>` JSON files; handles both plain-string types (`"INT NOT NULL"`, `"VARCHAR(255)"`) and object types (`{"type":"ARRAY","element":"BIGINT"}`); full Paimon type system including DECIMAL precision/scale, TIMESTAMP precision, nested ARRAY / MAP / ROW
- **Manifest list** — reads `manifest/manifest-list-*` Avro files → `[]ManifestFileMeta` with partition statistics
- **Manifest entries** — reads `manifest/manifest-*` Avro files in parallel (up to 8 goroutines) → resolves ADD/DELETE pairs → `[]ManifestEntry` with per-file statistics and metadata
- **BinaryRow decoder** — decodes Paimon's compact binary row format (used for partition min/max statistics and primary-key comparison); supports all atomic types including inline and heap-allocated strings

### Read pipeline — append-only tables

- `ReadBuilder` — entry point; attach filter predicate and/or column projection
- `TableScan.Plan()` — prunes manifest files and individual data files using partition and column statistics (stats-based pruning via BinaryRow decoded on demand)
- `TableRead.ToArrowReader()` — streams `arrow.RecordBatch` chunks (65 536 rows per batch)
- `TableRead.ToArrow()` — convenience method returning a single `arrow.Table`
- **Column projection** — only requested columns are read from Parquet
- **Schema evolution / missing columns** — columns present in the schema but absent from a given Parquet file are returned as null arrays
- **Type compatibility** — handles minor Arrow type mismatches between file physical type and schema type (e.g. `timestamp[us]` vs `timestamp[us, tz=UTC]`)

### Read pipeline — primary-key tables (merge-on-read)

Primary-key tables are detected automatically from the schema (`primaryKeys` field). The full
merge-on-read pipeline runs transparently inside `ToArrowReader` / `ToArrow` — callers use
the same API as for append-only tables.

- **`intervalPartition`** — groups files within a bucket by overlapping key ranges into
  non-overlapping sections; level-0 (overlapping) and level-N (sorted, non-overlapping)
  files are handled correctly
- **`sortMergeReader`** — min-heap merge across SortedRuns within each section; advances
  one row at a time from each run, grouping rows with identical primary keys and picking
  the winner by highest sequence number
- **Deduplicate merge engine** — keeps the row with the highest `_SEQUENCE_NUMBER` per
  primary key; all other merge engine options (`partial-update`, `aggregation`, `first-row`)
  also apply deduplicate semantics — this matches paimon-python's behaviour
- **Row-kind filtering** — `UPDATE_BEFORE` (kind=1) and `DELETE` (kind=3) rows are dropped;
  only `INSERT` (kind=0) and `UPDATE_AFTER` (kind=2) rows are emitted
- **Output schema** — internal PK columns (`_SEQUENCE_NUMBER`, `_VALUE_KIND`) are stripped;
  output contains only the user-visible value fields

### Streaming read

- `StreamReadBuilder` — entry point for continuous reads; attach filter, projection, poll interval, and starting position
- `TableStream.Next()` — blocks until a new `APPEND` snapshot appears; returns splits for only the newly added files in that commit; skips `COMPACT` / `OVERWRITE` / `ANALYZE` snapshots so no data is ever re-emitted after compaction
- `TableStreamReader` — implements `array.RecordReader` across an unbounded stream; drives `TableStream` internally and blocks on context cancellation
- `StartingFromLatest` — skips all existing data; emits only snapshots that arrive after the stream is started
- `StartingFromEarliest` — replays all existing `APPEND` snapshots from the beginning, then continues polling

> **Note:** Streaming is currently implemented for append-only commits only. PK table
> streaming reads deduplicated snapshots but does not track changelog semantics across
> multiple commits.

### Predicate / filter

- `PredicateBuilder` — builds typed predicates: `Equal`, `NotEqual`, `LessThan`, `LessOrEqual`, `GreaterThan`, `GreaterOrEqual`, `IsNull`, `IsNotNull`, `In`
- Logical combinators: `And`, `Or`, `Not`
- **Stats-based pruning** at both manifest-file level (partition stats) and data-file level (column value stats)
- Predicate index rebinding when used with column projection

### Output formats

| Method | Returns |
|---|---|
| `ToArrowReader()` | `array.RecordReader` — streaming batches |
| `ToArrow()` | `arrow.Table` — all data in memory |

### Data file formats

| Format | Status |
|---|---|
| Parquet | Supported (`.parquet`, all common compressions via `apache/arrow-go`) |
| ORC | Not supported |
| Avro (data files) | Not supported |
| Lance / Vortex / Blob | Not supported |

> **Note:** Paimon's default file format is ORC. To use this library, tables must be written with `'file.format' = 'parquet'`.

---

## What is out of scope (v1)

### Table types

- **Deletion vectors** — an alternative compaction strategy for primary-key tables; DV files
  referenced in `DataFileMeta.ExtraFiles` are not yet decoded or applied

### Catalog and metadata

- **REST catalog** — only the filesystem catalog is implemented
- **Tag-based and timestamp-based time travel** — only the latest snapshot is resolved
- **Schema evolution (type changes)** — columns added after table creation are null-filled correctly, but type changes (e.g. INT → BIGINT) are not handled
- **Index files** — BTree / full-text / vector global indexes are not read

### Data formats

- **ORC** — Paimon's default; thin Go library support requires CGO or a separate implementation
- **Avro data files** — manifest Avro is supported, but Avro as a data file format is not
- **Lance / Vortex / Blob** — no stable Go equivalents

### Storage

- **S3 / S3-compatible** (MinIO, etc.) — not yet wired up; straightforward addition via `gocloud.dev/blob` or AWS SDK v2
- **HDFS** — requires CGO or WebHDFS REST

### Write path

- No `TableWrite`, `FileStoreCommit`, or any mutation operations

---

## Compared to paimon-python

[paimon-python](https://github.com/apache/paimon-python) is the reference Python client
for Apache Paimon. The table below shows feature parity as of this writing.

| Feature | paimon-go | paimon-python |
|---|---|---|
| Append-only table read | ✓ | ✓ |
| Primary-key table read (deduplicate) | ✓ | ✓ |
| Aggregation / partial-update merge engines | — (falls back to deduplicate) | — (same) |
| Deletion vector support | — | ✓ |
| Filesystem catalog | ✓ | ✓ |
| REST catalog | — | ✓ |
| Parquet data files | ✓ | ✓ |
| ORC data files | — | ✓ |
| Avro data files | — | ✓ |
| Lance data files | — | ✓ |
| Local filesystem | ✓ | ✓ |
| Google Cloud Storage | ✓ | — |
| S3 / S3-compatible | — | ✓ |
| HDFS | — | ✓ |
| Column projection | ✓ | ✓ |
| Predicate / filter pushdown | ✓ | ✓ |
| Stats-based file pruning | ✓ | ✓ |
| Streaming / incremental reads | ✓ (append-only commits) | — |
| Time travel (tag / timestamp) | — | ✓ |
| Schema evolution (added columns) | ✓ (null-filled) | ✓ |
| Schema evolution (type changes) | — | ✓ |
| Write path | — (read-only) | ✓ |
| Output format | Apache Arrow (`arrow.Table` / `array.RecordReader`) | PyArrow / pandas |
| JVM required | No | No |
| CGO required | No | No |

> Neither paimon-go nor paimon-python implements aggregation or partial-update merge
> engines. Both libraries apply deduplicate (latest-value) semantics for all PK table
> types that are not explicitly handled.

---

## Roadmap

Prioritised next steps, roughly in order:

1. **E2E tests against real fixtures** — integration tests reading tables written by Java
   or paimon-python to validate the full pipeline end-to-end without mocks. See `TODO.md`
   for the detailed plan and fixture generation options.

2. **Deletion vector support** — read DV files referenced via `ExtraFiles` in
   `DataFileMeta`; apply row-level deletes to PK table reads. Required for tables using
   `'deletion-vectors.enabled' = 'true'`.

3. **S3 / S3-compatible storage** — wire up `gocloud.dev/blob` or `aws-sdk-go-v2`;
   straightforward addition, no core changes needed.

4. **Time travel** — resolve snapshots by tag name or timestamp rather than always
   picking the latest.

5. **ORC data files** — blocked on a pure-Go ORC reader with no CGO requirement.

6. **Partition filter pushdown on PK tables** — currently works for append-only;
   PK-specific predicate pruning tests needed.

7. **REST catalog** — implement `catalog.RESTCatalog` for managed / cloud-hosted
   Paimon deployments.

8. **Aggregation merge engine** — per-field aggregate functions
   (`fields.<name>.aggregate-function = sum|max|min|…`) for PK tables that use the
   aggregation merge engine.

---

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/apache/arrow-go/v18` | Arrow in-memory format + Parquet reader |
| `github.com/hamba/avro/v2` | Read manifest-list and manifest-entry Avro files |
| `cloud.google.com/go/storage` | GCS storage backend |

---

## Quick start

```sh
go get github.com/apache/paimon/paimon-go
```

```go
ctx := context.Background()

cat, err := paimon.NewCatalog(ctx, paimon.Options{
    Warehouse: "gs://my-bucket/warehouse", // or a local path
})
tbl, err := cat.GetTable(ctx, "mydb", "mytable")

rb := read.NewReadBuilder(tbl)
plan, err := rb.NewScan().Plan(ctx)

reader, err := rb.NewRead().ToArrowReader(ctx, plan.Splits)
defer reader.Release()

for reader.Next() {
    rec := reader.RecordBatch()
    // use rec ...
    rec.Release()
}
```

## Example program

A ready-to-run example that prints schema and rows to stdout is available at
`examples/read_table/`:

```sh
# Local
go run ./examples/read_table \
  --warehouse /path/to/warehouse \
  --database mydb \
  --table mytable \
  --limit 100

# GCS
go run ./examples/read_table \
  --warehouse gs://my-bucket/warehouse \
  --database mydb \
  --table mytable \
  --gcs-creds /path/to/sa.json
```

## Module layout

```
paimon-go/
├── paimon.go               # Entry point: NewCatalog()
├── catalog/                # FileSystemCatalog
├── fileio/                 # FileIO interface (local + GCS)
├── snapshot/               # Snapshot JSON + SnapshotManager
├── schema/                 # TableSchema JSON + Arrow type mapping
├── manifest/               # Manifest-list + manifest-entry Avro readers
├── table/                  # FileStoreTable + PathFactory
├── read/                   # ReadBuilder, TableScan, TableRead, StreamReadBuilder, TableStream
├── predicate/              # Predicate, PredicateBuilder, stats pruning
└── internal/
    ├── binaryrow/          # Paimon BinaryRow binary format decoder + key comparator
    └── pathutil/           # URI-safe path joining (handles gs://, s3://)
```
