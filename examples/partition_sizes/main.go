// partition_sizes prints data file size (MB), file count, and row count per
// partition for a Paimon table, plus a detailed file-size summary (min/max/
// average file size, sequence-number range, per-level breakdown, and
// deletion-vector usage) for the selected partition(s).
//
// Usage:
//
//	go run ./examples/partition_sizes \
//	  --warehouse /path/to/warehouse \
//	  --database mydb \
//	  --table mytable
//
// Optionally restrict to a single partition:
//
//	--partition dt:2024-01-01
//	--partition dt:2024-01-01,region:eu-west
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/larssk/paimon-go"
	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/read"
	"github.com/larssk/paimon-go/schema"
)

func main() {
	warehouse := flag.String("warehouse", "", "Warehouse path, local or gs://bucket/path  (required)")
	database := flag.String("database", "default", "Database name")
	tblName := flag.String("table", "", "Table name  (required)")
	partFilter := flag.String("partition", "", "Restrict to a partition: field:value,field:value")
	credsFile := flag.String("gcs-creds", "", "Path to GCS service-account JSON key file (GCS only)")
	flag.Parse()

	if *warehouse == "" || *tblName == "" {
		flag.Usage()
		log.Fatal("--warehouse and --table are required")
	}

	ctx := context.Background()

	opts := paimon.Options{Warehouse: *warehouse}
	if *credsFile != "" {
		opts.FileIOOptions = append(opts.FileIOOptions, paimon.WithCredentialsFile(*credsFile))
	}

	cat, err := paimon.NewCatalog(ctx, opts)
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}
	defer cat.Close()

	tbl, err := cat.GetTable(ctx, *database, *tblName)
	if err != nil {
		log.Fatalf("get table %s.%s: %v", *database, *tblName, err)
	}

	// Parse --partition filter into a map of field→value string pairs.
	partWant := parsePartitionFilter(*partFilter)

	plan, err := read.NewReadBuilder(tbl).NewScan().Plan(ctx)
	if err != nil {
		log.Fatalf("plan: %v", err)
	}

	partFields := tbl.Schema.PartitionFields()

	type row struct {
		label string
		files int
		rows  int64
		bytes int64
	}
	byLabel := make(map[string]*row)
	order := []string{}

	// bucketRow aggregates file/row/byte counts for one (partition, bucket) pair.
	byBucket := make(map[string]*bucketRow)
	bucketOrder := []string{}

	// fileStats aggregates file-level details across all matched splits,
	// used for the summary block printed after the partition table.
	fs := newFileStats()

	for _, split := range plan.Splits {
		label := formatPartition(split.Partition, partFields)

		// Apply --partition filter if specified.
		if len(partWant) > 0 && !matchesFilter(split.Partition, partFields, partWant) {
			continue
		}

		r, ok := byLabel[label]
		if !ok {
			r = &row{label: label}
			byLabel[label] = r
			order = append(order, label)
		}
		r.files += len(split.Files)

		bucketKey := fmt.Sprintf("%s\x00%d", label, split.Bucket)
		br, ok := byBucket[bucketKey]
		if !ok {
			br = &bucketRow{label: label, bucket: split.Bucket}
			byBucket[bucketKey] = br
			bucketOrder = append(bucketOrder, bucketKey)
		}
		br.files += len(split.Files)

		for _, f := range split.Files {
			r.bytes += f.FileSize
			r.rows += f.RowCount
			br.bytes += f.FileSize
			br.rows += f.RowCount
			fs.add(f)
		}
	}

	if len(byLabel) == 0 {
		if len(partWant) > 0 {
			log.Fatalf("no splits matched partition filter %q", *partFilter)
		}
		fmt.Println("no splits found")
		return
	}

	sort.Strings(order)

	// Column widths.
	const (
		wPart  = 40
		wFiles = 7
		wRows  = 14
		wMB    = 12
	)

	header := fmt.Sprintf("%-*s  %*s  %*s  %*s",
		wPart, "partition",
		wFiles, "files",
		wRows, "rows",
		wMB, "size (MB)",
	)
	sep := strings.Repeat("─", len(header))

	fmt.Println(header)
	fmt.Println(sep)

	var totalFiles int
	var totalRows, totalBytes int64

	for _, lbl := range order {
		r := byLabel[lbl]
		fmt.Printf("%-*s  %*d  %*d  %*.2f\n",
			wPart, r.label,
			wFiles, r.files,
			wRows, r.rows,
			wMB, float64(r.bytes)/(1024*1024),
		)
		totalFiles += r.files
		totalRows += r.rows
		totalBytes += r.bytes
	}

	fmt.Println(sep)
	fmt.Printf("%-*s  %*d  %*d  %*.2f\n",
		wPart, "total",
		wFiles, totalFiles,
		wRows, totalRows,
		wMB, float64(totalBytes)/(1024*1024),
	)

	printBucketBreakdown(byBucket, bucketOrder)
	printFileSummary(fs, len(byLabel) > 1)
}

// formatPartition builds a human-readable "k=v,k=v" label for a split's partition.
// Returns "(unpartitioned)" for tables without partition keys.
func formatPartition(entry *manifest.ManifestEntry, fields []schema.DataField) string {
	if entry == nil || entry.Partition == nil || len(fields) == 0 {
		return "(unpartitioned)"
	}
	parts := make([]string, 0, len(fields))
	for i, f := range fields {
		v, err := binaryrow.GetField(entry.Partition, i, schema.TypeTag(f.Type))
		if err != nil || v == nil {
			parts = append(parts, fmt.Sprintf("%s=NULL", f.Name))
		} else {
			parts = append(parts, fmt.Sprintf("%s=%v", f.Name, v))
		}
	}
	return strings.Join(parts, ",")
}

// matchesFilter returns true if the split's partition values match all pairs in want.
func matchesFilter(entry *manifest.ManifestEntry, fields []schema.DataField, want map[string]string) bool {
	if entry == nil || entry.Partition == nil {
		return len(want) == 0
	}
	for i, f := range fields {
		wantVal, ok := want[f.Name]
		if !ok {
			continue
		}
		v, err := binaryrow.GetField(entry.Partition, i, schema.TypeTag(f.Type))
		if err != nil {
			return false
		}
		if fmt.Sprintf("%v", v) != wantVal {
			return false
		}
	}
	return true
}

// parsePartitionFilter parses "field:value,field:value" into a map.
func parsePartitionFilter(raw string) map[string]string {
	m := make(map[string]string)
	if raw == "" {
		return m
	}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		idx := strings.IndexByte(pair, ':')
		if idx < 0 {
			log.Fatalf("--partition: pair %q has no ':' separator; expected field:value", pair)
		}
		m[strings.TrimSpace(pair[:idx])] = strings.TrimSpace(pair[idx+1:])
	}
	return m
}

// levelStats aggregates file count and total bytes for one LSM level.
type levelStats struct {
	files int
	bytes int64
}

// bucketRow aggregates file/row/byte counts for one (partition, bucket) pair.
type bucketRow struct {
	label  string
	bucket int
	files  int
	rows   int64
	bytes  int64
}

// printBucketBreakdown prints a per-(partition, bucket) file/row/size table,
// sorted by partition label then bucket number.
func printBucketBreakdown(byBucket map[string]*bucketRow, order []string) {
	if len(byBucket) == 0 {
		return
	}

	rows := make([]*bucketRow, 0, len(order))
	for _, key := range order {
		rows = append(rows, byBucket[key])
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].label != rows[j].label {
			return rows[i].label < rows[j].label
		}
		return rows[i].bucket < rows[j].bucket
	})

	const (
		wPart   = 40
		wBucket = 8
		wFiles  = 7
		wRows   = 14
		wMB     = 12
	)

	fmt.Println()
	fmt.Println("files per bucket")
	header := fmt.Sprintf("%-*s  %*s  %*s  %*s  %*s",
		wPart, "partition",
		wBucket, "bucket",
		wFiles, "files",
		wRows, "rows",
		wMB, "size (MB)",
	)
	sep := strings.Repeat("─", len(header))
	fmt.Println(header)
	fmt.Println(sep)

	for _, r := range rows {
		fmt.Printf("%-*s  %*d  %*d  %*d  %*.2f\n",
			wPart, r.label,
			wBucket, r.bucket,
			wFiles, r.files,
			wRows, r.rows,
			wMB, float64(r.bytes)/(1024*1024),
		)
	}
}

// fileStats aggregates file-level metadata across all matched data files,
// used to print a detailed size/level/deletion-vector summary.
type fileStats struct {
	files int
	rows  int64
	bytes int64

	minBytes int64
	maxBytes int64

	minSeq  int64
	maxSeq  int64
	haveSeq bool

	byLevel map[int]*levelStats

	dvFiles     int
	deletedRows int64
}

func newFileStats() *fileStats {
	return &fileStats{byLevel: make(map[int]*levelStats)}
}

// add folds one DataFileMeta's metadata into the running aggregate.
func (fs *fileStats) add(f manifest.DataFileMeta) {
	fs.files++
	fs.rows += f.RowCount
	fs.bytes += f.FileSize

	if fs.files == 1 || f.FileSize < fs.minBytes {
		fs.minBytes = f.FileSize
	}
	if f.FileSize > fs.maxBytes {
		fs.maxBytes = f.FileSize
	}

	if !fs.haveSeq {
		fs.minSeq = f.MinSequenceNumber
		fs.maxSeq = f.MaxSequenceNumber
		fs.haveSeq = true
	} else {
		if f.MinSequenceNumber < fs.minSeq {
			fs.minSeq = f.MinSequenceNumber
		}
		if f.MaxSequenceNumber > fs.maxSeq {
			fs.maxSeq = f.MaxSequenceNumber
		}
	}

	lv, ok := fs.byLevel[f.Level]
	if !ok {
		lv = &levelStats{}
		fs.byLevel[f.Level] = lv
	}
	lv.files++
	lv.bytes += f.FileSize

	if len(f.ExtraFiles) > 0 {
		fs.dvFiles++
	}
	if f.DeleteRowCount != nil {
		fs.deletedRows += *f.DeleteRowCount
	}
}

// printFileSummary prints a detailed file-size/level/deletion-vector summary
// for the aggregated file set. multiPartition indicates whether the
// aggregate spans more than one partition, adjusting the header label.
func printFileSummary(fs *fileStats, multiPartition bool) {
	fmt.Println()
	if multiPartition {
		fmt.Println("file size summary (selected partitions)")
	} else {
		fmt.Println("file size summary")
	}

	if fs.files == 0 {
		fmt.Println("  (no files)")
		return
	}

	avgBytes := float64(fs.bytes) / float64(fs.files)
	fmt.Printf("  smallest file:      %s\n", formatBytes(fs.minBytes))
	fmt.Printf("  largest file:       %s\n", formatBytes(fs.maxBytes))
	fmt.Printf("  average file:       %s\n", formatBytes(int64(avgBytes)))

	if fs.rows > 0 {
		avgRowBytes := float64(fs.bytes) / float64(fs.rows)
		fmt.Printf("  average row:        %.2f B\n", avgRowBytes)
	}

	if fs.haveSeq {
		fmt.Printf("  sequence range:     %d - %d\n", fs.minSeq, fs.maxSeq)
	}

	levels := make([]int, 0, len(fs.byLevel))
	for lv := range fs.byLevel {
		levels = append(levels, lv)
	}
	sort.Ints(levels)
	parts := make([]string, 0, len(levels))
	for _, lv := range levels {
		s := fs.byLevel[lv]
		parts = append(parts, fmt.Sprintf("L%d=%d (%s)", lv, s.files, formatBytes(s.bytes)))
	}
	fmt.Printf("  files by level:     %s\n", strings.Join(parts, ", "))

	fmt.Printf("  deletion vectors:   %d file(s), %d deleted row(s)\n", fs.dvFiles, fs.deletedRows)
}

// formatBytes renders a byte count using the largest unit that keeps the
// value >= 1 (B, KiB, MiB, GiB), with two decimal places for units above B.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), units[exp])
}
