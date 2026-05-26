// Package snapshot reads Paimon snapshot metadata.
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/pathutil"
)

// CommitKind matches Paimon's CommitKind enum and describes what kind of
// write produced a snapshot. Stream readers use this to decide which snapshots
// contain new logical rows.
type CommitKind string

const (
	// CommitAppend is produced by INSERT / streaming append writes.
	// These snapshots contain new ADD entries in the delta manifest and are
	// the only kind emitted by [read.TableStream.Next].
	CommitAppend CommitKind = "APPEND"

	// CommitOverwrite is produced by INSERT OVERWRITE writes that replace
	// an entire partition. The delta manifest contains DELETE entries for
	// the old files and ADD entries for the new ones.
	CommitOverwrite CommitKind = "OVERWRITE"

	// CommitCompact is produced by the background compaction job. It
	// reorganises physical files for read performance but does not add new
	// logical rows. Stream readers skip these snapshots.
	CommitCompact CommitKind = "COMPACT"

	// CommitAnalyze is produced when table statistics are collected.
	// It does not change any data files.
	CommitAnalyze CommitKind = "ANALYZE"
)

// Snapshot represents a Paimon snapshot (stored as JSON at snapshot/<id>).
type Snapshot struct {
	Version                int        `json:"version"`
	ID                     int64      `json:"id"`
	SchemaID               int64      `json:"schemaId"`
	BaseManifestList       string     `json:"baseManifestList"`
	DeltaManifestList      string     `json:"deltaManifestList"`
	TotalRecordCount       int64      `json:"totalRecordCount"`
	DeltaRecordCount       int64      `json:"deltaRecordCount"`
	CommitUser             string     `json:"commitUser"`
	CommitIdentifier       int64      `json:"commitIdentifier"`
	CommitKind             CommitKind `json:"commitKind"`
	TimeMillis             int64      `json:"timeMillis"`
	ChangelogManifestList  *string    `json:"changelogManifestList,omitempty"`
	IndexManifest          *string    `json:"indexManifest,omitempty"`
	ChangelogRecordCount   *int64     `json:"changelogRecordCount,omitempty"`
	Watermark              *int64     `json:"watermark,omitempty"`
	NextRowID              *int64     `json:"nextRowId,omitempty"`
	Statistics             *string    `json:"statistics,omitempty"`
}

// Manager resolves snapshots from a table's snapshot/ directory.
type Manager struct {
	tableRoot string
	io        fileio.FileIO
}

// NewManager creates a SnapshotManager for the given table root path.
func NewManager(tableRoot string, io fileio.FileIO) *Manager {
	return &Manager{tableRoot: tableRoot, io: io}
}

const snapshotPrefix = "snapshot-"

func (m *Manager) snapshotDir() string {
	return pathutil.Join(m.tableRoot, "snapshot")
}

// Latest returns the snapshot with the highest ID.
func (m *Manager) Latest(ctx context.Context) (*Snapshot, error) {
	ids, err := m.ListIDs(ctx)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("snapshot: no snapshots found in %s", m.snapshotDir())
	}
	return m.Read(ctx, ids[len(ids)-1])
}

// ListIDs returns all available snapshot IDs in ascending order.
func (m *Manager) ListIDs(ctx context.Context) ([]int64, error) {
	entries, err := m.io.List(ctx, m.snapshotDir())
	if err != nil {
		return nil, fmt.Errorf("snapshot: list dir: %w", err)
	}
	var ids []int64
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		name := pathutil.Base(e.Path)
		if !strings.HasPrefix(name, snapshotPrefix) {
			continue
		}
		id, err := strconv.ParseInt(name[len(snapshotPrefix):], 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Read loads a specific snapshot by ID.
func (m *Manager) Read(ctx context.Context, id int64) (*Snapshot, error) {
	p := pathutil.Join(m.snapshotDir(), snapshotPrefix+strconv.FormatInt(id, 10))
	data, err := m.io.ReadAll(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read %d: %w", id, err)
	}
	var s Snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("snapshot: parse %d: %w", id, err)
	}
	return &s, nil
}

// EarliestID returns the smallest snapshot ID available, or -1 if none.
func (m *Manager) EarliestID(ctx context.Context) (int64, error) {
	// Paimon may write an "EARLIEST" hint file; fall back to listing.
	hintPath := pathutil.Join(m.snapshotDir(), "EARLIEST")
	if data, err := m.io.ReadAll(ctx, hintPath); err == nil {
		id, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err == nil {
			return id, nil
		}
	}
	ids, err := m.ListIDs(ctx)
	if err != nil {
		return -1, err
	}
	if len(ids) == 0 {
		return -1, nil
	}
	return ids[0], nil
}
