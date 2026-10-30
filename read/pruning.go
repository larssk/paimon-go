package read

import (
	"github.com/larssk/paimon-go/manifest"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

// Both batch and streaming scans must use the same conservative pruning rules.
func pruneManifests(metas []manifest.ManifestFileMeta, filter *predicate.Predicate, s *schema.TableSchema) []manifest.ManifestFileMeta {
	p := predicate.WithProjection(filter, s.PartitionFields())
	if p == nil {
		return metas
	}
	result := metas[:0]
	for _, m := range metas {
		if predicate.TestByStats(p, m.PartitionStats, m.NumAddedFiles+m.NumDeletedFiles) {
			result = append(result, m)
		}
	}
	return result
}

func pruneDataFiles(entries []manifest.ManifestEntry, filter *predicate.Predicate, s *schema.TableSchema) []manifest.ManifestEntry {
	if filter == nil {
		return entries
	}
	partFields := s.PartitionFields()
	partFilter := predicate.WithProjection(filter, partFields)
	keyFields := storageKeyFields(s)
	keyFilter := predicate.WithProjection(filter, keyFields)
	buckets := newBucketPruner(s, filter)
	valueFilter := filter
	if s.IsPrimaryKeyTable() {
		// A newer version (or tombstone) that fails a mutable-value predicate
		// must still suppress older versions during merge-on-read.
		valueFilter = predicate.WithProjection(filter, s.PrimaryKeyFields())
	}
	result := entries[:0]
	for _, e := range entries {
		if e.Partition != nil && e.Partition.Arity() == len(partFields) {
			stats := manifest.SimpleStats{MinValues: e.Partition, MaxValues: e.Partition}
			stats.NullCounts = make([]interface{}, len(partFields))
			for i := range partFields {
				var n int64
				if e.Partition.IsNull(i) {
					n = 1
				}
				stats.NullCounts[i] = n
			}
			if !predicate.TestByStats(partFilter, stats, 1) {
				continue
			}
		}
		// Without a historical schema mapping, do not interpret old file stats
		// using the latest schema's positions or types.
		if e.File.SchemaID == s.ID {
			if !buckets.mayContain(e.Bucket, e.TotalBuckets) {
				continue
			}
			if s.IsPrimaryKeyTable() {
				if !predicate.TestByKeyRange(keyFilter, e.File.KeyBounds(), keyFields) {
					continue
				}
				if !predicate.TestByStats(keyFilter, e.File.KeyStats, e.File.RowCount) {
					continue
				}
			}
			fields := s.Fields
			if e.File.ValueStatsCols != nil {
				fields = make([]schema.DataField, len(e.File.ValueStatsCols))
				for i, name := range e.File.ValueStatsCols {
					fields[i], _ = s.FieldByName(name)
				}
			}
			p := predicate.WithProjection(valueFilter, fields)
			if !predicate.TestByStats(p, e.File.ValueStats, e.File.RowCount) {
				continue
			}
		}
		result = append(result, e)
	}
	return result
}

// Paimon omits partition columns from the primary key stored in file metadata.
func storageKeyFields(s *schema.TableSchema) []schema.DataField {
	parts := make(map[string]bool, len(s.PartitionKeys))
	for _, name := range s.PartitionKeys {
		parts[name] = true
	}
	var fields []schema.DataField
	for _, f := range s.PrimaryKeyFields() {
		if !parts[f.Name] {
			fields = append(fields, f)
		}
	}
	return fields
}
