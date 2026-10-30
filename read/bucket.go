package read

import (
	"strconv"
	"strings"

	"github.com/larssk/paimon-go/internal/binaryrow"
	"github.com/larssk/paimon-go/predicate"
	"github.com/larssk/paimon-go/schema"
)

const maxBucketKeyCombinations = 1000

type bucketPruner struct {
	hashes  []int32
	byCount map[int]map[int]bool
}

func newBucketPruner(s *schema.TableSchema, filter *predicate.Predicate) *bucketPruner {
	count, err := strconv.ParseInt(s.Options["bucket"], 10, 32)
	if err != nil || count <= 0 || filter == nil || strings.TrimSpace(s.Options["bucket-function.provider"]) != "" {
		return nil
	}
	function := strings.TrimSpace(s.Options["bucket-function.type"])
	if function != "" && !strings.EqualFold(function, "default") {
		return nil
	}
	fields := storageKeyFields(s)
	if option := strings.TrimSpace(s.Options["bucket-key"]); option != "" {
		fields = nil
		for _, name := range strings.Split(option, ",") {
			name = strings.TrimSpace(name)
			f, ok := s.FieldByName(name)
			if !ok || name == "" {
				return nil
			}
			for _, part := range s.PartitionKeys {
				if name == part {
					return nil
				}
			}
			if s.IsPrimaryKeyTable() {
				isKey := false
				for _, key := range s.PrimaryKeys {
					isKey = isKey || key == name
				}
				if !isKey {
					return nil
				}
			}
			fields = append(fields, f)
		}
	}
	if len(fields) == 0 {
		return nil // append-only tables without an explicit bucket-key
	}
	values := make([][]interface{}, len(fields))
	combinations := 1
	for i, f := range fields {
		var known bool
		values[i], known = finiteBucketValues(filter, f.Name)
		if !known || len(values[i]) == 0 || len(values[i]) > maxBucketKeyCombinations/combinations {
			return nil
		}
		combinations *= len(values[i])
	}
	p := &bucketPruner{byCount: make(map[int]map[int]bool)}
	key := make([]interface{}, len(fields))
	var assemble func(int) bool
	assemble = func(i int) bool {
		if i == len(fields) {
			hash, ok := binaryrow.BucketKeyHash(fields, key)
			if !ok {
				return false
			}
			p.hashes = append(p.hashes, hash)
			return true
		}
		for _, value := range values[i] {
			key[i] = value
			if !assemble(i + 1) {
				return false
			}
		}
		return true
	}
	if !assemble(0) {
		return nil
	}
	return p
}

// finiteBucketValues returns a superset of possible values for one field.
// AND can use any known necessary restriction; OR needs every branch known.
// Cross-field correlations are intentionally relaxed into a Cartesian product.
func finiteBucketValues(p *predicate.Predicate, name string) ([]interface{}, bool) {
	if p == nil {
		return nil, false
	}
	switch p.Op {
	case predicate.OpAnd:
		for _, child := range p.Children {
			if values, ok := finiteBucketValues(child, name); ok {
				return values, true
			}
		}
	case predicate.OpOr:
		var result []interface{}
		for _, child := range p.Children {
			values, ok := finiteBucketValues(child, name)
			if !ok || len(values) > maxBucketKeyCombinations-len(result) {
				return nil, false
			}
			result = append(result, values...)
		}
		return result, len(result) > 0
	case predicate.OpEqual, predicate.OpIn:
		if p.FieldName == name && len(p.Literals) > 0 && len(p.Literals) <= maxBucketKeyCombinations {
			return p.Literals, true
		}
	case predicate.OpIsNull:
		if p.FieldName == name {
			return []interface{}{nil}, true
		}
	}
	return nil, false
}

func (p *bucketPruner) mayContain(bucket, count int) bool {
	if p == nil || count <= 0 || count > 2147483647 || bucket < 0 || bucket >= count {
		return true
	}
	buckets, ok := p.byCount[count]
	if !ok {
		buckets = make(map[int]bool)
		for _, hash := range p.hashes {
			// Java uses abs(hash % count), NOT abs(hash) % count or floorMod.
			b := int(int64(hash) % int64(count))
			if b < 0 {
				b = -b
			}
			buckets[b] = true
		}
		p.byCount[count] = buckets
	}
	return buckets[bucket]
}
