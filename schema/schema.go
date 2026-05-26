// Package schema reads Paimon table schema metadata.
package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/paimon/paimon-go/fileio"
	"github.com/apache/paimon/paimon-go/internal/pathutil"
)

// DataType represents a Paimon field type.
//
// Paimon serialises simple types as a plain JSON string (e.g. "INT NOT NULL",
// "VARCHAR(255)") and complex types as a JSON object with a "type" key
// (e.g. {"type":"ARRAY NOT NULL","element":"INT"}). Nullability is encoded
// via the absence/presence of "NOT NULL" in the type string — there is no
// separate "nullable" boolean field in the wire format.
type DataType struct {
	// Type is the base type name in uppercase, e.g. "INT", "BIGINT", "ARRAY", "MAP".
	// For parameterised types the raw string is preserved here, e.g. "VARCHAR(255)".
	Type     string
	Nullable bool
	// For DECIMAL
	Precision int
	Scale     int
	// For CHAR / VARCHAR / BINARY / VARBINARY — extracted from e.g. "VARCHAR(255)"
	Length int
	// For ARRAY / MULTISET
	ElementType *DataType
	// For MAP
	KeyType   *DataType
	ValueType *DataType
	// For ROW
	Fields []DataField
}

// UnmarshalJSON implements json.Unmarshaler.
// Handles both the string form ("INT NOT NULL") and the object form
// ({"type":"ARRAY","element":"BIGINT"}).
func (dt *DataType) UnmarshalJSON(data []byte) error {
	// Try string form first.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		parseTypeString(s, dt)
		return nil
	}

	// Object form.
	var obj struct {
		Type    string          `json:"type"`
		Element json.RawMessage `json:"element"`
		Key     json.RawMessage `json:"key"`
		Value   json.RawMessage `json:"value"`
		Fields  []DataField     `json:"fields"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("schema: cannot parse DataType from %s: %w", data, err)
	}

	parseTypeString(obj.Type, dt)

	upper := strings.ToUpper(baseTypeName(obj.Type))

	switch {
	case strings.HasPrefix(upper, "ARRAY"), strings.HasPrefix(upper, "MULTISET"):
		if obj.Element != nil {
			dt.ElementType = new(DataType)
			if err := json.Unmarshal(obj.Element, dt.ElementType); err != nil {
				return err
			}
		}
	case strings.HasPrefix(upper, "MAP"):
		if obj.Key != nil {
			dt.KeyType = new(DataType)
			if err := json.Unmarshal(obj.Key, dt.KeyType); err != nil {
				return err
			}
		}
		if obj.Value != nil {
			dt.ValueType = new(DataType)
			if err := json.Unmarshal(obj.Value, dt.ValueType); err != nil {
				return err
			}
		}
	case strings.HasPrefix(upper, "ROW"):
		dt.Fields = obj.Fields
	}

	return nil
}

// parseTypeString fills dt from a Paimon type string like "INT NOT NULL" or "DECIMAL(10,2)".
func parseTypeString(s string, dt *DataType) {
	upper := strings.ToUpper(strings.TrimSpace(s))
	dt.Nullable = !strings.Contains(upper, "NOT NULL")
	// Strip nullability suffix.
	upper = strings.TrimSpace(strings.TrimSuffix(upper, "NOT NULL"))
	upper = strings.TrimSpace(strings.TrimSuffix(upper, "NULL"))

	// Extract parameters e.g. VARCHAR(255), DECIMAL(10,2), TIMESTAMP(6).
	if idx := strings.IndexByte(upper, '('); idx >= 0 {
		base := strings.TrimSpace(upper[:idx])
		params := strings.TrimSuffix(strings.TrimSpace(upper[idx+1:]), ")")
		dt.Type = base
		parts := strings.SplitN(params, ",", 2)
		if v, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
			switch base {
			case "DECIMAL":
				dt.Precision = v
				if len(parts) == 2 {
					if s, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						dt.Scale = s
					}
				}
			case "TIMESTAMP", "TIMESTAMP_LTZ", "TIME":
				dt.Precision = v
			default:
				dt.Length = v
			}
		}
	} else {
		dt.Type = upper
	}
}

// baseTypeName strips NOT NULL / NULL and parameters from a type string, returning just the base name.
func baseTypeName(s string) string {
	upper := strings.ToUpper(strings.TrimSpace(s))
	upper = strings.TrimSpace(strings.TrimSuffix(upper, "NOT NULL"))
	upper = strings.TrimSpace(strings.TrimSuffix(upper, "NULL"))
	if idx := strings.IndexByte(upper, '('); idx >= 0 {
		return strings.TrimSpace(upper[:idx])
	}
	return upper
}

// DataField is a named, typed column in a table schema.
//
// ID is the stable field identifier used for schema evolution — it never
// changes even if the field is renamed or reordered. Name is the column name
// as visible to callers.
type DataField struct {
	ID          int      `json:"id"`
	Name        string   `json:"name"`
	Type        DataType `json:"type"`
	Description string   `json:"description,omitempty"`
}

// TableSchema is the JSON representation of a Paimon schema file
// (stored at schema/<id> inside the table directory).
//
// Version indicates the Paimon schema-file format version; defaults that
// depend on version (e.g. file format) are applied automatically when the
// schema is loaded. ID is the monotonically increasing schema ID; it advances
// each time the schema is altered via ALTER TABLE.
//
// Fields lists all columns in declaration order. PartitionKeys and PrimaryKeys
// are field names (not IDs); use [TableSchema.PartitionFields] and
// [TableSchema.PrimaryKeyFields] for typed access.
//
// Options mirrors the table's WITH(...) properties and is keyed by the Paimon
// option name (e.g. "merge-engine", "file.format").
type TableSchema struct {
	// Version is the schema-file format version (0, 1, or 2).
	Version int `json:"version"`
	// ID is the schema's monotonically increasing identifier.
	ID int64 `json:"id"`
	// Fields lists all columns in declaration order.
	Fields []DataField `json:"fields"`
	// HighestFieldID is the largest field ID ever assigned; used for schema evolution.
	HighestFieldID int `json:"highestFieldId"`
	// PartitionKeys are the names of the partition columns (empty for unpartitioned tables).
	PartitionKeys []string `json:"partitionKeys"`
	// PrimaryKeys are the names of the primary key columns (empty for append-only tables).
	PrimaryKeys []string `json:"primaryKeys"`
	// Options holds the table's WITH(...) properties, e.g. "merge-engine": "deduplicate".
	Options map[string]string `json:"options"`
	// Comment is an optional human-readable table description.
	Comment string `json:"comment,omitempty"`
	// TimeMillis is the epoch-millisecond timestamp when this schema was created.
	TimeMillis int64 `json:"timeMillis,omitempty"`
}

// IsPrimaryKeyTable returns true if the table has primary keys defined.
func (s *TableSchema) IsPrimaryKeyTable() bool {
	return len(s.PrimaryKeys) > 0
}

// MergeEngine returns the merge engine name from table options,
// defaulting to "deduplicate" if not set.
func (s *TableSchema) MergeEngine() string {
	if v, ok := s.Options["merge-engine"]; ok && v != "" {
		return strings.ToLower(v)
	}
	return "deduplicate"
}

// PrimaryKeyIndices returns the integer positions of each primary key field
// within the TableSchema.Fields slice.
func (s *TableSchema) PrimaryKeyIndices() []int {
	idx := make([]int, 0, len(s.PrimaryKeys))
	for _, pkName := range s.PrimaryKeys {
		for i, f := range s.Fields {
			if f.Name == pkName {
				idx = append(idx, i)
				break
			}
		}
	}
	return idx
}

// FileFormat returns the data file format, defaulting to "orc" for v<=2 schemas.
func (s *TableSchema) FileFormat() string {
	if f, ok := s.Options["file.format"]; ok {
		return strings.ToLower(f)
	}
	if s.Version <= 2 {
		return "orc"
	}
	return "orc"
}

// FieldByName returns the DataField with the given name, or (DataField{}, false).
func (s *TableSchema) FieldByName(name string) (DataField, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return DataField{}, false
}

// PartitionFields returns the DataFields for the partition keys in order.
func (s *TableSchema) PartitionFields() []DataField {
	return s.fieldsFor(s.PartitionKeys)
}

// PrimaryKeyFields returns the DataFields for the primary keys in order.
func (s *TableSchema) PrimaryKeyFields() []DataField {
	return s.fieldsFor(s.PrimaryKeys)
}

func (s *TableSchema) fieldsFor(names []string) []DataField {
	result := make([]DataField, 0, len(names))
	for _, name := range names {
		if f, ok := s.FieldByName(name); ok {
			result = append(result, f)
		}
	}
	return result
}

// Manager reads schema files from a table's schema/ directory.
type Manager struct {
	tableRoot string
	io        fileio.FileIO
}

// NewManager creates a SchemaManager for the given table root path.
func NewManager(tableRoot string, io fileio.FileIO) *Manager {
	return &Manager{tableRoot: tableRoot, io: io}
}

const schemaPrefix = "schema-"

func (m *Manager) schemaDir() string {
	return pathutil.Join(m.tableRoot, "schema")
}

// Latest returns the schema with the highest ID.
func (m *Manager) Latest(ctx context.Context) (*TableSchema, error) {
	entries, err := m.io.List(ctx, m.schemaDir())
	if err != nil {
		return nil, fmt.Errorf("schema: list dir: %w", err)
	}
	var ids []int64
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		name := pathutil.Base(e.Path)
		if !strings.HasPrefix(name, schemaPrefix) {
			continue
		}
		id, err := strconv.ParseInt(name[len(schemaPrefix):], 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("schema: no schema files found in %s", m.schemaDir())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return m.Read(ctx, ids[len(ids)-1])
}

// Read loads a specific schema by ID.
func (m *Manager) Read(ctx context.Context, id int64) (*TableSchema, error) {
	p := pathutil.Join(m.schemaDir(), schemaPrefix+strconv.FormatInt(id, 10))
	data, err := m.io.ReadAll(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("schema: read %d: %w", id, err)
	}
	var s TableSchema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("schema: parse %d: %w", id, err)
	}
	applyVersionDefaults(&s)
	return &s, nil
}

func applyVersionDefaults(s *TableSchema) {
	if s.Options == nil {
		s.Options = make(map[string]string)
	}
	if s.Version <= 1 {
		if _, ok := s.Options["bucket"]; !ok {
			s.Options["bucket"] = "1"
		}
	}
	if s.Version <= 2 {
		if _, ok := s.Options["file.format"]; !ok {
			s.Options["file.format"] = "orc"
		}
	}
}

// --- Arrow type mapping ---

// ToArrowField converts a DataField to an Arrow field.
func ToArrowField(f DataField) (arrow.Field, error) {
	t, err := toArrowType(f.Type)
	if err != nil {
		return arrow.Field{}, fmt.Errorf("schema: field %q: %w", f.Name, err)
	}
	return arrow.Field{
		Name:     f.Name,
		Type:     t,
		Nullable: f.Type.Nullable,
	}, nil
}

// ToArrowSchema converts a slice of DataFields to an Arrow schema.
func ToArrowSchema(fields []DataField) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, f := range fields {
		af, err := ToArrowField(f)
		if err != nil {
			return nil, err
		}
		arrowFields = append(arrowFields, af)
	}
	return arrow.NewSchema(arrowFields, nil), nil
}

func toArrowType(dt DataType) (arrow.DataType, error) {
	upper := strings.ToUpper(dt.Type)
	switch upper {
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean, nil
	case "TINYINT", "BYTE":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT", "SHORT":
		return arrow.PrimitiveTypes.Int16, nil
	case "INT", "INTEGER":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT", "LONG":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT", "REAL":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "STRING", "VARCHAR", "CHAR":
		return arrow.BinaryTypes.String, nil
	case "BINARY", "VARBINARY", "BYTES", "BLOB":
		return arrow.BinaryTypes.Binary, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "TIME":
		return arrow.FixedWidthTypes.Time32ms, nil
	case "TIMESTAMP":
		// Plain TIMESTAMP has no timezone — stored as local time in Parquet files.
		p := dt.Precision
		if p == 0 {
			p = 6
		}
		if p <= 3 {
			return &arrow.TimestampType{Unit: arrow.Millisecond}, nil
		}
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nil
	case "TIMESTAMP_LTZ":
		// TIMESTAMP WITH LOCAL TIME ZONE is always UTC.
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	case "DECIMAL":
		p := dt.Precision
		if p == 0 {
			p = 38
		}
		return &arrow.Decimal128Type{Precision: int32(p), Scale: int32(dt.Scale)}, nil
	case "ARRAY":
		if dt.ElementType == nil {
			return nil, fmt.Errorf("ARRAY type missing elementType")
		}
		elemType, err := toArrowType(*dt.ElementType)
		if err != nil {
			return nil, err
		}
		return arrow.ListOf(elemType), nil
	case "MAP":
		if dt.KeyType == nil || dt.ValueType == nil {
			return nil, fmt.Errorf("MAP type missing keyType or valueType")
		}
		keyType, err := toArrowType(*dt.KeyType)
		if err != nil {
			return nil, err
		}
		valType, err := toArrowType(*dt.ValueType)
		if err != nil {
			return nil, err
		}
		return arrow.MapOf(keyType, valType), nil
	case "ROW":
		rowFields := make([]arrow.Field, 0, len(dt.Fields))
		for _, f := range dt.Fields {
			af, err := ToArrowField(f)
			if err != nil {
				return nil, err
			}
			rowFields = append(rowFields, af)
		}
		return arrow.StructOf(rowFields...), nil
	default:
		// Unknown type — fall back to string
		return arrow.BinaryTypes.String, nil
	}
}

// TypeTag returns a simple uppercase tag for a DataType (used with binaryrow.GetField).
func TypeTag(dt DataType) string {
	return strings.ToUpper(dt.Type)
}
