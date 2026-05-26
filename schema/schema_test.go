package schema_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/paimon/paimon-go/schema"
)

func TestDataType_SimpleString_Nullable(t *testing.T) {
	var dt schema.DataType
	if err := json.Unmarshal([]byte(`"INT"`), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "INT" {
		t.Fatalf("expected INT, got %q", dt.Type)
	}
	if !dt.Nullable {
		t.Fatal("expected nullable")
	}
}

func TestDataType_SimpleString_NotNull(t *testing.T) {
	var dt schema.DataType
	if err := json.Unmarshal([]byte(`"BIGINT NOT NULL"`), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "BIGINT" {
		t.Fatalf("expected BIGINT, got %q", dt.Type)
	}
	if dt.Nullable {
		t.Fatal("expected not nullable")
	}
}

func TestDataType_VarcharWithLength(t *testing.T) {
	var dt schema.DataType
	if err := json.Unmarshal([]byte(`"VARCHAR(255) NOT NULL"`), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "VARCHAR" {
		t.Fatalf("expected VARCHAR, got %q", dt.Type)
	}
	if dt.Length != 255 {
		t.Fatalf("expected length 255, got %d", dt.Length)
	}
	if dt.Nullable {
		t.Fatal("expected not nullable")
	}
}

func TestDataType_Decimal(t *testing.T) {
	var dt schema.DataType
	if err := json.Unmarshal([]byte(`"DECIMAL(10, 2)"`), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "DECIMAL" {
		t.Fatalf("expected DECIMAL, got %q", dt.Type)
	}
	if dt.Precision != 10 {
		t.Fatalf("expected precision 10, got %d", dt.Precision)
	}
	if dt.Scale != 2 {
		t.Fatalf("expected scale 2, got %d", dt.Scale)
	}
}

func TestDataType_TimestampPrecision(t *testing.T) {
	var dt schema.DataType
	if err := json.Unmarshal([]byte(`"TIMESTAMP(6)"`), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "TIMESTAMP" {
		t.Fatalf("expected TIMESTAMP, got %q", dt.Type)
	}
	if dt.Precision != 6 {
		t.Fatalf("expected precision 6, got %d", dt.Precision)
	}
}

func TestDataType_ArrayObject(t *testing.T) {
	var dt schema.DataType
	raw := `{"type":"ARRAY NOT NULL","element":"BIGINT NOT NULL"}`
	if err := json.Unmarshal([]byte(raw), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "ARRAY" {
		t.Fatalf("expected ARRAY, got %q", dt.Type)
	}
	if dt.Nullable {
		t.Fatal("expected not nullable")
	}
	if dt.ElementType == nil {
		t.Fatal("expected ElementType to be set")
	}
	if dt.ElementType.Type != "BIGINT" {
		t.Fatalf("expected element BIGINT, got %q", dt.ElementType.Type)
	}
}

func TestDataType_MapObject(t *testing.T) {
	var dt schema.DataType
	raw := `{"type":"MAP","key":"STRING NOT NULL","value":"INT"}`
	if err := json.Unmarshal([]byte(raw), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "MAP" {
		t.Fatalf("expected MAP, got %q", dt.Type)
	}
	if dt.KeyType == nil || dt.KeyType.Type != "STRING" {
		t.Fatalf("expected key STRING, got %v", dt.KeyType)
	}
	if dt.ValueType == nil || dt.ValueType.Type != "INT" {
		t.Fatalf("expected value INT, got %v", dt.ValueType)
	}
}

func TestDataType_RowObject(t *testing.T) {
	var dt schema.DataType
	raw := `{"type":"ROW","fields":[{"id":0,"name":"x","type":"INT"},{"id":1,"name":"y","type":"STRING"}]}`
	if err := json.Unmarshal([]byte(raw), &dt); err != nil {
		t.Fatal(err)
	}
	if dt.Type != "ROW" {
		t.Fatalf("expected ROW, got %q", dt.Type)
	}
	if len(dt.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(dt.Fields))
	}
	if dt.Fields[0].Name != "x" || dt.Fields[1].Name != "y" {
		t.Fatalf("unexpected field names: %v", dt.Fields)
	}
}

func TestDataField_UnmarshalFull(t *testing.T) {
	var f schema.DataField
	raw := `{"id":3,"name":"event_time","type":"TIMESTAMP(6) NOT NULL"}`
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatal(err)
	}
	if f.ID != 3 || f.Name != "event_time" {
		t.Fatalf("unexpected field: %+v", f)
	}
	if f.Type.Type != "TIMESTAMP" || f.Type.Nullable {
		t.Fatalf("unexpected type: %+v", f.Type)
	}
	if f.Type.Precision != 6 {
		t.Fatalf("expected precision 6, got %d", f.Type.Precision)
	}
}
