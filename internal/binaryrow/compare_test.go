package binaryrow

import (
	"encoding/binary"
	"testing"

	"github.com/apache/paimon/paimon-go/schema"
)

// makeIntRow builds a BinaryRow with a single INT32 field.
func makeIntRow(t *testing.T, val int32, isNull bool) *BinaryRow {
	t.Helper()
	arity := 1
	nullBitsSize := NullBitsSize(arity)
	slotSize := 8
	data := make([]byte, 4+nullBitsSize+slotSize)
	binary.BigEndian.PutUint32(data[:4], uint32(arity))
	actual := data[4:]
	if isNull {
		// Set null bit for field 0 (bit index = 0 + 8 = 8, byte 1, bit 0)
		actual[1] |= 1
	} else {
		binary.LittleEndian.PutUint32(actual[nullBitsSize:nullBitsSize+4], uint32(val))
	}
	r, err := New(data)
	if err != nil {
		t.Fatalf("makeIntRow: %v", err)
	}
	return r
}

// makeStringRow builds a BinaryRow with a single inline STRING field.
func makeStringRow(t *testing.T, val string) *BinaryRow {
	t.Helper()
	arity := 1
	nullBitsSize := NullBitsSize(arity)
	slotSize := 8
	data := make([]byte, 4+nullBitsSize+slotSize)
	binary.BigEndian.PutUint32(data[:4], uint32(arity))
	actual := data[4:]
	// Inline string: byte 7 of slot = 0x80 | len; bytes 0..len-1 = data
	if len(val) > 7 {
		t.Fatalf("makeStringRow: value too long for inline encoding (%d chars)", len(val))
	}
	slot := actual[nullBitsSize : nullBitsSize+8]
	copy(slot, val)
	slot[7] = 0x80 | byte(len(val))
	r, err := New(data)
	if err != nil {
		t.Fatalf("makeStringRow: %v", err)
	}
	return r
}

var intField = []schema.DataField{
	{Name: "id", Type: schema.DataType{Type: "INT"}},
}
var strField = []schema.DataField{
	{Name: "name", Type: schema.DataType{Type: "STRING"}},
}

func TestCompare_BothNull(t *testing.T) {
	a := makeIntRow(t, 0, true)
	b := makeIntRow(t, 0, true)
	if got := Compare(a, b, intField); got != 0 {
		t.Errorf("both null: want 0, got %d", got)
	}
}

func TestCompare_NullLessThanNonNull(t *testing.T) {
	a := makeIntRow(t, 0, true)
	b := makeIntRow(t, 1, false)
	if got := Compare(a, b, intField); got >= 0 {
		t.Errorf("null < non-null: want <0, got %d", got)
	}
	if got := Compare(b, a, intField); got <= 0 {
		t.Errorf("non-null > null: want >0, got %d", got)
	}
}

func TestCompare_Equal(t *testing.T) {
	a := makeIntRow(t, 42, false)
	b := makeIntRow(t, 42, false)
	if got := Compare(a, b, intField); got != 0 {
		t.Errorf("equal ints: want 0, got %d", got)
	}
}

func TestCompare_LessThan(t *testing.T) {
	a := makeIntRow(t, 1, false)
	b := makeIntRow(t, 2, false)
	if got := Compare(a, b, intField); got >= 0 {
		t.Errorf("1 < 2: want <0, got %d", got)
	}
}

func TestCompare_GreaterThan(t *testing.T) {
	a := makeIntRow(t, 99, false)
	b := makeIntRow(t, 1, false)
	if got := Compare(a, b, intField); got <= 0 {
		t.Errorf("99 > 1: want >0, got %d", got)
	}
}

func TestCompare_String(t *testing.T) {
	a := makeStringRow(t, "abc")
	b := makeStringRow(t, "abd")
	if got := Compare(a, b, strField); got >= 0 {
		t.Errorf("abc < abd: want <0, got %d", got)
	}
	if got := Compare(b, a, strField); got <= 0 {
		t.Errorf("abd > abc: want >0, got %d", got)
	}
	c := makeStringRow(t, "abc")
	if got := Compare(a, c, strField); got != 0 {
		t.Errorf("abc == abc: want 0, got %d", got)
	}
}
