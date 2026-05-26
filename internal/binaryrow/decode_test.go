package binaryrow_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/paimon/paimon-go/internal/binaryrow"
)

// buildRow builds a synthetic BinaryRow byte slice matching Paimon's wire format.
//
// Layout:
//
//	[4 bytes big-endian: arity]
//	[nullBitsSize bytes: byte0=rowKind, bits 8+ = null flags]
//	[arity*8 bytes: fixed slots]
//	[variable-length heap data]
func buildRow(arity int, rowKind binaryrow.RowKind, nullBits []bool, slots [][]byte, heap []byte) []byte {
	nullBitsSize := ((arity + 63 + 8) / 64) * 8
	fixedSize := nullBitsSize + arity*8
	actual := make([]byte, fixedSize+len(heap))

	// row kind in byte 0
	actual[0] = byte(rowKind)

	// null flags: bit index = pos + 8
	for i, isNull := range nullBits {
		if isNull {
			bitIndex := i + 8
			actual[bitIndex/8] |= 1 << uint(bitIndex%8)
		}
	}

	// fixed slots
	for i, slot := range slots {
		off := nullBitsSize + i*8
		copy(actual[off:off+8], slot)
	}

	// heap
	copy(actual[fixedSize:], heap)

	// prepend arity prefix (big-endian)
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(arity))
	return append(prefix, actual...)
}

func slot8(b [8]byte) []byte { return b[:] }

func int32Slot(v int32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}

func int64Slot(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func float32Slot(v float32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b, math.Float32bits(v))
	return b
}

func float64Slot(v float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	return b
}

// inlineStringSlot encodes a string <= 7 bytes as an inline slot.
func inlineStringSlot(s string) []byte {
	b := make([]byte, 8)
	copy(b, s)
	b[7] = 0x80 | byte(len(s))
	return b
}

// heapStringSlot encodes a heap pointer: (absoluteOffset << 32) | length.
// absoluteOffset is relative to the start of actual[] (after the 4-byte arity prefix).
func heapStringSlot(absoluteOffset, length int) []byte {
	b := make([]byte, 8)
	word := (int64(absoluteOffset) << 32) | int64(length)
	binary.LittleEndian.PutUint64(b, uint64(word))
	return b
}

// --- Tests ---

func TestBinaryRow_Arity(t *testing.T) {
	data := buildRow(3, binaryrow.Insert, []bool{false, false, false},
		[][]byte{int32Slot(1), int64Slot(2), int32Slot(3)}, nil)
	row, err := binaryrow.New(data)
	if err != nil {
		t.Fatal(err)
	}
	if row.Arity() != 3 {
		t.Fatalf("expected arity 3, got %d", row.Arity())
	}
}

func TestBinaryRow_RowKind(t *testing.T) {
	data := buildRow(1, binaryrow.Delete, []bool{false}, [][]byte{int32Slot(0)}, nil)
	row, _ := binaryrow.New(data)
	if row.RowKind() != binaryrow.Delete {
		t.Fatalf("expected Delete, got %v", row.RowKind())
	}
}

func TestBinaryRow_NullField(t *testing.T) {
	data := buildRow(2, binaryrow.Insert, []bool{true, false},
		[][]byte{make([]byte, 8), int32Slot(42)}, nil)
	row, _ := binaryrow.New(data)
	if !row.IsNull(0) {
		t.Fatal("expected field 0 to be null")
	}
	if row.IsNull(1) {
		t.Fatal("expected field 1 to be non-null")
	}
}

func TestBinaryRow_GetInt32(t *testing.T) {
	data := buildRow(1, binaryrow.Insert, []bool{false}, [][]byte{int32Slot(-12345)}, nil)
	row, _ := binaryrow.New(data)
	v, err := row.GetInt32(0)
	if err != nil {
		t.Fatal(err)
	}
	if v != -12345 {
		t.Fatalf("expected -12345, got %d", v)
	}
}

func TestBinaryRow_GetInt64(t *testing.T) {
	data := buildRow(1, binaryrow.Insert, []bool{false}, [][]byte{int64Slot(9876543210)}, nil)
	row, _ := binaryrow.New(data)
	v, err := row.GetInt64(0)
	if err != nil {
		t.Fatal(err)
	}
	if v != 9876543210 {
		t.Fatalf("expected 9876543210, got %d", v)
	}
}

func TestBinaryRow_GetFloat64(t *testing.T) {
	want := 3.14159265358979
	data := buildRow(1, binaryrow.Insert, []bool{false}, [][]byte{float64Slot(want)}, nil)
	row, _ := binaryrow.New(data)
	v, err := row.GetFloat64(0)
	if err != nil {
		t.Fatal(err)
	}
	if v != want {
		t.Fatalf("expected %f, got %f", want, v)
	}
}

func TestBinaryRow_GetString_Inline(t *testing.T) {
	// "hello" = 5 bytes, fits inline
	data := buildRow(1, binaryrow.Insert, []bool{false}, [][]byte{inlineStringSlot("hello")}, nil)
	row, _ := binaryrow.New(data)
	v, err := row.GetString(0)
	if err != nil {
		t.Fatal(err)
	}
	if v != "hello" {
		t.Fatalf("expected %q, got %q", "hello", v)
	}
}

func TestBinaryRow_GetString_Heap(t *testing.T) {
	// "hello world!" = 12 bytes, must go on heap
	s := "hello world!"
	arity := 1
	nullBitsSize := ((arity + 63 + 8) / 64) * 8
	fixedSize := nullBitsSize + arity*8
	// heap starts immediately after fixed region (offset from actual[], not from raw[])
	heapOffset := fixedSize
	slot := heapStringSlot(heapOffset, len(s))
	heap := []byte(s)

	data := buildRow(arity, binaryrow.Insert, []bool{false}, [][]byte{slot}, heap)
	row, _ := binaryrow.New(data)
	v, err := row.GetString(0)
	if err != nil {
		t.Fatal(err)
	}
	if v != s {
		t.Fatalf("expected %q, got %q", s, v)
	}
}

func TestBinaryRow_MultipleFields(t *testing.T) {
	// Row with: int32=100, null, string="go"
	arity := 3
	nullBitsSize := ((arity + 63 + 8) / 64) * 8
	fixedSize := nullBitsSize + arity*8
	heapOffset := fixedSize

	slots := [][]byte{
		int32Slot(100),
		make([]byte, 8),          // null slot
		heapStringSlot(heapOffset, 2), // "go" on heap (>7 bytes? no, 2 bytes — but let's test heap path explicitly)
	}
	// Force heap path: use heapStringSlot even though "go" fits inline.
	// Actually for correctness use inlineStringSlot for short strings:
	slots[2] = inlineStringSlot("go")

	data := buildRow(arity, binaryrow.Insert, []bool{false, true, false}, slots, nil)
	row, _ := binaryrow.New(data)

	v0, _ := row.GetInt32(0)
	if v0 != 100 {
		t.Fatalf("field 0: expected 100, got %d", v0)
	}
	if !row.IsNull(1) {
		t.Fatal("field 1 should be null")
	}
	v2, _ := row.GetString(2)
	if v2 != "go" {
		t.Fatalf("field 2: expected %q, got %q", "go", v2)
	}
}

func TestBinaryRow_GetField_Dispatch(t *testing.T) {
	data := buildRow(1, binaryrow.Insert, []bool{false}, [][]byte{int64Slot(42)}, nil)
	row, _ := binaryrow.New(data)
	v, err := binaryrow.GetField(row, 0, "BIGINT")
	if err != nil {
		t.Fatal(err)
	}
	if v.(int64) != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
}

func TestBinaryRow_GetField_Null(t *testing.T) {
	data := buildRow(1, binaryrow.Insert, []bool{true}, [][]byte{make([]byte, 8)}, nil)
	row, _ := binaryrow.New(data)
	v, err := binaryrow.GetField(row, 0, "INT")
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestBinaryRow_TooShort(t *testing.T) {
	_, err := binaryrow.New([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for too-short data")
	}
}
