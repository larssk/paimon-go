package deletionvector_test

import (
	"encoding/binary"
	"testing"

	"github.com/RoaringBitmap/roaring"

	"github.com/apache/paimon/paimon-go/internal/deletionvector"
)

// buildRecord serialises a roaring bitmap into a valid DV record byte slice.
func buildRecord(bm *roaring.Bitmap) []byte {
	roaringBytes, err := bm.ToBytes()
	if err != nil {
		panic(err)
	}
	totalLength := int32(4 + len(roaringBytes)) // magic(4) + roaring

	buf := make([]byte, 4+4+len(roaringBytes)+4) // length + magic + roaring + crc
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(buf[4:8], uint32(1581511376)) // magicV1
	copy(buf[8:], roaringBytes)
	// CRC left as zero — Decode skips it
	return buf
}

func TestDecode_happyPath(t *testing.T) {
	bm := roaring.BitmapOf(0, 2, 5, 100)
	data := buildRecord(bm)

	got, err := deletionvector.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []uint32{0, 2, 5, 100}
	gotArr := got.ToArray()
	if len(gotArr) != len(want) {
		t.Fatalf("bitmap mismatch: got %v, want %v", gotArr, want)
	}
	for i, v := range want {
		if gotArr[i] != v {
			t.Errorf("pos %d: got %d, want %d", i, gotArr[i], v)
		}
	}
}

func TestDecode_emptyBitmap(t *testing.T) {
	bm := roaring.New()
	data := buildRecord(bm)

	got, err := deletionvector.Decode(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.GetCardinality() != 0 {
		t.Fatalf("expected empty bitmap, got %v", got.ToArray())
	}
}

func TestDecode_wrongMagic(t *testing.T) {
	bm := roaring.BitmapOf(1, 3)
	roaringBytes, _ := bm.ToBytes()
	totalLength := int32(4 + len(roaringBytes))

	buf := make([]byte, 4+4+len(roaringBytes)+4)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(buf[4:8], 0xDEADBEEF) // wrong magic
	copy(buf[8:], roaringBytes)

	_, err := deletionvector.Decode(buf)
	if err == nil {
		t.Fatal("expected error for wrong magic, got nil")
	}
	var unsupported *deletionvector.ErrUnsupportedMagic
	if _, ok := err.(*deletionvector.ErrUnsupportedMagic); !ok {
		t.Fatalf("expected ErrUnsupportedMagic, got %T: %v", unsupported, err)
	}
}

func TestDecode_tooShort(t *testing.T) {
	_, err := deletionvector.Decode([]byte{0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for truncated data, got nil")
	}
}

func TestDecode_truncatedBitmap(t *testing.T) {
	bm := roaring.BitmapOf(1, 2, 3)
	data := buildRecord(bm)
	// chop off the last 10 bytes to simulate truncation
	data = data[:len(data)-10]

	_, err := deletionvector.Decode(data)
	if err == nil {
		t.Fatal("expected error for truncated bitmap, got nil")
	}
}
