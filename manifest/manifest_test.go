package manifest

import (
	"encoding/binary"
	"testing"
)

func intRowBytes(v int64) []byte {
	data := make([]byte, 20)
	binary.BigEndian.PutUint32(data, 1)
	binary.LittleEndian.PutUint64(data[12:], uint64(v))
	return data
}

func TestDataFileMeta_DecodesKeyBoundsSeparatelyFromStats(t *testing.T) {
	fm, err := dataFileMetaFromMap(map[string]interface{}{
		"_MIN_KEY": intRowBytes(3),
		"_MAX_KEY": intRowBytes(5),
		"_KEY_STATS": map[string]interface{}{
			"_MIN_VALUES": intRowBytes(1),
			"_MAX_VALUES": intRowBytes(9),
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fm.MinKey == nil || fm.MaxKey == nil {
		t.Fatal("explicit key bounds were not decoded")
	}
	min, _ := fm.KeyBounds().MinValues.GetInt64(0)
	max, _ := fm.KeyBounds().MaxValues.GetInt64(0)
	statMin, _ := fm.KeyStats.MinValues.GetInt64(0)
	statMax, _ := fm.KeyStats.MaxValues.GetInt64(0)
	if min != 3 || max != 5 || statMin != 1 || statMax != 9 {
		t.Fatalf("bounds/stats mixed up: bounds=[%d,%d] stats=[%d,%d]", min, max, statMin, statMax)
	}
	fm.MinKey, fm.MaxKey = nil, nil
	if fm.KeyBounds().MinValues != fm.KeyStats.MinValues || fm.KeyBounds().MaxValues != fm.KeyStats.MaxValues {
		t.Fatal("missing bounds should fall back to key-column envelope")
	}
}
