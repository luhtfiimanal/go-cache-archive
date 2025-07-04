package archive

import (
	"bytes"
	"testing"
)

func TestWriteHeadAndMeta(t *testing.T) {
	cache, base := newTestCache(t, 10, 16)
	defer cache.Close()

	// initial head should be 0
	if cache.Head() != 0 {
		t.Fatalf("expected head 0 on fresh cache, got %d", cache.Head())
	}

	p := bytes.Repeat([]byte{'x'}, 16)
	id, err := cache.WriteHead(p, true)
	if err != nil {
		t.Fatalf("WriteHead: %v", err)
	}
	if id != 1 || cache.Head() != 1 {
		t.Fatalf("expected head now 1, got %d", cache.Head())
	}

	// close & reopen to ensure meta persisted
	cache.Close()
	opts := DefaultOptions()
	opts.UseMmap = false
	opts.RecordSize = 16
	opts.MinIDAlloc = 1
	opts.MaxIDAlloc = 10
	reopened, err := NewRingBufferCacheWithOptions(base, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if reopened.Head() != 1 {
		t.Fatalf("meta not persisted, head=%d", reopened.Head())
	}
}

func TestWriteHeadWrap(t *testing.T) {
	opts := DefaultOptions()
	opts.MinIDAlloc = 3
	opts.MaxIDAlloc = 5
	cache, _ := newTestCacheWithOpts(t, 5, 8, opts)
	defer cache.Close()

	payload := bytes.Repeat([]byte{'y'}, 8)
	// write 4 times: ids 3,4,5,3(wrap)
	for i := 0; i < 4; i++ {
		if _, err := cache.WriteHead(payload, false); err != nil {
			t.Fatalf("WriteHead #%d: %v", i, err)
		}
	}
	if cache.Head() != 3 {
		t.Fatalf("expected head wrap to 3, got %d", cache.Head())
	}
}
