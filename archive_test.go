package archive

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// helper to create a cache in a temporary directory with deterministic options
func newTestCache(t *testing.T, slots int64, recordSize int) (*RingBufferCache, string) {
	return newTestCacheWithOpts(t, slots, recordSize, DefaultOptions())
}

func newTestCacheWithOpts(t *testing.T, totalSlots int64, recordSize int, opts CacheOptions) (*RingBufferCache, string) {
	t.Helper()
	dir := t.TempDir()
	base := filepath.Join(dir, "cache.data")
	// configure opts for tests
	opts.UseMmap = false
	opts.ShardCount = 1
	opts.RecordSize = recordSize
	if opts.MinIDAlloc == 0 {
		opts.MinIDAlloc = 1
	}
	if opts.MaxIDAlloc == 0 {
		opts.MaxIDAlloc = int64(totalSlots)
	}
	opts.BufferPoolSize = 10
	opts.PrefetchSize = 0

	c, err := NewRingBufferCacheWithOptions(base, opts)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	return c, base
}

func TestWriteRead(t *testing.T) {
	const (
		size       = 100
		recordSize = 32
	)
	cache, _ := newTestCache(t, size, recordSize)
	defer cache.Close()

	// Write random payloads and read them back
	for id := int64(1); id <= size; id++ {
		payload := make([]byte, recordSize)
		rand.Read(payload)

		if err := cache.Write(id, payload, true); err != nil {
			t.Fatalf("write id %d: %v", id, err)
		}
		got, err := cache.Read(id)
		if err != nil {
			t.Fatalf("read id %d: %v", id, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("payload mismatch at id %d", id)
		}
	}

	st := cache.GetStats()
	if st.Hits != uint64(size) || st.Misses != 0 {
		t.Fatalf("unexpected stats after hits, got %+v", st)
	}
}

func TestBulkWriteRead(t *testing.T) {
	cache, _ := newTestCache(t, 50, 16)
	defer cache.Close()

	payloads := [][]byte{
		[]byte("abcdefghijklmnop"), // 16 bytes
		[]byte("qrstuvwxyzABCDEF"), // 16 bytes
		[]byte("GHIJKLMNOPQRSTUV"), // 16 bytes
	}

	if err := cache.BulkWrite(10, payloads, true); err != nil {
		t.Fatalf("bulk write: %v", err)
	}

	got, err := cache.BulkRead(10, len(payloads))
	if err != nil {
		t.Fatalf("bulk read: %v", err)
	}
	for i := range payloads {
		if !bytes.Equal(got[i], payloads[i]) {
			t.Fatalf("payload mismatch at idx %d", i)
		}
	}
}

func TestCRCError(t *testing.T) {
	cache, base := newTestCache(t, 10, 8)
	defer cache.Close()

	payload := []byte("12345678")
	if err := cache.Write(1, payload, true); err != nil {
		t.Fatalf("write: %v", err)
	}
	// corrupt the first byte of the record on disk (skip CRC bytes to force error)
	f, err := os.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	defer f.Close()
	// offset to payload byte 0 -> 4 bytes CRC + 0
	if _, err := f.WriteAt([]byte{0xFF}, 4); err != nil {
		t.Fatalf("corrupt write: %v", err)
	}

	if _, err := cache.Read(1); err == nil {
		t.Fatalf("expected CRC error, got nil")
	}
	st := cache.GetStats()
	if st.Misses == 0 {
		t.Fatalf("expected miss stat increment, got %+v", st)
	}
}

func TestFlushClosePersistence(t *testing.T) {
	cache, base := newTestCache(t, 5, 12)
	payload := []byte("HelloWorld!!")
	if err := cache.Write(3, payload, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cache.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// reopen
	opts := DefaultOptions()
	opts.UseMmap = false
	opts.ShardCount = 1
	opts.RecordSize = 12
	reopened, err := NewRingBufferCacheWithOptions(base, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.Read(3)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch after reopen")
	}
}

func TestConcurrentAccess(t *testing.T) {
	cache, _ := newTestCache(t, 200, 24)
	defer cache.Close()

	wg := sync.WaitGroup{}

	// writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 200; i++ {
			p := make([]byte, 24)
			rand.Read(p)
			if err := cache.Write(i, p, false); err != nil {
				t.Errorf("write %d: %v", i, err)
			}
		}
	}()

	// reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(1); i <= 200; i++ {
			if _, err := cache.Read(i); err != nil {
				// reads may fail early before written, ignore
				continue
			}
		}
	}()

	wg.Wait()
}
