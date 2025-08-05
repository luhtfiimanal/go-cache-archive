package archive

import (
    "math/rand"
    "runtime"
    "testing"
    "time"
)

// TestPrefetchGoroutineLeak reproduces the goroutine explosion caused by the
// current implementation of prefetch which spawns an unbounded number of
// goroutines. The test purposely expects the number of goroutines to grow well
// beyond a safe threshold. After the prefetch implementation is fixed this test
// should be updated or removed.
func TestPrefetchGoroutineLeak(t *testing.T) {
    const (
        slots       = 1000
        recSize      = 64
        prefetchSize = 128
        iterations   = 3000
        leakThresh   = 1500 // expect >1500 goroutines when leak present
    )

    opts := DefaultOptions()
    opts.RecordSize = recSize
    opts.ShardCount = 1
    opts.UseMmap = false
    opts.MinIDAlloc = 1
    opts.MaxIDAlloc = slots
    opts.PrefetchSize = prefetchSize

    cache, _ := newTestCacheWithOpts(t, slots, recSize, opts)
    defer cache.Close()

    // write initial data
    payload := make([]byte, recSize)
    for i := int64(1); i <= slots; i++ {
        rand.Read(payload)
        if err := cache.Write(i, payload, false); err != nil {
            t.Fatalf("write %d failed: %v", i, err)
        }
    }

    // read repeatedly to trigger prefetch goroutines
    for i := 0; i < iterations; i++ {
        id := int64((i % slots) + 1)
        if _, err := cache.Read(id); err != nil {
            t.Fatalf("read %d failed: %v", id, err)
        }
    }

    // give some time for goroutines to spawn
    time.Sleep(200 * time.Millisecond)

    g := runtime.NumGoroutine()
    t.Logf("goroutines after %d reads with prefetch=%d: %d", iterations, prefetchSize, g)

    if g > leakThresh {
        t.Fatalf("goroutine leak detected: got %d which exceeds threshold %d", g, leakThresh)
    }
}
