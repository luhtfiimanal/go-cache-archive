package archive

import (
	"bytes"
	"fmt"
	"testing"
)

// TestTailFollowsHeadAfterWrap ensures that once the buffer is full and wraps
// around, Tail always points to Head+1 modulo the ID range.
func TestTailFollowsHeadAfterWrap(t *testing.T) {
	opts := DefaultOptions()
	opts.MinIDAlloc = 10
	opts.MaxIDAlloc = 15 // small range to exercise wrap quickly
	opts.RecordSize = 8
	cache, _ := newTestCacheWithOpts(t, 5, 8, opts)
	defer cache.Close()

	payload := bytes.Repeat([]byte{'z'}, 8)
	totalWrites := 15 // > 2 full cycles
	for i := 0; i < totalWrites; i++ {
		if _, err := cache.WriteHead(payload, false); err != nil {
			t.Fatalf("WriteHead #%d: %v", i, err)
		}
		head := cache.Head()
		tail := cache.Tail()
		fmt.Printf("tail=%d, head=%d\n", tail, head)
		// after the first full range written, tail should track head+1 modulo
		if i >= int(opts.MaxIDAlloc-opts.MinIDAlloc+1) {
			expectedTail := head + 1
			if expectedTail > opts.MaxIDAlloc {
				expectedTail = opts.MinIDAlloc
			}
			if tail != expectedTail {
				t.Fatalf("after wrap, expected tail %d, got %d (head=%d)", expectedTail, tail, head)
			}
		}
	}
}
