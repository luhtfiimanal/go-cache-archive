package archive

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
)

// meta file layout: 16 bytes (little-endian)
// 0..7  : uint64 head (last written ID)
// 8..15 : uint64 tail (oldest valid ID, currently informational)

func metaPath(base string) string { return base + ".meta" }

func saveMeta(path string, head, tail uint64) error {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], head)
	binary.LittleEndian.PutUint64(buf[8:16], tail)
	return os.WriteFile(path, buf, 0o666)
}

func loadMeta(path string) (head, tail uint64, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	if len(data) < 16 {
		return 0, 0, fmt.Errorf("meta file too small")
	}
	head = binary.LittleEndian.Uint64(data[0:8])
	tail = binary.LittleEndian.Uint64(data[8:16])
	return head, tail, nil
}

// Head returns current head (last written ID).
func (c *RingBufferCache) Head() int64 {
	return int64(atomic.LoadUint64(&c.head))
}

// Tail returns current tail (currently static unless eviction implemented).
func (c *RingBufferCache) Tail() int64 {
	return int64(atomic.LoadUint64(&c.tail))
}

// WriteHead writes payload to the next ID (head+1, wrapping) and returns the new ID.
func (c *RingBufferCache) WriteHead(payload []byte, flush bool) (int64, error) {
	// only one writer assumed; but keep writerActive flag for readers if needed later
	atomic.StoreUint32(&c.writerActive, 1)
	defer atomic.StoreUint32(&c.writerActive, 0)

	nextID := atomic.AddUint64(&c.head, 1)
	max := c.maxIDAlloc
	if max == 0 {
		max = uint64(c.size)
	}
	min := uint64(c.minIDAlloc)

	// wrap detection
	if nextID > max {
		// reset to min
		atomic.StoreUint64(&c.head, min)
		nextID = min
	}

	// handle tail tracking
	oldTail := atomic.LoadUint64(&c.tail)
	if nextID == min {
		// we just wrapped
		if oldTail == min {
			// first time buffer becomes full, move tail to min+1
			atomic.StoreUint64(&c.tail, min+1)
		} else {
			// subsequent wraps, tail already tracking, move to head+1
			atomic.StoreUint64(&c.tail, min+1)
		}
	} else if oldTail != min {
		// buffer already full, tail always head+1 modulo
		tail := nextID + 1
		if tail > max {
			tail = min
		}
		atomic.StoreUint64(&c.tail, tail)
	}

	if err := c.Write(int64(nextID), payload, flush); err != nil {
		return 0, err
	}

	// persist meta if flush requested
	if flush {
		if err := saveMeta(c.metaPath, atomic.LoadUint64(&c.head), atomic.LoadUint64(&c.tail)); err != nil {
			return int64(nextID), fmt.Errorf("save meta: %w", err)
		}
	}
	return int64(nextID), nil
}
