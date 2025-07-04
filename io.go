package archive

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// prefetch mengambil data di sekitar ID yang sedang diakses.
func (c *RingBufferCache) prefetch(s *shard, relID int64) {
	if c.options.PrefetchSize <= 0 {
		return
	}
	for i := int64(1); i <= int64(c.options.PrefetchSize); i++ {
		nextID := relID + i
		if nextID > s.size {
			break
		}
		id := s.offset + nextID
		if _, exists := c.prefetchMap.Load(id); exists {
			continue
		}
		c.prefetchMap.Store(id, true)
		go func(fetchID int64) {
			c.Read(fetchID)
			c.prefetchMap.Delete(fetchID) // simple eviction
		}(id)
	}
}

// Write menulis payload ke ID tertentu.
// translate absolute id to relative (1-based)
func (c *RingBufferCache) absToRel(id int64) (int64, error) {
	if id < c.minIDAlloc || id > int64(c.maxIDAlloc) {
		return 0, fmt.Errorf("id out of range: %d (allowed %d..%d)", id, c.minIDAlloc, c.maxIDAlloc)
	}
	return id - c.minIDAlloc + 1, nil
}

func (c *RingBufferCache) Write(id int64, payload []byte, flush bool) error {
	relID, err := c.absToRel(id)
	if err != nil {
		return err
	}

	if len(payload) != c.record {
		return fmt.Errorf("payload size mismatch: got %d want %d", len(payload), c.record)
	}

	shard, _, err := c.findShard(relID)
	if err != nil {
		return err
	}

	m := c.lock(id)
	m.Lock()
	defer m.Unlock()

	offset := (relID - 1) * int64(c.diskRec)

	buf := c.getBufFromPool()
	defer c.returnBufToPool(buf)

	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	copy(buf[4:], payload)

	if shard.mmap != nil {
		copy(shard.mmap[offset:offset+int64(c.diskRec)], buf)
		if flush {
			return unix.Msync(shard.mmap, unix.MS_SYNC)
		}
	} else {
		if _, err := shard.file.WriteAt(buf, offset); err != nil {
			return err
		}
		if flush {
			return shard.file.Sync()
		}
	}
	return nil
}

// Read mengambil payload dari ID tertentu.
func (c *RingBufferCache) Read(id int64) ([]byte, error) {
	relID, err := c.absToRel(id)
	if err != nil {
		return nil, err
	}

	shard, _, err := c.findShard(relID)
	if err != nil {
		return nil, err
	}

	m := c.lock(id)
	m.RLock()
	defer m.RUnlock()

	offset := (relID - 1) * int64(c.diskRec)

	buf := c.getBufFromPool()
	defer c.returnBufToPool(buf)

	if shard.mmap != nil {
		copy(buf, shard.mmap[offset:offset+int64(c.diskRec)])
	} else {
		if _, err := shard.file.ReadAt(buf, offset); err != nil {
			atomic.AddUint64(&c.statMisses, 1)
			return nil, err
		}
	}

	storedCRC := binary.LittleEndian.Uint32(buf[0:4])
	payload := buf[4:]
	if crc32.ChecksumIEEE(payload) != storedCRC {
		atomic.AddUint64(&c.statMisses, 1)
		return nil, fmt.Errorf("corrupted: CRC mismatch")
	}

	atomic.AddUint64(&c.statHits, 1)

	if c.options.PrefetchSize > 0 {
		go c.prefetch(shard, relID)
	}

	out := make([]byte, c.record)
	copy(out, payload)
	return out, nil
}

// BulkWrite menulis beberapa payload berturut-turut.
func (c *RingBufferCache) BulkWrite(startID int64, payloads [][]byte, flush bool) error {
	if startID < 1 || startID+int64(len(payloads))-1 > c.size {
		return fmt.Errorf("id range out of bounds")
	}
	for i, p := range payloads {
		if len(p) != c.record {
			return fmt.Errorf("payload %d must be exactly %d bytes", i, c.record)
		}
	}
	for i, p := range payloads {
		id := startID + int64(i)
		shouldFlush := flush && i == len(payloads)-1
		if err := c.Write(id, p, shouldFlush); err != nil {
			return fmt.Errorf("gagal menulis record %d: %w", id, err)
		}
	}
	return nil
}

// BulkRead membaca beberapa record berturut-turut.
func (c *RingBufferCache) BulkRead(startID int64, count int) ([][]byte, error) {
	if startID < 1 || startID+int64(count)-1 > c.size {
		return nil, fmt.Errorf("id range out of bounds")
	}
	res := make([][]byte, count)
	for i := 0; i < count; i++ {
		id := startID + int64(i)
		p, err := c.Read(id)
		if err != nil {
			return res, fmt.Errorf("gagal membaca record %d: %w", id, err)
		}
		res[i] = p
	}
	return res, nil
}
