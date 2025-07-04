package archive

import "sync/atomic"

// Stats menyimpan statistik hit/miss cache.
// HitRatio dalam persentase (0-100).
type Stats struct {
	Hits     uint64
	Misses   uint64
	HitRatio float64
}

// GetStats mengambil snapshot statistik tanpa lock berat.
func (c *RingBufferCache) GetStats() Stats {
	hits := atomic.LoadUint64(&c.statHits)
	misses := atomic.LoadUint64(&c.statMisses)
	total := hits + misses
	ratio := 0.0
	if total > 0 {
		ratio = float64(hits) / float64(total) * 100.0
	}
	return Stats{Hits: hits, Misses: misses, HitRatio: ratio}
}

// ResetStats mengatur ulang penghitung hit/miss.
func (c *RingBufferCache) ResetStats() {
	atomic.StoreUint64(&c.statHits, 0)
	atomic.StoreUint64(&c.statMisses, 0)
}

// Size mengembalikan jumlah slot ID total.
func (c *RingBufferCache) Size() int64 { return c.size }

// RecordSize mengembalikan ukuran setiap record (payload).
func (c *RingBufferCache) RecordSize() int { return c.record }

// ShardCount mengembalikan jumlah shard di disk.
func (c *RingBufferCache) ShardCount() int { return len(c.shards) }
