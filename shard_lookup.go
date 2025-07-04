package archive

import "fmt"

// findShard menentukan shard mana yang berisi ID tertentu.
//
// Mengembalikan pointer ke shard, ID relatif di dalam shard (1-based), atau error
// bila ID di luar rentang 1..c.size.
func (c *RingBufferCache) findShard(id int64) (*shard, int64, error) {
	if id < 1 || id > c.size {
		return nil, 0, fmt.Errorf("id out of range: %d (max: %d)", id, c.size)
	}

	// Cari shard yang intervalnya mencakup id.
	for _, s := range c.shards {
		if id > s.offset && id <= s.offset+s.size {
			return s, id - s.offset, nil
		}
	}
	// Seharusnya tidak terjadi.
	return nil, 0, fmt.Errorf("id tidak ditemukan dalam shard manapun: %d", id)
}
