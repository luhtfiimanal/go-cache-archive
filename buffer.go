package archive

import "sync"

// getBufFromPool mengambil buffer dari pool atau membuat baru jika tidak tersedia.
// Ukuran buffer selalu c.diskRec byte (CRC + payload).
func (c *RingBufferCache) getBufFromPool() []byte {
	if c.bufPool != nil {
		return c.bufPool.Get().([]byte)
	}
	return make([]byte, c.diskRec)
}

// returnBufToPool mengembalikan buffer ke pool untuk digunakan kembali.
// Hanya buffer dengan ukuran tepat yang akan dimasukkan kembali ke pool untuk
// menghindari fragmentasi.
func (c *RingBufferCache) returnBufToPool(buf []byte) {
	if c.bufPool != nil && len(buf) == c.diskRec {
		c.bufPool.Put(buf)
	}
}

// lock mengembalikan RWMutex yang di-shard berdasarkan id sehingga kita tidak
// membuat satu mutex per record.
func (c *RingBufferCache) lock(id int64) *sync.RWMutex {
	return &c.locks[id%int64(c.nLock)]
}
