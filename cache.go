package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// RingBufferCache menyediakan implementasi ring buffer berbasis file dengan
// dukungan sharding, memory-mapping, buffer-pool, dan prefetching.
//
// Semua operasi aman untuk goroutine.
type RingBufferCache struct {
	shards  []*shard       // Daftar file shards (selalu >=1)
	size    int64          // Jumlah slot ID total (basis 1)
	record  int            // Ukuran payload publik
	diskRec int            // Ukuran sebenarnya di disk = record + 4 (CRC)
	locks   []sync.RWMutex // Sharded locks
	nLock   int            // Total mutex shards
	options CacheOptions
	bufPool *sync.Pool // Pool untuk reuse buffer

	prefetchMap *sync.Map // Map[id]bool untuk menandai data yang diprefetch

	statMisses uint64 // statistik miss (access atau CRC corrupt)
	statHits   uint64 // statistik hit
}

// NewRingBufferCache membuat cache dengan opsi default (lihat DefaultOptions).
func NewRingBufferCache(path string, size int64, recordSize int) (*RingBufferCache, error) {
	return NewRingBufferCacheWithOptions(path, size, recordSize, DefaultOptions())
}

// NewRingBufferCacheWithOptions membuat cache dengan opsi kustom.
func NewRingBufferCacheWithOptions(basePath string, size int64, recordSize int, opts CacheOptions) (*RingBufferCache, error) {
	if size <= 0 || recordSize <= 0 {
		return nil, fmt.Errorf("size dan recordSize harus positif")
	}

	diskRec := recordSize + 4 // +4 byte CRC32

	// Tentukan nilai default opsi
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}

	// Hitung ukuran shard default bila multi-shard
	shardSize := size
	if opts.ShardCount > 1 {
		shardSize = size / int64(opts.ShardCount)
		if size%int64(opts.ShardCount) != 0 {
			shardSize++ // round-up
		}
	}

	// Pastikan direktori ada
	if err := os.MkdirAll(filepath.Dir(basePath), 0o755); err != nil {
		return nil, fmt.Errorf("gagal membuat direktori: %w", err)
	}

	// Inisialisasi shards
	shards := make([]*shard, opts.ShardCount)
	var offset int64

	for i := 0; i < opts.ShardCount; i++ {
		currentShardSize := shardSize
		if i == opts.ShardCount-1 {
			currentShardSize = size - offset // shard terakhir mungkin lebih kecil
		}

		shardPath := basePath
		if opts.ShardCount > 1 {
			shardPath = fmt.Sprintf("%s.%d", basePath, i)
		}

		f, err := os.OpenFile(shardPath, os.O_RDWR|os.O_CREATE, 0o666)
		if err != nil {
			// cleanup opened shards
			for j := 0; j < i; j++ {
				shards[j].file.Close()
				if shards[j].mmap != nil {
					unix.Munmap(shards[j].mmap)
				}
			}
			return nil, fmt.Errorf("gagal membuka shard %d: %w", i, err)
		}

		diskSize := currentShardSize * int64(diskRec)
		if err := f.Truncate(diskSize); err != nil {
			f.Close()
			for j := 0; j < i; j++ {
				shards[j].file.Close()
				if shards[j].mmap != nil {
					unix.Munmap(shards[j].mmap)
				}
			}
			return nil, fmt.Errorf("gagal mengalokasikan shard %d: %w", i, err)
		}

		s := &shard{
			file:     f,
			filePath: shardPath,
			size:     currentShardSize,
			offset:   offset,
		}

		if opts.UseMmap {
			mmap, err := unix.Mmap(int(f.Fd()), 0, int(diskSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
			if err != nil {
				f.Close()
				for j := 0; j < i; j++ {
					shards[j].file.Close()
					if shards[j].mmap != nil {
						unix.Munmap(shards[j].mmap)
					}
				}
				return nil, fmt.Errorf("gagal mmap shard %d: %w", i, err)
			}
			s.mmap = mmap
		}

		shards[i] = s
		offset += currentShardSize
	}

	// Buffer pool
	var pool *sync.Pool
	if opts.BufferPoolSize > 0 {
		pool = &sync.Pool{New: func() any { return make([]byte, diskRec) }}
	}

	nLocks := 256
	locks := make([]sync.RWMutex, nLocks)

	return &RingBufferCache{
		shards:      shards,
		size:        size,
		record:      recordSize,
		diskRec:     diskRec,
		locks:       locks,
		nLock:       nLocks,
		options:     opts,
		bufPool:     pool,
		prefetchMap: &sync.Map{},
	}, nil
}
