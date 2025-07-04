package archive

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

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

	// ring buffer meta
	head         uint64 // last written id
	tail         uint64 // oldest valid id (future use)
	minIDAlloc   int64
	maxIDAlloc   uint64
	metaPath     string
	writerActive uint32

	prefetchMap *sync.Map // Map[id]bool untuk menandai data yang diprefetch

	statMisses uint64 // statistik miss (access atau CRC corrupt)
	statHits   uint64 // statistik hit
}

// NewRingBufferCache membuat cache dengan opsi default (lihat DefaultOptions).
func NewRingBufferCache(path string, opts CacheOptions) (*RingBufferCache, error) {
	return NewRingBufferCacheWithOptions(path, opts)
}

// NewRingBufferCacheWithOptions creates a new RingBufferCache with the specified options.
// Parameters:
//   - basePath: The base file path for the cache shards.
//   - opts: Configuration options for the cache.
//
// Returns:
//   - A pointer to the created RingBufferCache.
//   - An error if initialization fails, including directory creation, file opening, or memory mapping.
func NewRingBufferCacheWithOptions(basePath string, opts CacheOptions) (*RingBufferCache, error) {
	if opts.RecordSize <= 0 {
		return nil, fmt.Errorf("RecordSize harus positif")
	}
	if opts.MaxIDAlloc <= opts.MinIDAlloc {
		return nil, fmt.Errorf("MaxIDAlloc harus > MinIDAlloc")
	}

	size := int64(opts.MaxIDAlloc - opts.MinIDAlloc + 1)
	recordSize := opts.RecordSize

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

	// verifikasi konfigurasi persist
	configPath := basePath + ".cfg"
	if err := verifyOrWriteConfig(configPath, opts); err != nil {
		log.Printf("[archive] configuration mismatch: %v", err)
		panic(err)
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

	cache := &RingBufferCache{
		shards:      shards,
		size:        size,
		record:      recordSize,
		diskRec:     diskRec,
		locks:       locks,
		nLock:       nLocks,
		options:     opts,
		minIDAlloc:  opts.MinIDAlloc,
		maxIDAlloc:  uint64(opts.MaxIDAlloc),
		bufPool:     pool,
		prefetchMap: &sync.Map{},
		metaPath:    metaPath(basePath),
	}

	// load meta if exists, otherwise set initial head/tail
	if h, t, err := loadMeta(cache.metaPath); err == nil {
		atomic.StoreUint64(&cache.head, h)
		atomic.StoreUint64(&cache.tail, t)
	} else {
		// fresh cache
		start := uint64(cache.minIDAlloc)
		if start == 0 {
			start = 1
		}
		atomic.StoreUint64(&cache.head, start-1)
		atomic.StoreUint64(&cache.tail, start)
	}

	return cache, nil
}
