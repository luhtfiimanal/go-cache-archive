package archive

// CacheOptions menyediakan opsi konfigurasi untuk RingBufferCache.
//
//   - UseMmap:     aktifkan memory-mapping untuk akses data lebih cepat
//   - ShardCount:  jumlah shard untuk memecah file besar (0 = single file)
//   - BufferPoolSize: ukuran pool buffer untuk mengurangi alokasi (0 = nonaktif)
//   - PrefetchSize:   jumlah record diprefetch saat membaca (0 = nonaktif)
//
// Semua bidang bersifat opsi; nilai 0 artinya gunakan default.
// Lihat DefaultOptions() untuk nilai bawaan.
type CacheOptions struct {
	// Ring ID allocation range
	MinIDAlloc     int64 // ID pertama yang akan digunakan (default 1)
	MaxIDAlloc     int64 // Batas maksimal ID (0 = sama dengan size)
	UseMmap        bool  // Gunakan memory-mapping untuk performa lebih baik
	ShardCount     int   // Jumlah shard (0 = single file)
	RecordSize     int   // Ukuran payload setiap record (byte), wajib >0
	BufferPoolSize int   // Ukuran pool buffer (0 = disable)
	PrefetchSize   int   // Prefetch N records ke depan (0 = disable)
}

// DefaultOptions mengembalikan konfigurasi default yang digunakan NewRingBufferCache.
func DefaultOptions() CacheOptions {
	return CacheOptions{
		UseMmap:        true,
		MinIDAlloc:     1,
		MaxIDAlloc:     1000000, // default 1M slots
		ShardCount:     4,
		RecordSize:     32,
		BufferPoolSize: 1000,
		PrefetchSize:   4,
	}
}
