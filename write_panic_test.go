package archive_test

import (
	"os"
	"path/filepath"
	"testing"

	archive "github.com/luhtfiimanal/go-cache-archive"
)

func TestWritePanicsWhenOffsetExceedsShard(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "cache.dat")

	// Buat opsi dengan range 1..10 dan 2 shard → tiap shard hanya 5 record.
	opts := archive.CacheOptions{
		MinIDAlloc: 1,
		MaxIDAlloc: 10,
		ShardCount: 2,
		RecordSize: 2048,
		UseMmap:    true,
	}

	cache, err := archive.NewRingBufferCacheWithOptions(path, opts)
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()
	defer os.RemoveAll(tmp)

	// Tulis 6 record berturut-turut. Penulisan ID ke-6 menyeberang shard
	// dan saat ini menyebabkan slice bounds panic.
	for i := int64(1); i <= 6; i++ {
		payload := make([]byte, opts.RecordSize)
		if _, err := cache.WriteHead(payload, false); err != nil {
			t.Fatalf("write head failed at id %d: %v", i, err)
		}
	}
}
