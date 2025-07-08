package bench_test

import (
    archive "github.com/luhtfiimanal/go-cache-archive"
    "database/sql"
    "math/rand"
    "os"
    "path/filepath"
    "testing"
    _ "modernc.org/sqlite"
)

const readBenchRecordSize = recordSize // reuse 48 bytes from sqlite_compare_test.go

// prepareTestStores membuat cache dan sqlite berisi 'total' record.
func prepareTestStores(b *testing.B, total int64) (*archive.RingBufferCache, *sql.DB) {
    // --- cache
    opts := archive.DefaultOptions()
    opts.UseMmap = false
    opts.ShardCount = 1
    opts.RecordSize = readBenchRecordSize
    opts.MinIDAlloc = 1
    opts.MaxIDAlloc = total
    tmpDir, _ := os.MkdirTemp("", "cachepre")
    cacheBase := filepath.Join(tmpDir, "cache.data")
    cache, err := archive.NewRingBufferCacheWithOptions(cacheBase, opts)
    if err != nil {
        b.Fatalf("create cache: %v", err)
    }

    // --- sqlite
    db, err := sql.Open("sqlite", ":memory:")
    if err != nil {
        b.Fatalf("open sqlite: %v", err)
    }
    _, _ = db.Exec(`CREATE TABLE tbl (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT, d INTEGER);`)

    // populate
    for i := int64(1); i <= total; i++ {
        r := testRow{
            ID: i,
            A:  randomASCII(asciiLen),
            B:  rand.Int63(),
            C:  randomASCII(asciiLen),
            D:  rand.Int63(),
        }
        if err := cache.Write(i, encodeRow(r), false); err != nil {
            b.Fatalf("cache write: %v", err)
        }
        if _, err := db.Exec(`INSERT INTO tbl (id,a,b,c,d) VALUES (?,?,?,?,?)`, r.ID, r.A, r.B, r.C, r.D); err != nil {
            b.Fatalf("sqlite insert: %v", err)
        }
    }
    cache.Flush()
    return cache, db
}

// BenchmarkReadSeq10 membaca 10 id berurutan per iterasi.
func BenchmarkReadSeq10(b *testing.B) {
    total := int64(b.N*10 + 10)
    cache, db := prepareTestStores(b, total)
    defer cache.Close()
    defer db.Close()

    b.Run("ringbuffer", func(bb *testing.B) {
        for i := 0; i < bb.N; i++ {
            id := int64((i*10)%int(total-9) + 1)
            if _, err := cache.BulkRead(id, 10); err != nil {
                bb.Fatalf("read: %v", err)
            }
        }
    })

    b.Run("sqlite", func(bb *testing.B) {
        for i := 0; i < bb.N; i++ {
            id := int64((i*10)%int(total-9) + 1)
            for j := int64(0); j < 10; j++ {
                row := db.QueryRow(`SELECT id FROM tbl WHERE id=?`, id+j)
                var tmp int64
                if err := row.Scan(&tmp); err != nil {
                    bb.Fatalf("read sqlite: %v", err)
                }
            }
        }
    })
}

// BenchmarkReadRandom membaca id acak per iterasi.
func BenchmarkReadRandom(b *testing.B) {
    total := int64(b.N) * 2
    if total < 1000 {
        total = 1000
    }
    cache, db := prepareTestStores(b, total)
    defer cache.Close()
    defer db.Close()

    indexRand := rand.New(rand.NewSource(42))

    b.Run("ringbuffer", func(bb *testing.B) {
        for i := 0; i < bb.N; i++ {
            id := int64(indexRand.Int63n(total) + 1)
            if _, err := cache.Read(id); err != nil {
                bb.Fatalf("cache read: %v", err)
            }
        }
    })

    b.Run("sqlite", func(bb *testing.B) {
        for i := 0; i < bb.N; i++ {
            id := int64(indexRand.Int63n(total) + 1)
            row := db.QueryRow(`SELECT id FROM tbl WHERE id=?`, id)
            var tmp int64
            if err := row.Scan(&tmp); err != nil {
                bb.Fatalf("sqlite read: %v", err)
            }
        }
    })
}
