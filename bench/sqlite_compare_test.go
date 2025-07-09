package bench_test

import (
    archive "github.com/luhtfiimanal/go-cache-archive"
    "bytes"
    "context"
    "database/sql"
    "encoding/binary"
    "math/rand"
    "os"
    "path/filepath"
    "testing"
    "time"

    _ "modernc.org/sqlite"
)

type testRow struct {
    ID  int64
    A   string // ascii, fixed 16 bytes
    B   int64
    C   string // ascii, fixed 16 bytes
    D   int64
}

const (
    asciiLen   = 16
    int64Bytes = 8
    recordSize = asciiLen*2 + int64Bytes*2 // 48 bytes
)

// encodeRow converts row to fixed-length byte slice following layout:
// A[16] | B[8] | C[16] | D[8]
func encodeRow(r testRow) []byte {
    buf := make([]byte, recordSize)

    // A
    copy(buf[0:asciiLen], []byte(r.A))
    // B
    binary.LittleEndian.PutUint64(buf[asciiLen:asciiLen+8], uint64(r.B))
    // C
    copy(buf[asciiLen+8:asciiLen+8+asciiLen], []byte(r.C))
    // D
    binary.LittleEndian.PutUint64(buf[asciiLen+8+asciiLen:], uint64(r.D))
    return buf
}

// decodeRow converts bytes back to struct (helper for verification)
func decodeRow(id int64, b []byte) testRow {
    r := testRow{ID: id}
    r.A = string(bytes.TrimRight(b[0:asciiLen], "\x00"))
    r.B = int64(binary.LittleEndian.Uint64(b[asciiLen : asciiLen+8]))
    r.C = string(bytes.TrimRight(b[asciiLen+8:asciiLen+8+asciiLen], "\x00"))
    r.D = int64(binary.LittleEndian.Uint64(b[asciiLen+8+asciiLen:]))
    return r
}

func randomASCII(n int) string {
    letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

// TestCompareWithSQLite inserts records into both RingBufferCache and SQLite and validates equality.
func TestCompareWithSQLite(t *testing.T) {
    rand.Seed(time.Now().UnixNano())

    const total = 1000

    // --- Create RingBufferCache
    opts := archive.DefaultOptions()
    opts.UseMmap = false
    opts.ShardCount = 1
    opts.RecordSize = recordSize
    opts.MinIDAlloc = 1
    opts.MaxIDAlloc = total

    tmpDir, _ := os.MkdirTemp("", "cachetest")
    base := filepath.Join(tmpDir, "cache.data")
    cache, err := archive.NewRingBufferCacheWithOptions(base, opts)
    if err != nil {
        t.Fatalf("create cache: %v", err)
    }
    defer cache.Close()

    // --- Prepare SQLite (in-memory DB)
    db, err := sql.Open("sqlite", ":memory:")
    if err != nil {
        t.Fatalf("open sqlite: %v", err)
    }
    defer db.Close()

    ctx := context.Background()
    _, err = db.ExecContext(ctx, `CREATE TABLE tbl (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT, d INTEGER);`)
    if err != nil {
        t.Fatalf("create table: %v", err)
    }

    // prepare insert statement for sqlite
    stmt, err := db.PrepareContext(ctx, `INSERT INTO tbl (id, a, b, c, d) VALUES (?, ?, ?, ?, ?);`)
    if err != nil {
        t.Fatalf("prepare: %v", err)
    }
    defer stmt.Close()

    // Insert data into both stores
    rows := make([]testRow, total)
    for i := int64(1); i <= total; i++ {
        r := testRow{
            ID: i,
            A:  randomASCII(asciiLen),
            B:  rand.Int63(),
            C:  randomASCII(asciiLen),
            D:  rand.Int63(),
        }
        rows[i-1] = r

        // encode and write to cache
        if err := cache.Write(i, encodeRow(r), false); err != nil {
            t.Fatalf("cache write %d: %v", i, err)
        }

        // insert to sqlite
        if _, err := stmt.ExecContext(ctx, r.ID, r.A, r.B, r.C, r.D); err != nil {
            t.Fatalf("sqlite insert %d: %v", i, err)
        }
    }
    // flush cache
    if err := cache.Flush(); err != nil {
        t.Fatalf("flush cache: %v", err)
    }

    // Validate random subset
    for i := 0; i < 100; i++ {
        idx := int64(rand.Intn(total) + 1)

        // read from cache
        payload, err := cache.Read(idx)
        if err != nil {
            t.Fatalf("cache read %d: %v", idx, err)
        }
        rc := decodeRow(idx, payload)

        // read from sqlite
        var sq testRow
        row := db.QueryRowContext(ctx, `SELECT id, a, b, c, d FROM tbl WHERE id=?;`, idx)
        if err := row.Scan(&sq.ID, &sq.A, &sq.B, &sq.C, &sq.D); err != nil {
            t.Fatalf("sqlite read %d: %v", idx, err)
        }

        if rc != sq {
            t.Fatalf("mismatch for id %d: cache=%+v sqlite=%+v", idx, rc, sq)
        }
    }
}

// BenchmarkWrite compares write throughput between cache and sqlite.
func BenchmarkWrite(b *testing.B) {
    rand.Seed(42)
    total := int64(b.N)
    if total < 2 {
        total = 2 // ensure MaxIDAlloc > MinIDAlloc
    }

    // setup cache
    opts := archive.DefaultOptions()
    opts.UseMmap = false
    opts.ShardCount = 1
    opts.RecordSize = recordSize
    opts.MinIDAlloc = 1
    opts.MaxIDAlloc = total

    tmpDir, _ := os.MkdirTemp("", "cachebench")
    base := filepath.Join(tmpDir, "cache.data")
    cache, err := archive.NewRingBufferCacheWithOptions(base, opts)
    if err != nil {
        b.Fatalf("create cache: %v", err)
    }
    defer cache.Close()


    b.Run("ringbuffer", func(bb *testing.B) {
        opts := archive.DefaultOptions()
        opts.UseMmap = false
        opts.ShardCount = 1
        opts.RecordSize = recordSize
        opts.MinIDAlloc = 1
        maxID := int64(bb.N)
        if maxID < 2 {
            maxID = 2
        }
        opts.MaxIDAlloc = maxID
        tmpDir, _ := os.MkdirTemp("", "cacherb")
        base := filepath.Join(tmpDir, "cache.data")
        cache, err := archive.NewRingBufferCacheWithOptions(base, opts)
        if err != nil {
            bb.Fatalf("create cache: %v", err)
        }
        defer cache.Close()

        // prepare data specific for this sub-benchmark size
        rowBuf := make([]testRow, bb.N)
        for i := range rowBuf {
            rowBuf[i] = testRow{
                ID: int64(i + 1),
                A:  randomASCII(asciiLen),
                B:  rand.Int63(),
                C:  randomASCII(asciiLen),
                D:  rand.Int63(),
            }
        }
        for i := 0; i < bb.N; i++ {
            r := rowBuf[i]
            if err := cache.Write(r.ID, encodeRow(r), false); err != nil {
                bb.Fatalf("write: %v", err)
            }
        }
    })

    b.Run("sqlite", func(bb *testing.B) {
        db, err := sql.Open("sqlite", ":memory:")
        if err != nil {
            bb.Fatalf("open sqlite: %v", err)
        }
        defer db.Close()
        _, _ = db.Exec(`CREATE TABLE tbl (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT, d INTEGER);`)
        stmt, _ := db.Prepare(`INSERT INTO tbl (id, a, b, c, d) VALUES (?, ?, ?, ?, ?);`)
        rowBuf := make([]testRow, bb.N)
        for i := range rowBuf {
            rowBuf[i] = testRow{
                ID: int64(i + 1),
                A:  randomASCII(asciiLen),
                B:  rand.Int63(),
                C:  randomASCII(asciiLen),
                D:  rand.Int63(),
            }
        }
        for i := 0; i < bb.N; i++ {
            r := rowBuf[i]
            if _, err := stmt.Exec(r.ID, r.A, r.B, r.C, r.D); err != nil {
                bb.Fatalf("insert: %v", err)
            }
        }
    })
}
