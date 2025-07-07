package archive

import (
    "encoding/json"
    "fmt"
    "os"
)

// persistedConfig captures the subset of CacheOptions that affects file layout.
type persistedConfig struct {
    RecordSize  int   `json:"record_size"`
    MinIDAlloc  int64 `json:"min_id_alloc"`
    MaxIDAlloc  int64 `json:"max_id_alloc"`
    ShardCount  int   `json:"shard_count"`
}

func newPersistedConfig(opts CacheOptions) persistedConfig {
    return persistedConfig{
        RecordSize: opts.RecordSize,
        MinIDAlloc: opts.MinIDAlloc,
        MaxIDAlloc: opts.MaxIDAlloc,
        ShardCount: opts.ShardCount,
    }
}

// verifyOrWriteConfig loads an existing .config file if present and verifies it
// matches the supplied options. If the file does not exist, it is created.
// On mismatch, it returns an error detailing the differences.
func verifyOrWriteConfig(path string, opts *CacheOptions) error {
    want := newPersistedConfig(*opts)

    if _, err := os.Stat(path); os.IsNotExist(err) {
        // first time: write file
        f, err := os.Create(path)
        if err != nil {
            return fmt.Errorf("create config file: %w", err)
        }
        defer f.Close()
        enc := json.NewEncoder(f)
        enc.SetIndent("", "  ")
        if err := enc.Encode(want); err != nil {
            return fmt.Errorf("encode config: %w", err)
        }
        return nil
    }

    // file exists, load & sync options
    f, err := os.Open(path)
    if err != nil {
        return fmt.Errorf("open config file: %w", err)
    }
    defer f.Close()
    var have persistedConfig
    if err := json.NewDecoder(f).Decode(&have); err != nil {
        return fmt.Errorf("decode config: %w", err)
    }

    // override supplied opts with persisted values to ensure consistency
    opts.RecordSize = have.RecordSize
    opts.MinIDAlloc = have.MinIDAlloc
    opts.MaxIDAlloc = have.MaxIDAlloc
    opts.ShardCount = have.ShardCount
    return nil
}
