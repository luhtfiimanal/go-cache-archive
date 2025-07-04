package archive

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Flush memaksa semua data tersimpan ke disk.
func (c *RingBufferCache) Flush() error {
	var firstErr error
	for i, s := range c.shards {
		if s.mmap != nil {
			if err := unix.Msync(s.mmap, unix.MS_SYNC); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("gagal msync shard %d: %w", i, err)
			}
		} else {
			if err := s.file.Sync(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("gagal sync shard %d: %w", i, err)
			}
		}
	}
	return firstErr
}

// Close menutup semua sumber daya (file & mmap) milik cache.
func (c *RingBufferCache) Close() error {
	var firstErr error
	for i, s := range c.shards {
		if s.mmap != nil {
			if err := unix.Munmap(s.mmap); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("gagal unmap shard %d: %w", i, err)
			}
		}
		if err := s.file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("gagal menutup shard %d: %w", i, err)
		}
	}
	return firstErr
}
