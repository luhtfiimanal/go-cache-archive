package archive

import (
    "runtime"
)

// initPrefetchWorkers starts a fixed pool of goroutines that handle
// background prefetch tasks. Pool size = 2×CPU but at least 4.
func (c *RingBufferCache) initPrefetchWorkers() {
    c.prefetchOnce.Do(func() {
        worker := runtime.NumCPU() * 2
        if worker < 4 {
            worker = 4
        }
        c.prefetchCh = make(chan prefetchTask, worker*4)
        for i := 0; i < worker; i++ {
            go func() {
                for task := range c.prefetchCh {
                    // ignore error; best-effort warm-up
                    _, _ = c.Read(task.id)
                    c.prefetchMap.Delete(task.id)
                }
            }()
        }
    })
}

// enqueuePrefetch sends an id into the worker pool if not already queued.
func (c *RingBufferCache) enqueuePrefetch(absID int64) {
    if _, exists := c.prefetchMap.LoadOrStore(absID, true); exists {
        return // already in-flight or done
    }
    if c.prefetchCh == nil {
        return // pool not initialized (PrefetchSize == 0)
    }
    select {
    case c.prefetchCh <- prefetchTask{id: absID}:
    default:
        // channel full, drop silently
    }
}
