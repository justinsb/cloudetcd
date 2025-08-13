package gcslog

import (
	"context"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
)

type Cache struct {
	mu    sync.Mutex
	cache map[Revision]*cacheEntry
}

type cacheEntry struct {
	mu       sync.Mutex
	revision Revision
	record   *persistence.LogRecord
}

func NewCache() *Cache {
	c := &Cache{
		cache: make(map[Revision]*cacheEntry),
	}
	return c
}

func (c *Cache) Has(revision Revision) bool {
	entry := c.getEntry(revision, false)
	if entry == nil {
		return false
	}
	return entry.hasValue()
}

// getEntry returns the cache entry for the given revision
func (c *Cache) getEntry(revision Revision, create bool) *cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.cache[revision]
	if !ok && create {
		entry = &cacheEntry{
			revision: revision,
		}
		c.cache[revision] = entry
	}
	return entry
}

type LoadBatchFunc func(ctx context.Context, meta logFileMeta) (*persistedBatch, error)

func (c *Cache) Get(ctx context.Context, revision Revision, load LoadBatchFunc, meta logFileMeta) (*persistence.LogRecord, error) {
	entry := c.getEntry(revision, true)

	batch, record, err := entry.loadRecord(ctx, meta, load)
	if err != nil {
		return nil, err
	}
	if batch != nil {
		for i := 0; i < meta.count; i++ {
			pos := meta.firstRevision + Revision(i)
			logEntry := batch.Records[i]
			entry := c.getEntry(pos, true)
			entry.setRecord(logEntry)
		}
	}
	return record, nil
}

func (e *cacheEntry) setRecord(record *persistence.LogRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.record = record
}

func (e *cacheEntry) loadRecord(ctx context.Context, meta logFileMeta, load LoadBatchFunc) (*persistedBatch, *persistence.LogRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.record != nil {
		return nil, e.record, nil
	}

	batch, err := load(ctx, meta)
	if err != nil {
		return nil, nil, err
	}

	logEntry := batch.Records[e.revision-meta.firstRevision]
	e.record = logEntry
	return batch, logEntry, nil
}

func (e *cacheEntry) hasValue() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.record != nil
}
