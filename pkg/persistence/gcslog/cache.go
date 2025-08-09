package gcslog

import (
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

func (c *Cache) Get(revision Revision, load func() (*persistence.LogRecord, error)) (*persistence.LogRecord, error) {
	entry := c.getEntry(revision, true)

	return entry.getRecord(load)
}

func (e *cacheEntry) getRecord(load func() (*persistence.LogRecord, error)) (*persistence.LogRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.record != nil {
		return e.record, nil
	}

	logEntry, err := load()
	if err != nil {
		return nil, err
	}
	e.record = logEntry
	return logEntry, nil
}

func (e *cacheEntry) hasValue() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.record != nil
}
