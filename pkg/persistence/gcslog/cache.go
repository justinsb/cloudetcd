package gcslog

import (
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
)

type Cache struct {
	mu    sync.Mutex
	cache map[Revision]*persistence.LogRecord
}

func NewCache() *Cache {
	c := &Cache{
		cache: make(map[Revision]*persistence.LogRecord),
	}
	return c
}

func (c *Cache) Has(revision Revision) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.cache[revision]
	return ok
}

func (c *Cache) Get(revision Revision, load func() (*persistence.LogRecord, error)) (*persistence.LogRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO: Concurrent downloads
	logEntry, ok := c.cache[revision]
	if ok {
		return logEntry, nil
	}

	logEntry, err := load()
	if err != nil {
		return nil, err
	}
	c.cache[revision] = logEntry
	return logEntry, nil
}
