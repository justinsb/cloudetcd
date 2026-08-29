// Copyright 2026 Justin Santa Barbara
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package recordcache is a byte-budgeted LRU cache of decoded log records,
// keyed by revision. It is the in-memory working set of a log whose values
// live on disk or in an object store: recently written and recently read
// records stay in memory; everything else is re-read from the backend.
package recordcache

import (
	"container/list"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
)

type Revision = persistence.Revision

// Cache is safe for concurrent use.
type Cache struct {
	mu     sync.Mutex
	budget int64
	used   int64
	ll     *list.List // front = most recently used
	items  map[Revision]*list.Element
}

type item struct {
	revision Revision
	record   *persistence.LogRecord
	size     int64
}

// New creates a cache that holds at most budgetBytes of records (as measured
// by Size). A budget <= 0 means unbounded.
func New(budgetBytes int64) *Cache {
	return &Cache{budget: budgetBytes, ll: list.New(), items: map[Revision]*list.Element{}}
}

// Size estimates the memory a record occupies: its keys and values plus a
// fixed overhead per event for the structs.
func Size(record *persistence.LogRecord) int64 {
	const eventOverhead = 160
	var n int64 = 64
	for _, e := range record.Events {
		n += eventOverhead
		if e.Kv != nil {
			n += int64(len(e.Kv.Key) + len(e.Kv.Value))
		}
		if e.PrevKv != nil {
			n += int64(len(e.PrevKv.Key) + len(e.PrevKv.Value))
		}
	}
	return n
}

// Get returns the record for revision, marking it recently used.
func (c *Cache) Get(revision Revision) (*persistence.LogRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[revision]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*item).record, true
}

// Has reports whether revision is cached, without touching its recency.
func (c *Cache) Has(revision Revision) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[revision]
	return ok
}

// Put caches the record for revision, evicting least recently used records
// to stay within budget. A record larger than the whole budget is not cached.
func (c *Cache) Put(revision Revision, record *persistence.LogRecord) {
	size := Size(record)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[revision]; ok {
		it := el.Value.(*item)
		c.used += size - it.size
		it.record, it.size = record, size
		c.ll.MoveToFront(el)
	} else {
		if c.budget > 0 && size > c.budget {
			return
		}
		c.items[revision] = c.ll.PushFront(&item{revision: revision, record: record, size: size})
		c.used += size
	}
	for c.budget > 0 && c.used > c.budget && c.ll.Len() > 1 {
		c.evictOldest()
	}
}

// Remove drops the record for revision, if cached.
func (c *Cache) Remove(revision Revision) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[revision]; ok {
		c.removeElement(el)
	}
}

func (c *Cache) evictOldest() {
	if el := c.ll.Back(); el != nil {
		c.removeElement(el)
	}
}

func (c *Cache) removeElement(el *list.Element) {
	it := el.Value.(*item)
	c.ll.Remove(el)
	delete(c.items, it.revision)
	c.used -= it.size
}

// Len returns the number of cached records.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Bytes returns the estimated bytes of cached records.
func (c *Cache) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}
