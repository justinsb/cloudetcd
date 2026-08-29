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

package gcslog

import (
	"context"
	"fmt"
	"sync"

	"justinsb.com/cloudetcd/pkg/persistence"
	"justinsb.com/cloudetcd/pkg/persistence/recordcache"
)

// Cache is the log's in-memory working set: a byte-budgeted LRU of decoded
// records (see recordcache), filled as batches are written and as they are
// fetched from GCS. Concurrent fetches of the same batch are coalesced.
type Cache struct {
	records *recordcache.Cache

	mu      sync.Mutex
	loading map[Revision]*sync.Mutex // per batch (by first revision)
}

// NewCache creates a cache holding at most budgetBytes of records (<= 0:
// unbounded).
func NewCache(budgetBytes int64) *Cache {
	return &Cache{records: recordcache.New(budgetBytes), loading: map[Revision]*sync.Mutex{}}
}

// Has reports whether revision is cached.
func (c *Cache) Has(revision Revision) bool {
	return c.records.Has(revision)
}

type LoadBatchFunc func(ctx context.Context, meta logFileMeta) (*persistedBatch, error)

// Get returns the record for revision, fetching and caching its whole batch
// with load if it is not cached.
func (c *Cache) Get(ctx context.Context, revision Revision, load LoadBatchFunc, meta logFileMeta) (*persistence.LogRecord, error) {
	if record, ok := c.records.Get(revision); ok {
		return record, nil
	}

	c.mu.Lock()
	batchMu := c.loading[meta.firstRevision]
	if batchMu == nil {
		batchMu = &sync.Mutex{}
		c.loading[meta.firstRevision] = batchMu
	}
	c.mu.Unlock()

	batchMu.Lock()
	defer batchMu.Unlock()
	if record, ok := c.records.Get(revision); ok {
		return record, nil
	}
	batch, err := load(ctx, meta)
	if err != nil {
		return nil, err
	}
	pos := int(revision - meta.firstRevision)
	if pos < 0 || pos >= len(batch.Records) {
		return nil, fmt.Errorf("log entry not found in batch for revision %d (pos %d, count %d)", revision, pos, len(batch.Records))
	}
	c.notifyBatch(meta.firstRevision, batch)
	return batch.Records[pos], nil
}

// notifyBatch caches every record of a batch.
func (c *Cache) notifyBatch(firstRevision Revision, data *persistedBatch) {
	for i, record := range data.Records {
		c.records.Put(firstRevision+Revision(i), record)
	}
}
