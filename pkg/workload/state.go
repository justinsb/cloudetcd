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

package workload

import (
	"hash/fnv"
	"sync"
	"time"
)

// keyState is what the generator remembers about a key it has written: the
// mod_revision the server reported (needed for the conditional update that
// follows, exactly as the apiserver remembers resourceVersion), and when the
// most recent write was issued (to measure watch lag).
type keyState struct {
	modRev     int64
	writeStart time.Time
	inflight   bool
}

// stateMap is a sharded map from key to keyState.
type stateMap struct {
	shards [256]stateShard
}

type stateShard struct {
	mu sync.Mutex
	m  map[string]keyState
}

func newStateMap() *stateMap {
	s := &stateMap{}
	for i := range s.shards {
		s.shards[i].m = map[string]keyState{}
	}
	return s
}

func (s *stateMap) shard(key string) *stateShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &s.shards[h.Sum32()%uint32(len(s.shards))]
}

func (s *stateMap) get(key string) (keyState, bool) {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st, ok := sh.m[key]
	return st, ok
}

// beginWrite marks a write as issued and returns the last known mod_revision.
func (s *stateMap) beginWrite(key string) int64 {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.m[key]
	st.writeStart = time.Now()
	st.inflight = true
	sh.m[key] = st
	return st.modRev
}

// endWrite records the mod_revision the server assigned to the write.
func (s *stateMap) endWrite(key string, modRev int64) {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	st := sh.m[key]
	st.modRev = modRev
	st.inflight = false
	sh.m[key] = st
}

func (s *stateMap) delete(key string) {
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	delete(sh.m, key)
}
