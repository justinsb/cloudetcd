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

package batch

import (
	"context"
	"fmt"
	"sync"
)

// Sequencer orders the publication of batches that were written
// concurrently. Batching hands consecutive batches to the backend without
// waiting for the previous write to finish (see Batching); the backend does
// its slow write unlocked, then calls Wait for its predecessor to be
// published before it publishes its own records and calls Advance. That
// keeps the log's visible revision contiguous and in order no matter which
// write finishes first.
type Sequencer struct {
	mu       sync.Mutex
	position Revision
	// changed is closed and replaced on every Advance, so waiters can select
	// on it together with their context.
	changed chan struct{}
}

// NewSequencer creates a Sequencer at position.
func NewSequencer(position Revision) *Sequencer {
	return &Sequencer{position: position, changed: make(chan struct{})}
}

// Position returns the last published revision.
func (s *Sequencer) Position() Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position
}

// Wait blocks until the last published revision is exactly position, i.e.
// the batch that starts at position+1 may publish. It returns an error if the
// sequencer has already moved past position (the batch is not contiguous),
// or ctx's error if ctx is done first — which is how Batching unblocks a
// batch whose predecessor failed.
func (s *Sequencer) Wait(ctx context.Context, position Revision) error {
	for {
		s.mu.Lock()
		current, changed := s.position, s.changed
		s.mu.Unlock()
		if current == position {
			return nil
		}
		if current > position {
			return fmt.Errorf("batch is not contiguous with the log: expected to publish after %d, log is at %d", position, current)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Advance publishes up to position and wakes waiters.
func (s *Sequencer) Advance(position Revision) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if position <= s.position {
		return
	}
	s.position = position
	close(s.changed)
	s.changed = make(chan struct{})
}
