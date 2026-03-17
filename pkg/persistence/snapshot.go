// Copyright 2026 Google LLC
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

package persistence

import (
	"context"
	"time"
)

// Snapshot represents a snapshot of the entire state
type Snapshot struct {
	Revision  int64     // The revision number when this snapshot was taken
	Timestamp time.Time // When this snapshot was created
	Data      []byte    // The serialized state data
}

// Snapshotter is the interface for taking and restoring snapshots
type Snapshotter interface {
	// CreateSnapshot creates a snapshot of the current state
	CreateSnapshot(ctx context.Context, data []byte) (*Snapshot, error)

	// GetLatestSnapshot returns the most recent snapshot
	GetLatestSnapshot(ctx context.Context) (*Snapshot, error)

	// GetSnapshotAtRevision returns the snapshot at or before the given revision
	GetSnapshotAtRevision(ctx context.Context, revision int64) (*Snapshot, error)

	// Close closes the snapshotter and releases any resources
	Close() error
}
