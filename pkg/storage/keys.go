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

package storage

import "bytes"

// InternalPrefix reserves a corner of the keyspace for cloudetcd's own
// state (today, lease records; see pkg/lease), kept as ordinary records in
// the log so that replay and compaction need no special cases. The prefix
// starts with a NUL byte, which no ordinary client key uses (Kubernetes
// keys live under '/'). Internal keys are hidden from List results and
// Watch events; reads and writes of an exact internal key pass through,
// which is how the internal callers use them.
var InternalPrefix = []byte("\x00cloudetcd/")

// IsInternalKey reports whether key is in the reserved internal namespace.
func IsInternalKey(key []byte) bool {
	// Every ordinary key fails on the first byte, before the prefix
	// comparison; this runs for every listed key and watched event.
	return len(key) > 0 && key[0] == 0 && bytes.HasPrefix(key, InternalPrefix)
}
