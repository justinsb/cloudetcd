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

//go:build unix

package filesystemlog

import (
	"os"
	"syscall"
)

// mapFile maps a whole file read-only. The mapping is backed by the page
// cache: pages are read on first touch and evicted by the OS under memory
// pressure, so old records cost no heap.
func mapFile(path string, size int64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func unmapFile(m []byte) error {
	if m == nil {
		return nil
	}
	return syscall.Munmap(m)
}
