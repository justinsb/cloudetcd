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

// Package memorylog provides a throwaway log for tests and benchmarks: the
// file log over a temporary directory that is removed on Close.
package memorylog

import (
	"justinsb.com/cloudetcd/pkg/persistence/filesystemlog"
)

// MemoryLog is a file log over a temporary directory.
type MemoryLog = filesystemlog.FilesystemLog

// New creates a new log over a fresh temporary directory.
func New() *MemoryLog {
	log, err := filesystemlog.NewTempLog(filesystemlog.Options{})
	if err != nil {
		panic(err)
	}
	return log
}
