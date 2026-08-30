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

package filesystemlog

import "time"

// worker runs a function over queued items, in order, on its own goroutine.
// It is the shape of the log's background work: callers add items and
// never wait for them; stop drains the queue.
type worker[T any] struct {
	queue chan T
	done  chan struct{}
}

// newWorker makes a worker whose queue holds capacity items. Items can be
// added before start; nothing runs until it is called.
func newWorker[T any](capacity int) *worker[T] {
	return &worker[T]{queue: make(chan T, capacity), done: make(chan struct{})}
}

// start runs fn on each queued item, on a new goroutine.
func (w *worker[T]) start(fn func(T)) {
	go func() {
		defer close(w.done)
		for item := range w.queue {
			fn(item)
		}
	}()
}

// add queues an item, blocking if the queue is full.
func (w *worker[T]) add(item T) { w.queue <- item }

// stop closes the queue and waits up to timeout for the worker to finish
// the items in it. It reports whether the worker finished.
func (w *worker[T]) stop(timeout time.Duration) bool {
	close(w.queue)
	select {
	case <-w.done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ticker calls a function every interval, on its own goroutine, until
// stopped.
type ticker struct {
	stopping chan struct{}
	done     chan struct{}
}

func startTicker(interval time.Duration, fn func()) *ticker {
	t := &ticker{stopping: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(t.done)
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-t.stopping:
				return
			case <-tick.C:
				fn()
			}
		}
	}()
	return t
}

// stop stops the ticker and waits for a call in progress to return.
func (t *ticker) stop() {
	close(t.stopping)
	<-t.done
}
