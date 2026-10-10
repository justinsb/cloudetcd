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
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// watchPrefix consumes a watch on prefix the way the apiserver's watch cache
// does (prefix, from rev+1, with prev_kv and progress notifications) until ctx
// is cancelled, re-establishing it from the last seen revision if the server
// ends it. For each event it measures the lag since the corresponding write
// was issued, and, if check is set, verifies the event against the oracle.
func (r *Runner) watchPrefix(ctx context.Context, watcher clientv3.Watcher, label, prefix string, rev int64, stats *WatchStats, check *watchCheck) {
	for ctx.Err() == nil {
		wch := watcher.Watch(ctx, prefix,
			clientv3.WithPrefix(),
			clientv3.WithRev(rev+1),
			clientv3.WithPrevKV(),
			clientv3.WithProgressNotify(),
		)
		for wr := range wch {
			if wr.IsProgressNotify() {
				stats.ProgressNotifies.Add(1)
				if check != nil {
					check.progress(wr.Header.Revision)
				}
			}
			if err := wr.Err(); err != nil {
				r.exec.getStats().recordError("watch/"+label, err)
				break
			}
			now := time.Now()
			for _, ev := range wr.Events {
				stats.Events.Add(1)
				if ev.Kv.ModRevision > rev {
					rev = ev.Kv.ModRevision
				}
				if st, ok := r.state.get(string(ev.Kv.Key)); ok && !st.writeStart.IsZero() && (st.inflight || st.modRev == ev.Kv.ModRevision) {
					stats.Lag.Observe(now.Sub(st.writeStart))
				}
				if check != nil {
					check.event(ev, now)
				}
			}
		}
		if ctx.Err() == nil {
			stats.Restarts.Add(1)
		}
	}
}

// watchSet is the apiserver's set of prefix watches, one per watched
// resource, attached for the duration of a phase.
type watchSet struct {
	r       *Runner
	watcher clientv3.Watcher
	checks  []*watchCheck
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	// startRev is the revision the watches were opened after.
	startRev int64
}

// startWatches opens the apiserver's prefix watches from the current
// revision and, if the server exposes it, starts sampling its watchers.
func (r *Runner) startWatches(ctx context.Context, stats *Stats) (*watchSet, error) {
	c := &r.cfg
	// The apiserver lists then watches from the list's revision; here we just
	// need a current revision to watch from.
	revResp, err := r.client.Get(ctx, c.Prefix+healthKey)
	if err != nil {
		return nil, fmt.Errorf("getting current revision: %w", err)
	}
	rev := revResp.Header.Revision
	if r.exec.check != nil {
		r.exec.check.header(revResp.Header, 0, "watch-start")
	}

	watchCtx, cancel := context.WithCancel(ctx)
	ws := &watchSet{r: r, watcher: clientv3.NewWatcher(r.client), cancel: cancel, startRev: rev}
	for _, res := range watchedResources {
		var check *watchCheck
		if r.exec.check != nil {
			check = r.exec.check.Watch(res, c.resourcePrefix(res), rev)
			ws.checks = append(ws.checks, check)
		}
		ws.wg.Add(1)
		go func() {
			defer ws.wg.Done()
			r.watchPrefix(watchCtx, ws.watcher, res, c.resourcePrefix(res), rev, stats.Watch(res), check)
		}()
	}
	if r.WatcherStatus != nil {
		ws.wg.Add(1)
		go func() {
			defer ws.wg.Done()
			r.sampleWatchers(watchCtx, stats)
		}()
	}
	return ws, nil
}

// stop closes the watches once they have caught up with endRev (the last
// revision written in the phase), requesting progress to find out, and
// checks that every write in the phase was delivered. Writes must have
// stopped: it is called after the pool is closed.
func (ws *watchSet) stop(ctx context.Context, endRev int64) {
	if len(ws.checks) > 0 {
		deadline := time.Now().Add(10 * time.Second)
		for {
			behind := false
			for _, check := range ws.checks {
				if check.reached() < endRev {
					behind = true
				}
			}
			if !behind || time.Now().After(deadline) || ctx.Err() != nil {
				break
			}
			_ = ws.watcher.RequestProgress(ctx)
			time.Sleep(50 * time.Millisecond)
		}
		// Anything the server still sends after it claimed to be caught up
		// is a violation the checks will catch; give it a moment to arrive.
		time.Sleep(200 * time.Millisecond)
	} else {
		time.Sleep(100 * time.Millisecond)
	}
	ws.cancel()
	ws.wg.Wait()
	_ = ws.watcher.Close()
	for _, check := range ws.checks {
		check.finish(endRev)
	}
}
