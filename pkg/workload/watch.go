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
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// watchPrefix consumes a watch on prefix the way the apiserver's watch cache
// does (prefix, from rev+1, with prev_kv and progress notifications) until ctx
// is cancelled, re-establishing it from the last seen revision if the server
// ends it. For each event it measures the lag since the corresponding write
// was issued.
func (r *Runner) watchPrefix(ctx context.Context, watcher clientv3.Watcher, label, prefix string, rev int64, stats *WatchStats) {
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
			}
		}
		if ctx.Err() == nil {
			stats.Restarts.Add(1)
		}
	}
}
