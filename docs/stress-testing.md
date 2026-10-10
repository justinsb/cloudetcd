# Stress and performance testing

cloudetcd's only client is kube-apiserver, and nodes never talk to etcd
directly. So "what does a 1,000-node cluster do to cloudetcd?" is a question
about the etcd v3 request stream a kube-apiserver produces, which scales with
the number of nodes and pods in ways that are easy to model. The stress test
therefore does **not** run kube-apiserver or nodes: it records what a real
apiserver does once (a *template*), and regenerates that traffic at any scale
using only the etcd client library.

```
   capture (once, needs kube-apiserver + kwok)         run (any N, needs nothing)
   ┌───────────────┐   etcd RPCs   ┌───────────┐       ┌──────────────┐   etcd RPCs   ┌───────────┐
   │ kube-apiserver│──────────────▶│ cloudetcd │       │ pkg/workload │──────────────▶│ cloudetcd │
   │  + kwok nodes │               │ --record  │       │  N nodes     │               │           │
   └───────────────┘               └─────┬─────┘       └──────────────┘               └───────────┘
                                         │ recording.jsonl        ▲
                                         ▼                        │ cadences, shapes, value blobs
                                  analyze / extract-blobs ────────┘
```

## Running a benchmark

`cmd/cloud-etcd-bench` starts an in-process cloudetcd (or targets an existing
endpoint) and drives it with the traffic of a cluster of the given size:

```bash
# 1,000 nodes with 30 pods each, 2 minutes of steady state
go run ./cmd/cloud-etcd-bench run -nodes 1000 -pods-per-node 30 -duration 2m

# On a real directory (memory:// is a temporary directory removed at exit)
go run ./cmd/cloud-etcd-bench run -nodes 1000 -log filesystem:///var/tmp/bench

# ... with rotated files archived to a bucket (or the local emulator, see docs/walkthrough)
go run ./cmd/cloud-etcd-bench run -nodes 1000 -log 'filesystem:///var/tmp/bench?archive=gs://bucket/bench&rotateAfter=10s'

# All phases, JSON output, CPU profile
go run ./cmd/cloud-etcd-bench run -nodes 100 -phases populate,steady,list-storm -o json -cpuprofile cpu.prof

# Against a running server
go run ./cmd/cloud-etcd-bench run -endpoint 127.0.0.1:2379 -nodes 100
```

The phases, run in order on the same state:

| phase | what it models |
|---|---|
| `populate` | the cluster coming up: every node registers, creates its lease, and its pods are created, bound and reported running — as fast as `-workers` allows |
| `steady` | the cluster humming for `-duration`: node lease renewals (1 per node / 10s — the dominant write), node status reports (1 per node / 5m), pod churn with events, apiserver housekeeping (masterlease, per-resource counts, compaction), consistent reads, with the apiserver's prefix watches attached |
| `saturate` | the same operations closed-loop, as fast as the server takes them, in a seeded random mix with contended keys (`-saturate-duration`, `-seed`, `-contention`); prints a `HEADLINE` line |
| `list-storm` | many clients relisting all pods and nodes at once, which is what happens when watch caches fall behind and controllers restart |
| `verify` | lists the whole state and compares it with what was written (see the stress test below); the run fails on any violation, in any phase, unless `-verify=false` |

Every cadence has a flag (`-node-lease-interval`, `-pod-churn`, ...); see
`go run ./cmd/cloud-etcd-bench run -h`.

### Reading the report

```
== steady: 1000 nodes, 0 pods (2000 keys), 30.0s, 3110 ops (104/s), 0 errors
op                  count  errors  conflicts  rate/s  expected/s  p50     p90     p99     max
node-lease-update   3000   0       0          100.0   100.00      11.4ms  31.2ms  42.1ms  61.3ms
...
scheduling lag: p50 593µs  p90 955µs  p99 1.0ms  max 1.0ms (ops started this long after they were due)
watch                   events  events/s  lag p50  lag p90  lag p99
leases/kube-node-lease  3000    100.0     11.4ms   31.2ms   42.1ms
```

- **rate/s vs expected/s**: the model is open-loop — it decides when each
  op is *due* from the cluster size and cadences. If the achieved rate is
  below the expected rate, the server is not keeping up.
- **scheduling lag**: how late ops started relative to when they were due.
  It grows without bound when the server falls behind, so it is the primary
  "is this cluster size sustainable" signal; latency percentiles alone hide
  this (coordinated omission).
- **watch lag**: time from a write being issued to its event arriving on the
  apiserver's watch — what a kubelet's status update takes to reach the
  scheduler.
- **conflicts**: conditional Txns that failed their mod_revision guard and
  were retried, as the apiserver would.

### What N nodes looks like at the etcd API

| source | etcd op | rate |
|---|---|---|
| kubelet heartbeat | `Txn(If mod_rev(/registry/leases/kube-node-lease/N)==r Then Put)` | 1 per node / 10s |
| kubelet node status | same shape on `/registry/minions/N` | 1 per node / 5m |
| pod lifecycle | create, bind, status, deletionTimestamp, status, delete on `/registry/pods/ns/p` | per churn |
| events | `Txn` create with a lease on `/registry/events/ns/e` | ~4 per pod lifecycle |
| apiserver housekeeping | masterlease get+list+`LeaseGrant`+conditional `Put`, peerserverlease `LeaseGrant`+`Put`, identity Lease update in `kube-system` | 1 / 10s |
| consistent reads | `Range(/registry/<resource>)` — a single-key get for the current revision | as requested |
| object counts | `Range(prefix, count_only)` for resources with no watch cache (events) | 1 / 1m |
| health probe | `Range(/registry/health)` | 1 / 25s |
| compaction | `Txn(If version(compact_rev_key)==v Then Put)`; `Compact(rev)` | 1 / 5m |
| watch cache | `Watch(prefix, rev+1, prev_kv, progress_notify)`, one stream per resource, plus progress requests | **independent of N** |

The last row matters: 100k nodes is not 100k watches. It is a handful of
prefix watches each receiving 10k events/s. So the interesting numbers are
write throughput, watch event throughput and watch lag — not watcher count.

### Compaction

The model issues `Compact` every 5 minutes as the apiserver does (`-compact-interval` shortens that). The server prunes its index to each key's latest version at or below the compaction point, drops deleted keys, and rewrites closed log files in the background keeping only live records (a compacted file is sparse in revision and named by the range it covers). The report's `log … MB on disk` per phase is how to see it: with compaction the log settles at the live set plus one interval's tail; without it the log grows at the write rate.

## The gated stress test: benchmark and correctness check in one

`tests/stress` runs every phase against an in-process server and is two
things at once: a benchmark with one headline number, and a correctness
test that checks every response the server gives. It is skipped unless
`RUN_STRESS` is set:

```bash
RUN_STRESS=1 go test ./tests/stress -v
RUN_STRESS=1 STRESS_NODES=200 STRESS_PODS_PER_NODE=10 STRESS_SATURATE=1m go test ./tests/stress -v
```

The run is, in order: `populate`, `steady` (`STRESS_DURATION`, default
15s), `saturate` (`STRESS_SATURATE`, default 30s), `list-storm`, a check
of the whole state against what was written, then a **restart** — the
server is stopped, a new one started on the same log, the state checked
again and the run's history replayed on a fresh watch and compared with what
happened. The cadences of the model are compressed (`STRESS_SPEEDUP`,
default 10, so 50 nodes produce the traffic of 500) and the paths
kube-apiserver depends on are stressed harder than a real cluster would:
the masterlease is re-attached to a new lease every second with a 2s TTL,
Events expire mid-run, compaction runs every 5s against live traffic.

### The headline

```
HEADLINE saturate: 2343 ops/s over 10.5s with 64 workers (20 nodes x 3 pods); writes p50 24.4ms p99 164.2ms; reads p50 334µs p99 5.8ms; 20749 watch events, lag p99 92.7ms; 0 errors, 0 violations (seed 1791633727273573000)
```

The `saturate` phase is a closed loop: every worker issues the next
operation as soon as the previous one completes, choosing from the
steady-state mix at random (node lease renewals dominate, then node status,
pod lifecycles with their Events, single-key reads, consistent reads,
namespace lists, counts) with a 15% share of *contended* writes — a second
writer doing a GuaranteedUpdate on a pod or node its kubelet is also
writing, with the conflict-and-retry that follows, as the scheduler and
controllers do. Housekeeping, compaction and the apiserver's prefix watches
run alongside. Its ops/s is the number to optimize; the write and read
tails and the watch lag say where the time goes. `cloud-etcd-bench run
-phases populate,saturate,verify` runs the same phase at any size.

### What is checked

The workload is the only client, so it knows the complete history of every
key: every value written carries a trailer with a nonce unique to that write
(cloudetcd never inspects values), and `workload.Checker` records every
acknowledged write by revision. Every response is checked against that
record, in terms of what kube-apiserver relies on:

| kube depends on | checked |
|---|---|
| conditional writes (`GuaranteedUpdate`, `Create`, `Delete`) | a revision is never assigned twice; a failed compare returns a mod_revision other than the one compared; a create cannot succeed over a key known to be live; a key cannot vanish unless deleted or its lease lapsed |
| reads at a revision (consistent reads, paginated lists) | value, version, create_revision and lease are the oracle's latest write at or below that revision; a write acknowledged at `w <= R` is never missing from a read at `R`; no unknown revisions |
| real-time order | no response carries a revision below one seen before the request was issued |
| watch cache | events in strictly increasing revision order, each matching the write at that revision, `prev_kv` matching the previous version; every write under the prefix delivered exactly once; **no event for a revision a progress notification already covered** (what consistent reads from the cache rest on) |
| lists | keys ascending across pages; a full list has exactly the live keys |
| leases (Events, masterlease) | the server deletes a key only once its current lease could have expired; a key re-attached to a fresh lease survives the old lease's expiry |
| restart | the state and the history come back from the log exactly |

Anything else is a *violation*, which fails the test. (A check that cannot
be settled because a write to that key is in flight is parked and settled at
the end of the phase.)

### When it fails: seed, diagnostic mode, repro

Every run prints its seed and the exact command to re-run it, and the first
violation is logged the moment it happens, next to the server's own logging
of that instant. The failure lists each violation with the oracle's full
history of the key involved:

```
FIRST VIOLATION at ...: watch-event-after-progress (watch/minions) key /registry/minions/node-000003 rev 8146: event at revision 8146 arrived after a progress notification at revision 8150 claimed everything through it had been delivered
history of /registry/minions/node-000003 (109 versions):
  rev 12  PUT nonce 2 version 1 create_revision 12 lease 0
  ...
29 correctness violations (seed 1791633727273573000); re-run with: RUN_STRESS=1 STRESS_SEED=1791633727273573000 STRESS_NODES=20 ... STRESS_DIAG=1 go test ./tests/stress -run TestStress -v
```

The seed fixes each worker's sequence of choices (not the interleaving, so a
race may take a few runs to recur). `STRESS_DIAG=1` is diagnostic mode: the
run stops at the first violation, and every RPC the server handled is
recorded (`pkg/recording`, the same format as `cloud-etcd -record`) to
`$ARTIFACTS/stress/seed-<seed>/recording.jsonl` (or `.build/stress/...`),
so the exact request stream that led to the bug is on disk for building a
focused test. Each run also writes `summary.json` there with the reports and
violations, and `server.log` with the server's own logging (which is kept
out of the test output). The same checks run in `pkg/workload`'s smoke test on every `go
test`, and the stress test works under `-race` (slower; the headline then
means little).

## Capturing a template from a real kube-apiserver

The model in `pkg/workload` encodes the request shapes of
`k8s.io/apiserver/pkg/storage/etcd3` and the default kubelet/apiserver
cadences. To check it against reality — or to update it when Kubernetes
changes — record a real apiserver:

```bash
# Boot kube-apiserver against a recording cloudetcd, have kwok fake 20 nodes
# with 5 pods each, record 2 minutes of steady state plus the pods' deletion.
RUN_CAPTURE=1 CAPTURE_NODES=20 CAPTURE_PODS_PER_NODE=5 CAPTURE_DURATION=2m \
  go test ./tests/e2e -run TestCaptureKwok -v -timeout 30m

# Summarize by request shape and key pattern: counts, rates, per-node rates,
# value sizes, watches and their event counts.
go run ./cmd/cloud-etcd-bench analyze .build/capture/kwok-20n-5p.jsonl

# Pull median-sized real values out of the recording for the benchmark to use
# instead of the generated objects.
go run ./cmd/cloud-etcd-bench extract-blobs .build/capture/kwok-20n-5p.jsonl .build/capture/blobs
go run ./cmd/cloud-etcd-bench run -nodes 1000 -blobs .build/capture/blobs
```

Any cloudetcd can record: `cloud-etcd -record traffic.jsonl` writes one JSON
line per RPC (protojson, so it decodes back to the exact `etcdserverpb`
messages) — point a kind cluster's apiserver at it to capture real kubelets.

The model shipped in `pkg/workload` was checked this way against
kube-apiserver v1.36.2 with 10 kwok nodes × 5 pods (`.build/capture` after
running the capture): node lease renewals came out at 5.2/node/min (the
model says 6), pods took 1 create + 3 updates + get&delete per lifecycle, 60
watch streams were opened with exactly the modelled options, and the
recording corrected the model on object counts (only events go through
etcd), the shape of consistent reads, and the apiserver's own 10s
housekeeping.

### Values

cloudetcd never inspects values, so what matters is their size and, for any
future compression, their content. By default the bench and stress test
write **generated Kubernetes objects encoded exactly as the apiserver stores
them** (`pkg/workload/kubeblobs`: the protobuf serializer with its `k8s\x00`
framing, including `managedFields`, which are a large share of a stored
object). The objects resemble a managed cluster's: a kubelet-reported Node
with its image list (~12 KB at 30 images, `-node-images`), a Deployment pod
with probes/env/volumes (~6 KB, `-pod-containers`), a node Lease (~480 B;
the capture measured 510), an Event, and the masterlease Endpoints. This is
the only part of the benchmark that imports Kubernetes API types;
`pkg/workload` itself stays kube-free, and `-blobs synthetic` or
`-blobs <dir>` (captured values from `extract-blobs`) substitute other
values.

`analyze` prints a `per-node/min` column: a shape whose rate scales with the
node count shows a stable per-node rate across captures of different sizes
(e.g. `txn update /registry/leases/kube-node-lease/*` at 6/min), while
cluster-wide housekeeping does not. Comparing that output with
`Config.ExpectedRates()` is how the model is validated.

[kwok](https://kwok.sigs.k8s.io/) plays the kubelets (registration, lease
renewal, status, pod status) without running containers, so captures scale
to thousands of nodes on one machine. Two things it does not reproduce:
a real kubelet's object sizes (a real Node carries its image list, so is
larger than kwok's), and the controller-manager's traffic (none runs in the
harness). A kind cluster gives both, at N=1.
