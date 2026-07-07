# Walkthrough: cloud-etcd on your laptop, no cloud credentials required

This walkthrough runs the full cloud-etcd stack locally. Instead of a real
GCS bucket we use [objectstorage](https://github.com/justinsb/objectstorage),
a small object storage server that speaks the GCS JSON API — the official
Google Cloud Go client talks to it unchanged via `STORAGE_EMULATOR_HOST`.

You will:

1. Start a local object storage server and create a bucket.
2. Run cloud-etcd with the bucket as its change log.
3. Write and read keys through the etcd API.
4. Peek at the log objects to see how state is stored.
5. Restart cloud-etcd and watch it recover its state from the log.

All you need is Go and `curl`. (`etcdctl` is optional but nice; on macOS
`brew install etcd` provides it.)

## 1. Start the object storage server

In a terminal of its own:

```bash
go run github.com/justinsb/objectstorage/cmd/objectstorage@latest \
  --data-dir /tmp/objectstorage --listen :8080 --s3-listen ""
```

This serves the GCS JSON API on port 8080, storing everything under
`/tmp/objectstorage`. (`--s3-listen ""` disables the S3 endpoint, which we
don't need.)

Create a bucket for cloud-etcd:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"cloudetcd"}' \
  "http://localhost:8080/storage/v1/b?project=demo"
```

(The project name is arbitrary — the server ignores it, but the API requires
one.)

## 2. Start cloud-etcd

In a second terminal, from the root of this repository:

```bash
STORAGE_EMULATOR_HOST=localhost:8080 \
  go run ./cmd/cloud-etcd --log gs://cloudetcd/log/
```

`STORAGE_EMULATOR_HOST` points the GCS client at the local server; the same
command with the variable unset (and real credentials) talks to real GCS.
cloud-etcd is now serving the etcd v3 API on port 2379, using the bucket as
its write-ahead log.

## 3. Write and read some keys

In a third terminal:

```bash
etcdctl --endpoints=localhost:2379 put /hello world
etcdctl --endpoints=localhost:2379 get /hello
```

Any etcd v3 client works the same way — including `kube-apiserver`; see
[start-kube.md](../start-kube.md).

## 4. Look behind the curtain

Every transaction was committed as an object in the bucket before the client
saw a success response. List them:

```bash
curl -s "http://localhost:8080/storage/v1/b/cloudetcd/o" | python3 -m json.tool
```

You'll see objects like `log/0000000000000002-1.log`: the object name encodes
the first revision in the batch (hex, zero-padded so lexical order is revision
order) and the number of records in the batch. Download one — it's a JSON
batch of log records (keys and values are base64, since they are byte
strings):

```bash
curl -s "http://localhost:8080/cloudetcd/log/0000000000000002-1.log" | python3 -m json.tool
```

This is the whole persistence story: the bucket *is* the database. There is
no etcd cluster, no Raft, no disk state that matters — the object store's
conditional writes serialize commits, and everything else is rebuilt from
these objects.

## 5. Kill it and bring it back

Stop cloud-etcd (Ctrl-C in its terminal), then start it again:

```bash
STORAGE_EMULATOR_HOST=localhost:8080 \
  go run ./cmd/cloud-etcd --log gs://cloudetcd/log/
```

On startup it lists the log objects, replays them to rebuild its state, and
carries on:

```bash
etcdctl --endpoints=localhost:2379 get /hello
# /hello
# world
```

## 6. Optional: the tiered log

Committing every transaction to object storage costs one round-trip per
batch. The tiered log commits to a fast local tier first and drains to the
object-store archive in the background:

```bash
STORAGE_EMULATOR_HOST=localhost:8080 \
  go run ./cmd/cloud-etcd \
  --log 'tiered:?fast=filesystem:///tmp/cloudetcd-fast&archive=gs://cloudetcd/log/&flushInterval=5s'
```

Write a key, wait a few seconds, and list the bucket again — you'll see the
batch appear in the archive after the flush interval.

## Running the tests against the emulator

The GCS log's test suite runs against an in-process instance of the same
emulator, so it needs no credentials and runs with plain `go test`:

```bash
go test ./pkg/persistence/gcslog/ -run TestGCSLogWithEmulator -v
```

(The `TestGCSLog` suite additionally runs against real GCS when
`TEST_GCS_BUCKET` is set.)
