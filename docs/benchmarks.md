# Benchmarks

Every number here was measured on a running cluster with `cmd/raftkv-bench`,
over real gRPC, with real fsyncs underneath. Nothing is extrapolated and
nothing is a microbenchmark of a function call. The methodology and the
hardware are written down so the numbers can be argued with, which is the only
thing that makes them worth publishing.

**Hardware.** Apple M3 Pro, 11 cores, 18 GB, macOS 26.6.2, Go 1.25.6.
Every node runs on the one machine, so network latency is close to zero and
these figures are a ceiling rather than a prediction for a real network.

**Method.** Closed loop: each client sends one request, waits for the reply,
and sends the next. That measures latency honestly and throughput
conservatively. An open loop generator would report bigger throughput numbers
by queueing work the cluster has not agreed to, which describes the queue
rather than the cluster. Every run has a warmup period that is excluded, so no
election or cold cache is counted.

## Three nodes, local processes

16 clients, 128 byte values, 1000 keys, 20 seconds measured.

| Workload | Throughput | mean | p50 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- |
| Write | 126 ops/s | 127.5ms | 133.7ms | 176.9ms | 204.1ms |
| Read | 7,881 ops/s | 2.0ms | 2.0ms | 2.7ms | 3.0ms |
| Mixed, 90% read | 1,322 ops/s | 12.1ms | 9.8ms | 36.0ms | 46.3ms |

Reads are 62 times faster than writes. That is the shape you want: a
linearizable read costs one round trip to a majority to confirm leadership and
touches no disk at all, while a write has to reach a majority *and* be made
durable on the way.

## The write ceiling

Write throughput does not improve with concurrency:

| Clients | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| 1 | 125 ops/s | 8.9ms | 10.1ms |
| 2 | 166 ops/s | 12.0ms | 18.1ms |
| 4 | 147 ops/s | 27.5ms | 37.3ms |
| 8 | 157 ops/s | 50.6ms | 81.9ms |
| 16 | 157 ops/s | 102.5ms | 141.2ms |
| 32 | 154 ops/s | 206.6ms | 282.3ms |
| 64 | 148 ops/s | 424.3ms | 684.4ms |

Throughput is flat from 1 client to 64 while latency grows linearly with the
client count. That is the signature of a fully serialized resource: every
client added buys queueing and nothing else.

The metrics say exactly what the resource is. Under the 16 client write load
the leader reported:

```
persist calls        2923      mean 7.81 ms
entries persisted    2922      entries per persist call: 1.00
proposals            2921      mean 125.66 ms
```

**One entry per fsync.** The driver's loop takes one proposal per iteration and
then drains a `Ready`, so each write gets its own durable write and its own
fsync. Write throughput is therefore pinned at `1 / fsync`, which here is
`1 / 7.81ms = 128 ops/s`, against 126 measured. The 125ms latency is not the
cluster being slow, it is 16 clients queueing behind a 7.8ms serialized
pipeline.

This is the single biggest performance limitation in the system and it is a
design gap, not a tuning problem. Group commit is the fix: batch the proposals
that arrive while an fsync is in flight and persist them together, which
divides the per-write disk cost by the batch size. It is not implemented.

## Five nodes in Docker

The same model, on a different disk. Docker Desktop runs Linux in a VM, where
an fsync costs 1.10ms rather than the 7.81ms macOS charges for a real
`F_FULLFSYNC` against APFS.

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| Write, 16 clients | 823 ops/s | 19.1ms | 32.2ms |
| Read, 16 clients | 4,440 ops/s | 3.2ms | 6.4ms |

The prediction holds: `1 / 1.10ms = 908 ops/s` against 823 measured, still at
exactly 1.00 entries per persist.

It is worth being clear about what that 6.5 times improvement is. It is not
faster code and it is not a better configuration. It is a cheaper fsync, and it
is cheaper partly because it is weaker: a durability guarantee inside a
virtualized disk is not the same guarantee macOS gives you against power loss.
A benchmark that reported only the larger number would be describing the
storage stack while appearing to describe the database.

Reads are slower here than on three local nodes, which is expected. A read
confirms leadership with a majority, and a majority of five spread across
container networking costs more than a majority of three on loopback.

## Losing nodes

Five nodes, writes throughout:

| Cluster state | Result |
| --- | --- |
| 5 of 5 healthy | 772 ops/s |
| 3 of 5 (minority lost) | 479 ops/s, zero errors |
| 2 of 5 (majority lost) | No leader. Writes and reads both refused, `/ready` returns 503 |

Losing two of five costs throughput without costing correctness or
availability, which is the entire promise of the algorithm. Throughput drops
because a five node cluster can commit as soon as the *fastest* two followers
acknowledge, whereas with three nodes left it needs all of them, so the slowest
one sets the pace every time.

Losing a third node stops the cluster, which is correct rather than a failure.
The `RaftKVNoLeader` alert fired during this window.

## Kubernetes

Five replicas as a StatefulSet on kind, driven through the load balanced client
service so every request lands on an arbitrary node and follows a redirect to
the leader.

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| Write, 8 clients | 793 ops/s | 9.6ms | 17.6ms |

Redirection through a service costs almost nothing, because a redirect is one
extra round trip on a connection that is then reused.

## What the benchmarks found

Two things worth more than the numbers.

**No group commit.** Described above. Write throughput is capped at one fsync
per write, and no amount of client concurrency moves it.

**A pod restart is disruptive out of proportion to the event.** Deleting one
pod of five produced 31 leadership changes and drove the term from 3 to 21
before the cluster settled. A node that restarts campaigns immediately, which
raises the term and deposes a leader that was serving perfectly well. This is
the missing pre-vote optimization from §9.6, and until now it was a known gap
described in the README rather than a measured one.
