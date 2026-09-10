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
| Write | 521 ops/s | 30.7ms | 28.9ms | 68.1ms | 80.8ms |
| Read | 7,332 ops/s | 2.2ms | 2.1ms | 3.6ms | 6.6ms |
| Mixed, 90% read | 1,616 ops/s | 9.9ms | 8.4ms | 28.1ms | 35.4ms |

Reads are 14 times faster than writes. That is the shape you want: a
linearizable read costs one round trip to a majority to confirm leadership and
touches no disk at all, while a write has to reach a majority *and* be made
durable on the way.

## The write ceiling, and removing it

The first version of these benchmarks found write throughput completely flat
with concurrency, which is what prompted the work described in this section.
Both columns below are the same machine and the same test, before and after
group commit:

| Clients | Before | After | Before p50 | After p50 |
| --- | --- | --- | --- | --- |
| 1 | 125 ops/s | 141 ops/s | 8.9ms | 7.1ms |
| 2 | 166 ops/s | 166 ops/s | 12.0ms | 12.0ms |
| 4 | 147 ops/s | 184 ops/s | 27.5ms | 21.3ms |
| 8 | 157 ops/s | 287 ops/s | 50.6ms | 26.5ms |
| 16 | 157 ops/s | 571 ops/s | 102.5ms | 26.4ms |
| 32 | 154 ops/s | 1,101 ops/s | 206.6ms | 28.6ms |
| 64 | 148 ops/s | 2,114 ops/s | 424.3ms | 29.9ms |

**Before.** Throughput was flat from 1 client to 64 while latency grew linearly
with the client count. That is the signature of a fully serialized resource:
every client added bought queueing and nothing else. The metrics named the
resource exactly. Under the 16 client write load the leader reported:

```
persist calls        2923      mean 7.81 ms
entries persisted    2922      entries per persist call: 1.00
proposals            2921      mean 125.66 ms
```

One entry per fsync. The driver's loop took one proposal per iteration and then
drained a `Ready`, so every write got its own durable write. Throughput was
pinned at `1 / fsync`, which was `1 / 7.81ms = 128 ops/s` against 126 measured.
The 125ms latency was not the cluster being slow, it was 16 clients queueing
behind a 7.8ms serialized pipeline.

**After.** The driver now takes every write that is already waiting and appends
them as a single log write. A write cannot be acknowledged until it is durable,
and an fsync costs the same whether it covers one entry or fifty, so the disk
cost per write falls by the size of the batch. The same load now reports 3.71
entries per persist call.

The shape of the curve inverts. Throughput scales with concurrency instead of
staying flat, and latency stays flat instead of growing: at 64 clients it went
from 424ms to 30ms while throughput went from 148 to 2,114 ops/s, a factor of
14. Nothing got faster in absolute terms, the fsync still costs 6.8ms. The
writes simply stopped queueing for a resource they could have shared.

Two details worth keeping in mind. Batching never waits: a batch contains only
writes that had already arrived, so a single client talking to an idle cluster
batches one at a time and pays nothing extra. And the batch is capped, by
default at 64, because the client that starts a batch waits for all of it.

## Five nodes in Docker

The same model, on a different disk. Docker Desktop runs Linux in a VM, where
an fsync costs 1.10ms rather than the 7.81ms macOS charges for a real
`F_FULLFSYNC` against APFS.

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| Write, 16 clients | 2,999 ops/s | 5.3ms | 8.6ms |
| Read, 16 clients | 4,552 ops/s | 3.1ms | 6.4ms |

Before group commit this was 823 ops/s at 19.1ms, and the ceiling model
predicted it precisely: `1 / 1.10ms = 908 ops/s` against 823 measured, at
exactly 1.00 entries per persist. With batching the same cluster reaches 2,999
ops/s at 4.06 entries per persist, and the fsync is unchanged at 1.10ms.

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
| 5 of 5 healthy | 1,603 ops/s |
| 3 of 5 (minority lost) | 1,299 ops/s, zero errors |
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

**No group commit.** Described above. Write throughput was capped at one fsync
per write and no amount of client concurrency moved it. This has since been
fixed, and finding it is the best argument for measuring a system rather than
reasoning about it: every test passed, every scenario held, and the cluster was
quietly doing a disk write per client request.

**A pod restart is disruptive out of proportion to the event.** Deleting one
pod of five produced 31 leadership changes and drove the term from 3 to 21
before the cluster settled. A node that restarts campaigns immediately, which
raises the term and deposes a leader that was serving perfectly well. This is
the missing pre-vote optimization from §9.6, and until now it was a known gap
described in the README rather than a measured one.
