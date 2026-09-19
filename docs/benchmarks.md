# Benchmarks

Every number here was measured on a running cluster with `cmd/raftkv-bench`,
over real gRPC, with real fsyncs underneath. Nothing is extrapolated and
nothing is a microbenchmark of a function call. The methodology and the
hardware are written down so the numbers can be argued with, which is the only
thing that makes them worth publishing.

**Hardware.** Apple M3 Pro, 11 cores, 18 GB, macOS 26.6.2, Go 1.25.6.
Every node runs on the one machine, so network latency is close to zero and
these figures are a ceiling rather than a prediction for a real network.

**The instrument.** `cmd/raftkv-bench` produced every figure here, so its
arithmetic is tested rather than assumed. It had no tests until that was
noticed, and it was computing percentiles as floor(f*n) rather than nearest
rank, which is one sample too high whenever f*n lands on a whole number. At
these sample sizes, tens of thousands of operations, that moves a percentile
by less than a microsecond and changes nothing quoted below. It is fixed
because an instrument nobody has calibrated is one nobody can argue with.

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
| Write | 564 ops/s | 28.3ms | 27.9ms | 47.3ms | 56.9ms |
| Read | 27,226 ops/s | 0.6ms | 0.5ms | 1.2ms | 2.2ms |
| Mixed, 90% read | 1,585 ops/s | 10.1ms | 8.9ms | 28.7ms | 35.2ms |

Reads are roughly fifty times faster than writes. That is the shape you want: a
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
| 1 | 125 ops/s | 151 ops/s | 8.9ms | 6.3ms |
| 2 | 166 ops/s | 152 ops/s | 12.0ms | 13.0ms |
| 4 | 147 ops/s | 156 ops/s | 27.5ms | 25.9ms |
| 8 | 157 ops/s | 321 ops/s | 50.6ms | 24.9ms |
| 16 | 157 ops/s | 564 ops/s | 102.5ms | 27.9ms |
| 32 | 154 ops/s | 1,143 ops/s | 206.6ms | 27.1ms |
| 64 | 148 ops/s | 2,284 ops/s | 424.3ms | 27.5ms |

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
from 424ms to 27ms while throughput went from 148 to 2,284 ops/s, a factor of
15. Nothing got faster in absolute terms, the fsync still costs 6.8ms. The
writes simply stopped queueing for a resource they could have shared.

Two details worth keeping in mind. Batching never waits: a batch contains only
writes that had already arrived, so a single client talking to an idle cluster
batches one at a time and pays nothing extra. And the batch is capped, by
default at 64, because the client that starts a batch waits for all of it.

## Reads were paying for a broadcast each

The read-index protocol confirms leadership with a round of heartbeats before
answering, and the first version sent one such round per read. Measured on the
leader that came to 2.03 peer messages per read on three nodes: exactly one
heartbeat to each follower, every time.

Only one round is needed at a time. A read arriving while a round is in flight
cannot be confirmed by it, since those heartbeats went out before the read
existed, but it can wait and be covered by the next. Sharing rounds costs a
queued read part of a round trip and saves a broadcast. Both figures below were
measured back to back on the same machine:

| | Before | After |
| --- | --- | --- |
| Read, 16 clients | 7,615 ops/s at 2.1ms | 27,226 ops/s at 0.5ms |
| Read, 64 clients | not measured | 56,997 ops/s at 1.1ms |
| Messages per read | 2.03 | 0.39 at 16 clients, 0.15 at 64 |

Latency improving alongside throughput is the interesting part, because
queuing behind a round should make an individual read slower, and it does. It
is swamped by what the broadcasts were costing. Those messages were not
saturating the network; they were saturating the single loop that also ticks
the clock, replicates entries and applies them. Removing most of them made
everything that loop does faster.

Writes are unaffected at 575 ops/s and the mixed workload is unchanged within
noise, since both are bounded by the fsync rather than by message handling.

## Message size is its own limit

Throughput and latency are the obvious things to measure, and they miss an
entire class of failure. Two messages in this system grow with the data rather
than with the protocol, and both of them worked perfectly until the data was
large enough that they did not.

A snapshot was the first. It crossed gRPC as a single message, so once a state
machine passed the receiver's four megabyte default, a follower that needed one
could never be caught up. It now streams in chunks.

AppendEntries was the second, and worse, because it is the ordinary path rather
than the recovery path. The leader sent a follower every entry it was missing
in one message, and how far behind a follower can fall has no bound at all. A
node offline for a few minutes would come back, be sent a message too large to
deliver, and sit there forever. Entries now go out in bounded slices.

Neither showed up in any latency figure. Both showed up immediately in a test
that used eight megabytes of data instead of a few hundred bytes.

There is a third message-shaped limit that is not a message at all. Committed
entries are applied on the same goroutine that ticks the clock and reads
incoming messages, and that batch was unbounded too. A 20,000 entry replay
applied in a single pass and blocked the loop for 478ms: half a default
election timeout during which the node cannot send a heartbeat, answer one, or
notice its own timer. Capping the batch at a thousand entries brings the
longest single apply to 29ms while leaving total replay time unchanged at
about 460ms, so the work is the same and the node stays reachable while it
happens.

Bounding the append has a cost worth recording. The first version followed up
with the next slice whenever a follower was still behind, which under load is
almost always, and that cost about 25% of write throughput at 64 clients by
adding a message per acknowledgement. Following up only when the size budget
actually held entries back recovers all of it, because a follower that is one
entry behind a busy leader is about to receive that entry anyway.

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

## Snapshots cost more memory than they should have

Compaction is the operation a node performs while it is already busy, so what
it costs in memory matters. Measured on a store of sixteen thousand four
kilobyte values, about 66 MB resident:

| | Before | After |
| --- | --- | --- |
| Allocated to produce a 64.5 MB snapshot | 352.6 MB | 64.8 MB |
| Peak heap during the snapshot | 194.1 MB (2.9x the store) | 131.0 MB (2.0x) |

The buffer was sized from a guess of thirty-two bytes per key and value pair,
which is about right for tiny values and wrong by two orders of magnitude for
real ones. A 64 MB snapshot started from a 0.5 MB buffer and doubled seven
times, copying itself on each one. The store already knows the exact size, so
it computes it and allocates once.

The 2.0x that remains is the store plus the snapshot of it, which is inherent
until the state machine can serialize straight to the file rather than to a
buffer first. That is the one structural limitation still documented rather
than fixed, and the measurement above is what says how much of it is left:
a single extra copy, rather than the six that were there.

## What fsync costs

The write path is bounded by making the log durable, so the obvious question is
what the guarantee is worth. Measured on the same three node cluster, 16
clients:

| | Throughput | p50 | Durable write |
| --- | --- | --- | --- |
| `--fsync=true` | 598 ops/s | 26.5ms | 6.82ms |
| `--fsync=false` | 2,980 ops/s | 5.4ms | 0.06ms |

Five times the throughput, and the durable write becomes essentially free
because it is no longer durable. That is the trade in its entirety: the flag
does not make the system faster, it makes it stop promising the thing that was
slow. A committed write is only committed because a majority wrote it down, and
without the fsync a power cut takes back writes that clients were told had
succeeded.

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

**A pod restart was disruptive out of proportion to the event.** Deleting one
pod of five produced 31 leadership changes and drove the term from 3 to 21
before the cluster settled. A node that restarts campaigns immediately, which
raises the term and deposes a leader that was serving perfectly well. This was
the missing pre-vote optimization from §9.6, and until it was measured it had
been a gap described in the README rather than a number.

It is now implemented. A node asks whether an election would be won before
starting one, and a follower still hearing from its leader answers no. Three
consecutive restarts of a follower in a three node cluster left the term, the
leader, and the leadership-change counter all unmoved: the restart is
invisible to the cluster rather than costing it an election.
