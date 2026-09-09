# RaftKV

A distributed key-value store with the Raft consensus algorithm implemented from scratch. No `etcd/raft`, no `hashicorp/raft`, no consensus library underneath.

**Status: in progress.** Phases 1 and 2 are done. Phase 3 is most of the way there. Everything below describes what actually exists and passes tests today; the roadmap at the bottom is honest about what doesn't.

```
go test ./...
```

177 tests, all green, and clean under `-race`.

---

## Why this exists

Plenty of portfolio projects show you can build on top of a distributed system. Very few show the layer underneath, which is how a handful of machines agree on a single source of truth when one of them crashes, or when the network splits them in half and both halves think they're in charge.

That's what this is. The Raft paper (Ongaro & Ousterhout) from first principles, with the parts that are easy to skip actually implemented, and the failure modes actually tested rather than described in a comment.

The bar I set for myself: **every safety property in the paper should have a test that fails if I break it.** Not a test that passes because the happy path happens to work, but a test that would go red if I deleted the rule it's checking. More on that below, because it turned out to be harder than it sounds.

---

## What works right now

**Leader election.** Randomized timeouts, one vote per node per term, and the §5.4.1 restriction that stops a node with a stale log from ever winning. Split votes resolve instead of deadlocking.

**Log replication.** AppendEntries with the log matching property, conflict repair that backs up a whole term per round trip rather than one entry at a time, and commit advancement that follows §5.4.2 properly. More on that in a second, because it's the rule most toy implementations get wrong.

**Durable storage.** A hand-rolled write-ahead log in segment files, CRC-checked records, snapshotting with log compaction, and recovery that can tell the difference between "this file was cut off mid-write by a `kill -9`" and "this file is actually corrupt." Those two need different answers, and conflating them costs you data.

**Linearizable reads.** The read-index protocol. A leader confirms with a majority that it is *still* the leader before answering, so a partitioned-off leader can't serve you stale data while believing everything is fine.

**Client deduplication.** Client ID plus sequence number, checked inside the state machine so every replica reaches the same verdict.

**A node driver.** The thing that owns the consensus core, the WAL, and the state machine, and runs the loop connecting them. Real goroutines, real timers, real recovery on restart.

**A gRPC wire protocol.** Defined and generated, with the codec between it and the core fully tested, plus a server that redirects a client to the leader instead of just refusing it.

**Cluster membership changes.** Joint consensus, so a node can be added or removed while the cluster keeps serving, with both the old and new configurations required to agree during the transition.

**Chaos testing with a linearizability checker.** Partitions, crashes, packet loss and duplication driven against real nodes, with every operation recorded and checked against what a single correct machine could have done. Ten scenarios, run across multiple seeds.

**Observability.** Prometheus metrics, health and readiness endpoints, a Grafana dashboard and alert rules. Details in [docs/observability.md](docs/observability.md).

**A deployment story.** A multi-stage Docker image that runs the tests during the build, a five node compose stack with Prometheus and Grafana already wired up, and Kubernetes manifests verified against a real cluster. Details in [docs/deployment.md](docs/deployment.md).

---

## The design decision everything else follows from

The consensus core has **no clock, no goroutines, and no network.**

Not "minimal", but none. `internal/raft` doesn't import `time`, doesn't start a goroutine, and never touches a socket. A node advances when you call `Tick()`, receives a message when you call `Step(msg)`, and hands you everything it wants to do (messages to send, entries to apply) from `Ready()`.

This sounds like an inconvenience and it is the single best decision in the project. It means an entire five-node cluster runs inside one goroutine with a fake clock, and a test that fails does so *identically* every time. No sleeps, no polling, no "run it again and see." In the chaos harness, with partitions and message reordering and crashes mid-write, this is what makes those scenarios reproducible instead of a flaky mess I'd learn to ignore.

The cost is real. All the concurrency has to live somewhere, and it lives in exactly one place: a single loop in `internal/node` that owns the core and is the only thing allowed to touch it. Everything else talks to it over channels. One file to review when something's racy.

---

## The rule everyone gets wrong

This one, in `maybeCommit`:

```go
t, err := n.log.term(candidate)
if err != nil || t != n.term {
    return false
}
```

Raft says an entry is committed once a majority stores it. That is *not sufficient*, and §5.4.2 of the paper exists to explain why. An entry from a previous term can sit on a majority of nodes and **still get overwritten** by a future leader. If you commit it on replica count alone, two state machines diverge and nothing anywhere reports a problem. It's Figure 8 in the paper, and it's the difference between a Raft implementation and something that looks like one.

The entry only becomes safe once something from the leader's *own* term commits on top of it. That's why every new leader appends a no-op the moment it's elected: it gives the leader an in-term entry to commit immediately, which transitively secures everything inherited.

I wrote a test for this. **It passed, and it was worthless.** It only checked that a leader commits its own-term entry, and never built the dangerous case at all. A green test on the most important rule in the codebase, verifying nothing. I threw it out and wrote one that constructs the Figure 8 setup directly, then deleted the guard to confirm the test actually goes red. It does, and it's the only test that does.

That happened three more times over the project. It's the thing I'd most want someone to take from this repo: **a passing test is not evidence until you've watched it fail.**

---

## Bugs the tests actually caught

Not a highlight reel. These are real, and they're the reason the test discipline was worth it.

**A single-node cluster could never commit anything.** `becomeLeader` appends its no-op and broadcasts, but with no peers there is nothing to broadcast and no response ever arrives to advance the commit index. So the node became leader and then sat there, unable to commit its own entry. Present since Phase 1, invisible because the test only checked that it became leader.

**On restart with a compacted log, the node panicked.** The log's committed and applied cursors started at zero, but the storage had been compacted past that point, so the first `Ready()` went looking for entries that no longer existed. What makes this one interesting is that **neither package's tests could have found it.** The Raft tests use in-memory storage that is never compacted; the storage tests never run the consensus core. It took both layers running together.

**Compaction could delete your vote.** Hard state records live in WAL segments interleaved with log entries, so deleting an old segment could take the most recent vote with it, and a node that forgets its vote can vote twice in one term and elect two leaders. Found while writing the WAL rather than by a test, which is its own kind of luck.

**Three tests that passed while testing nothing.** The §5.4.2 one above, plus three compaction tests that were "passing" while truncating zero segments. My test setup batched appends, so everything landed in one file and nothing ever rolled over.

**The chaos suite had a blind spot exactly where it mattered most.** Nine scenarios, all passing. So I deliberately broke the read-index protocol, letting a leader answer reads without confirming with a majority that it was still the leader. Every scenario still passed. The harness routed each read to whichever live node had the highest term, so after a partition the client was quietly steered to the *new* leader and never touched the stale one. A real client does the opposite: it remembers an address and keeps using it until something redirects it. Clients can now target a specific node, and the scenario that does so catches the injected bug immediately. A chaos suite you have not tried to fool is a chaos suite you should not trust.

---

## Layout

```
internal/
├── raft/           consensus core: no clock, no goroutines, no network
│   ├── types.go        NodeID/Term/Index, Entry, the one flat Message envelope
│   ├── storage.go      the Storage interface + an in-memory implementation
│   ├── log.go          log matching, up-to-date comparison, conflict resolution
│   ├── node.go         state transitions, Tick/Step/Ready
│   ├── election.go     RequestVote (§5.2, §5.4.1)
│   ├── replication.go  AppendEntries and commit advancement (§5.3, §5.4.2)
│   ├── readonly.go     read-index protocol for linearizable reads (§6.4)
│   ├── membership.go   configurations and double majorities (§6)
│   ├── confchange.go   joint consensus transitions
│   └── snapshot.go     InstallSnapshot past the compaction point (§7)
├── storage/        durability
│   ├── record.go       on-disk framing, CRC, torn-vs-corrupt detection
│   ├── wal.go          segmented append-only log
│   ├── snapshot.go     atomic snapshots with fallback recovery
│   └── disk.go         binds WAL + snapshots into a raft.Storage
├── statemachine/   the replicated KV store
│   ├── kv.go           deterministic apply, deterministic snapshots
│   └── session.go      client dedup (§6.3)
├── node/           the driver, where goroutines and real time live
├── transport/      gRPC wire protocol, peer transport, KV server
└── metrics/        Prometheus collectors, the only package that knows them

chaos/              fault injection and the linearizability checker
├── network.go          partitions, loss, delay, duplication on a virtual clock
├── cluster.go          real Raft nodes behind that network
├── checker.go          Wing and Gong, split per key, memoized
└── scenario.go         the scenarios and the report they generate

cmd/
├── raftkv-server/      the node binary
└── raftkv-bench/       closed-loop load generator
deploy/
├── prometheus.yml      scrape config, alert rules, Grafana dashboard
├── grafana/            dashboard JSON and provisioning
├── compose/            compose-specific Prometheus config
└── kubernetes/         StatefulSet, services, PodDisruptionBudget
docs/               chaos report, observability, benchmarks, deployment
```

Roughly 10,000 lines of implementation and 12,000 of tests, across 367 tests. The ratio is not an accident.

---

## Raft's five safety properties, and where they're tested

The paper names five. Here's what covers each:

| Property | Test |
|---|---|
| Election Safety | asserted continuously by the harness, so *every* test checks it |
| Leader Append-Only | `TestLeaderNeverOverwritesItsOwnLog` |
| Log Matching | `TestFollowerWithConflictingLogIsRepaired` |
| Leader Completeness | `TestCommittedEntrySurvivesLeaderChange` |
| State Machine Safety | `assertAppliedConsistent`, called throughout |

Plus the one that isn't in that list but should be: `TestCommitRequiresEntryFromCurrentTerm`, for §5.4.2.

---

## Things I decided on purpose

**Hand-rolled disk format, not protobuf or gob.** The Raft log is the part whose on-disk representation I should be able to explain byte by byte. A fixed little-endian layout is also debuggable with a hex dump when a record goes bad. (The KV *values* are opaque bytes, which is a different call, made deliberately.)

**Read-index rather than leader leases.** Leases are faster, with no round trip, but they buy that by assuming clocks don't drift more than some bound. Read-index costs one round trip and assumes nothing about clocks. For a project about correctness under adversarial conditions, trading a network assumption for a timing assumption is the wrong direction. The tradeoff is written up in `readonly.go`.

**Deduplication lives in the state machine, not the server.** A server-side check only filters duplicates arriving at the node that saw the original. The entry still commits and applies everywhere else, and the replicas **diverge**, which is strictly worse than a duplicate.

**Sorted keys in snapshots.** Go randomizes map iteration, so an unsorted encoding produces different bytes every time for identical state. Sorting makes a snapshot a deterministic function of the state, which makes two replicas' snapshots directly comparable. That's the cheapest possible convergence check, and something the chaos harness will lean on.

**`buf` instead of `protoc`.** It carries its own protobuf compiler in Go, so the whole toolchain installs with `go install` and nobody needs a system package manager to build this.

---

## Things that are honestly not done

- **No group commit.** This is the big one, and the benchmarks found it. The driver takes one proposal per loop iteration, so every write gets its own fsync and write throughput is pinned at `1 / fsync` no matter how many clients you add. Measured at 126 ops/s against a 7.81ms fsync, with throughput completely flat from 1 client to 64. Batching proposals that arrive while an fsync is in flight would divide the per-write disk cost by the batch size.
- **No pre-vote.** A node that restarts campaigns immediately, bumping the term and deposing a leader that was serving perfectly well. Deleting one pod of five produced 31 leadership changes and drove the term from 3 to 21 before it settled. §9.6 describes the fix and it is not implemented.
- **Snapshots are held in memory**, capping them at 64 MiB, enforced with a clear error rather than discovered as a corrupt file later. Streaming is the fix.

---

## Roadmap

- [x] **Phase 1.** Leader election, log replication, commit rules, deterministic test harness
- [x] **Phase 2.** WAL, snapshotting, compaction, crash recovery, KV state machine
- [x] **Phase 3.** Client API: read-index, dedup, node driver, wire protocol, gRPC server
- [x] **Phase 4.** Cluster membership via joint consensus
- [x] **Phase 5.** Chaos testing, linearizability checking, observability
- [x] **Phase 6.** Deployment story, writeup, real benchmarks

---

## Numbers

Measured with `cmd/raftkv-bench` against running clusters, closed loop, real gRPC, real fsyncs. Full methodology and hardware in [docs/benchmarks.md](docs/benchmarks.md).

Three nodes as local processes on an M3 Pro, 16 clients:

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| Write | 126 ops/s | 133.7ms | 176.9ms |
| Read | 7,881 ops/s | 2.0ms | 2.7ms |
| Mixed, 90% read | 1,322 ops/s | 9.8ms | 36.0ms |

Five nodes in Docker, where an fsync costs 1.10ms instead of 7.81ms:

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| Write | 823 ops/s | 19.1ms | 32.2ms |
| Read | 4,440 ops/s | 3.2ms | 6.4ms |

Reads are dramatically faster than writes because a linearizable read costs one round trip to a majority and touches no disk. Writes are capped at one fsync each, which is the missing group commit described above. The Docker numbers are larger only because that fsync is cheaper and weaker, not because the code is faster, and a benchmark that reported the bigger number alone would be describing the storage stack while pretending to describe the database.

Losing two nodes of five costs throughput and nothing else. Losing a third stops the cluster, which is correct.

---

## Running it

Requires Go 1.25+. No system packages needed.

```bash
go test ./...              # everything
go test ./... -race        # with the race detector
go test ./internal/raft/   # just the consensus core (fast, deterministic)
go test ./chaos/           # fault injection and linearizability checking
```

A three node cluster on one machine, each node given the same peer list
including itself:

```bash
go build -o bin/raftkv-server ./cmd/raftkv-server

PEERS=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003
for i in 1 2 3; do
  bin/raftkv-server --id $i --peers $PEERS \
    --data-dir /tmp/raftkv-$i --metrics-listen 127.0.0.1:910$i &
done

curl -s 127.0.0.1:9101/health
curl -s 127.0.0.1:9101/metrics | grep raftkv_is_leader
```

Nodes may be started in any order. One that comes up first will campaign, fail
to reach a majority, and keep trying until the others appear.

Or the whole thing in containers, with Prometheus and Grafana already wired up:

```bash
docker compose up --build
open http://localhost:3000
```

On Kubernetes:

```bash
kubectl apply -f deploy/kubernetes/raftkv.yaml
```

Both are covered in [docs/deployment.md](docs/deployment.md), including the two
settings that will otherwise deadlock a StatefulSet on first start.

To regenerate the protobuf code after editing the `.proto`:

```bash
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
buf generate
```

Generated files are committed, so a fresh clone builds without any of that.

---

## Reference

Diego Ongaro and John Ousterhout, *[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)* (extended version). Section references throughout the code point at this paper.
