# RaftKV

A distributed key-value store with the Raft consensus algorithm implemented from scratch. No `etcd/raft`, no `hashicorp/raft`, no consensus library underneath.

**Status: in progress.** Phases 1 and 2 are done. Phase 3 is close to completion . Everything below describes what actually exists and passes tests today; the roadmap at the bottom is honest about what doesn't.

```
go test ./...
```

474 tests and eight fuzz targets, all green and clean under `-race`, run on every push by CI.

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

**Group commit.** Writes that are already waiting are appended as one durable log write, so the fsync every write blocks on is paid once per batch instead of once per write. It never blocks waiting for writes that have not arrived, though it does yield once before concluding there are none: whether concurrent writes are actually waiting is the scheduler's decision, and with fewer processors than writers they are usually runnable rather than ready.

**Batched read confirmation.** Concurrent linearizable reads share one leadership confirmation round instead of each sending its own, which took reads from 7,600 to 27,000 a second and cut median latency from 2ms to 0.5ms. A read arriving mid-round waits for the next one, because heartbeats sent before it existed cannot prove anything about it.

**Bounded apply batches.** Committed entries are handed to the state machine a bounded number at a time, because applying shares a goroutine with ticking the clock and reading messages, and a node applying a large backlog is a node that has stopped being a cluster member for the duration.

**Bounded replication messages.** A leader sends a lagging follower its backlog in slices rather than in one message, because how far behind a follower can fall has no limit and every transport has a maximum message size.

**Streamed snapshots.** A state machine image is the one message whose size follows the data rather than the protocol, so it travels over its own streaming RPC in chunks instead of as one message that would fail past the receiver's size limit.

**Check quorum.** A leader that has not heard from a majority within an election timeout steps down. Raft does not require it and is safe without it, but a leader cut off from everyone otherwise never finds out: it keeps advertising itself, so a readiness probe asking whether there is a leader gets yes forever and traffic keeps arriving at the one node that cannot serve it.

**Pre-vote.** A node asks whether an election would be won before starting one (§9.6). Without it, a node that restarts or rejoins deposes a perfectly healthy leader simply by campaigning, because its vote request carries a higher term and everyone must step down to it.

**A node driver.** The thing that owns the consensus core, the WAL, and the state machine, and runs the loop connecting them. Real goroutines, real timers, real recovery on restart.

**A gRPC wire protocol.** Defined and generated, with the codec between it and the core fully tested, plus a server that redirects a client to the leader instead of just refusing it.

**Cluster membership changes.** Joint consensus, so a node can be added or removed while the cluster keeps serving, with both the old and new configurations required to agree during the transition.

**Chaos testing with a linearizability checker.** Partitions, crashes, packet loss, duplication and membership changes driven against real nodes, with every operation recorded and checked against what a single correct machine could have done. Twenty-three scenarios covering membership changes, snapshot transfer, client retries and whole-cluster restarts, run across multiple seeds, and Election Safety checked on every tick.

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

**A sixteen byte snapshot could allocate three gigabytes.** The decoder read a key count, allocated a map that size, and only then discovered the payload was empty. The count arrives from a peer or off a disk, so a corrupt or hostile snapshot small enough to ignore could take a node down. It now has to be consistent with the bytes that follow before anything is sized from it.

**A corrupt entry type decoded as a valid one.** The write-ahead log writes an entry's type in an eight byte field, but the type is a single byte wide, so a damaged field was silently truncated to whatever its low byte happened to be. A record corrupted above that byte decoded as a perfectly ordinary entry of a type it never had. It is now checked against the types that actually exist.

Fuzzing the decoders found that, a three gigabyte allocation, and four more of the same shape in a few minutes of machine time, all of them accepting input the encoder could never produce. `DecodeCommand` ignored trailing bytes, so two byte strings decoded to one command. `Restore` accepted keys in any order and repeated keys, silently keeping whichever copy came last, even though the encoder sorts them precisely so two replicas' snapshots can be compared directly. None of it was a safety violation, and all of it threw away corruption checks that were free.

**Group commit did nothing at all on a single core, and looked fine.** It collected the writes already waiting on an unbuffered channel, with one non-blocking look. Where there are fewer processors than writers the others are runnable but have not reached their send yet, so the look found nobody and every batch was one entry. Tuned on eleven cores, measured on eleven cores, broken on one. CI caught it on a smaller runner, which is the entire reason for having CI rather than a habit of running the tests. The fix yields once before giving up, but only when the batch would otherwise be a single entry: yielding unconditionally, which was the obvious version, cost a third of the write throughput at sixty-four clients. There is now a single-processor run in CI so the next thing like it is caught deliberately.

**The chaos suite crashed nodes hundreds of times against storage that cannot lose anything.** Every run used an in-memory log, so a "crash" discarded a map. Real recovery, reading segments back, checking frames, stitching the log together, was covered by the storage package alone and never by an adversarial crash sequence. Every scenario now runs a second time against the write-ahead log the server actually ships.

Worth stating exactly what that buys, because the obvious claim is wrong and I measured it rather than assuming. Breaking replay so it returns entries out of order is caught. Breaking it so it loses the tail of the log is not, and should not be: an entry that never reached a majority was never committed, and the leader simply sends it again. Losing the entire log on restart is not caught either, because the scenarios restart one node at a time and a majority still holds the data. So the disk runs are mostly an exercise of that code path rather than an independent oracle for it, and the storage tests remain where corruption is detected. What they add is that the path runs at all under hundreds of crash, compaction and membership sequences, which is how the compacted-restart panic was originally found.

**Two tests named for deduplication missed the case it exists for.** Both retried a request that a newer request from the same client had already superseded. Neither retried a client's *most recent* request, which is the one a timeout actually produces: you send something, hear nothing, send it again. A dedup rule comparing sequence numbers with the wrong strictness rejects the first kind and lets the second straight through, so both tests passed against a broken implementation. I found it by breaking dedup deliberately and noticing which suites stayed green.

The other half of the same gap was that every write in the chaos suite carried a fresh sequence number, so no retry was ever modelled there at all. A duplicate write is invisible by itself, since writing a key twice leaves the same value; it only becomes observable when another client writes that key in between and the stale duplicate discards their write.

**The chaos harness was quietly delivering messages to dead processes.** A node that crashed had its in-flight messages dropped, but anything sent to it while it was down stayed queued and was handed to the process that replaced it. Real machines do not work that way: connections die with the process.

That sounds like a detail and it hid an entire subsystem. A message queued before the cluster compacted still carries entries no node holds any more, so a restarting node kept catching up from a log that no longer existed and never needed a snapshot. Finding it took a long chain of wrong guesses, and what finally settled it was hooking the storage write itself rather than reasoning about what should have been sent. A second gap turned up immediately behind it: the harness rebuilt a restarted node's state machine by replaying the log, which stops being enough once the node has compacted and the entries before its own snapshot are gone.

**A leader that removed itself from the cluster crashed the process.** Finishing the transition dereferenced the leader's own replication progress, which adopting the new configuration had just deleted, because the leader was no longer a member of it. A nil pointer, in the consensus core, on an operation an operator would reasonably run.

Nothing in the existing tests could have found it. Membership changes were covered by deterministic unit tests on a healthy cluster, and the chaos suite, which is the thing built to break the algorithm, had ten scenarios and not one of them changed the membership. The bug appeared on the first run of the first scenario that did. It also needed a second fix beyond not crashing: a leader that is no longer a member cannot count itself towards a majority, so it has to stand down rather than keep issuing orders to a cluster it has left.

**A restarting node went off the air for half an election timeout.** Applying committed entries happens on the same goroutine that ticks the clock and reads messages, and the batch was unbounded, so a 20,000 entry replay applied in one go and blocked the loop for 478ms. During that the node could not send a heartbeat, answer one, or even count its own election timer. On a leader with a larger log that is an availability outage, and nothing about it would look like an apply problem.

Two of my attempts to test the fix were worthless and I nearly kept them. The fix has two halves: cap the batch, and come back for the remainder without waiting. The second half only matters when nothing else wakes the loop, and my tests polled the node's status every two milliseconds, which sends on a channel the loop selects on. The test was supplying the very wake-up it was meant to prove unnecessary, and passed identically with the mechanism removed. Reading the applied index once, at the end, tells the real story: 2002 of 2001 entries applied with it, 200 without.

**Ordinary replication had the same 4 MiB cliff, and I only looked because the snapshot bug had just taught me to.** A leader sent a lagging follower every entry it was missing in a single message, with no bound on how many that could be. A node offline for a couple of minutes came back, was sent a message too large to deliver, and sat at commit 0 forever. The fix has a tail: the first version followed up with the next slice whenever a follower was behind, which under load is always, and cost 25% of write throughput. Following up only when the budget actually held entries back recovers it.

**A cluster with more than 4 MiB of data could never catch up a lagging follower.** Snapshots went over gRPC as a single message, and gRPC's default receive limit is 4 MiB. Every test passed, because every test had a state machine measured in kilobytes. The symptom was about as unhelpful as symptoms get: the follower sat at commit 0 forever while the leader moved on, with nothing anywhere saying "too big". I found it by writing the same snapshot-transfer test again with 8 MiB of data instead of a few hundred bytes. The README had also claimed snapshots were capped at 64 MiB "with a clear error", which was wrong twice over: that number bounds individual length prefixes inside the encoder, not the image, and the real failure was silent.

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
├── metrics/        Prometheus collectors, the only package that knows them
└── determinism/    enforces that the core and state machine stay pure

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

Roughly 12,250 lines of implementation and 17,021 of tests, across 474 tests. The ratio is not an accident.

---

## Raft's five safety properties, and where they're tested

The paper names five. Here's what covers each:

| Property | Test |
|---|---|
| Election Safety | asserted continuously by both harnesses, so *every* test and *every* chaos scenario checks it |
| Leader Append-Only | `TestLeaderNeverOverwritesItsOwnLog` |
| Log Matching | `TestFollowerWithConflictingLogIsRepaired` |
| Leader Completeness | `TestCommittedEntrySurvivesLeaderChange` |
| State Machine Safety | `assertAppliedConsistent`, called throughout |

Plus the one that isn't in that list but should be: `TestCommitRequiresEntryFromCurrentTerm`, for §5.4.2.

The core's usage contract has two runnable examples rather than only prose. Go compiles them and compares their printed output, so an example that stopped describing the code fails the suite: breaking a sole voter's ability to commit its own proposals makes one of them fail with a diff. They are the answer to "how do I drive this thing", in a form that cannot quietly stop being true.

The numbers in this README are checked too. They had drifted twice, once claiming 177 tests in one paragraph and 367 in another when there were 434, so `internal/determinism` counts the tests, the fuzz targets and the lines and fails if the text disagrees. A document that is confidently wrong about something checkable invites doubt about the parts that are harder to check.

The property underneath all of them is that the consensus core and the state machine are pure: no clock, no network, no goroutines, and no randomness a seed cannot reproduce. Everything above depends on it, and a plausible one-line fix breaks it without failing anything, so `internal/determinism` parses both packages and enforces it rather than trusting the comments that claim it.

---

## Things I decided on purpose

**Hand-rolled disk format, not protobuf or gob.** The Raft log is the part whose on-disk representation I should be able to explain byte by byte. A fixed little-endian layout is also debuggable with a hex dump when a record goes bad. (The KV *values* are opaque bytes, which is a different call, made deliberately.)

**Read-index rather than leader leases.** Leases are faster, with no round trip, but they buy that by assuming clocks don't drift more than some bound. Read-index costs one round trip and assumes nothing about clocks. For a project about correctness under adversarial conditions, trading a network assumption for a timing assumption is the wrong direction. The tradeoff is written up in `readonly.go`.

**Deduplication lives in the state machine, not the server.** A server-side check only filters duplicates arriving at the node that saw the original. The entry still commits and applies everywhere else, and the replicas **diverge**, which is strictly worse than a duplicate.

**Sorted keys in snapshots.** Go randomizes map iteration, so an unsorted encoding produces different bytes every time for identical state. Sorting makes a snapshot a deterministic function of the state, which makes two replicas' snapshots directly comparable. That's the cheapest possible convergence check, and something the chaos harness will lean on.

**`buf` instead of `protoc`.** It carries its own protobuf compiler in Go, so the whole toolchain installs with `go install` and nobody needs a system package manager to build this.

---

## Things that are honestly not done

- **Snapshots are still materialized in memory** at both ends, though far less wastefully than they were: sizing the buffer exactly took producing a 64 MB snapshot from 353 MB of allocation down to 65 MB, leaving one copy rather than six. They now travel over the wire in chunks, so size no longer breaks transfer, but the sender holds the whole image and the receiver assembles the whole image before handing it over. Making the state machine serialize and restore through an `io.Reader` and `io.Writer` would remove that, and would ripple through the storage layer and the core's `Snapshot` type.

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
| Write | 564 ops/s | 27.9ms | 47.3ms |
| Read | 27,226 ops/s | 0.5ms | 1.2ms |
| Mixed, 90% read | 1,585 ops/s | 8.9ms | 28.7ms |

Five nodes in Docker, where an fsync costs 1.10ms instead of 6.8ms:

| Workload | Throughput | p50 | p99 |
| --- | --- | --- | --- |
| Write | 2,999 ops/s | 5.3ms | 8.6ms |
| Read | 4,552 ops/s | 3.1ms | 6.4ms |

Reads are much faster than writes because a linearizable read costs one round trip to a majority and touches no disk. The Docker write numbers are larger only because that fsync is cheaper and weaker, not because the code is faster, and a benchmark that reported the bigger number alone would be describing the storage stack while pretending to describe the database.

Losing two nodes of five costs throughput and nothing else. Losing a third stops the cluster, which is correct.

The benchmarks also found that a single pod restart cost 31 leadership changes and drove the term from 3 to 21. That was the missing pre-vote round, and with it a node can now be restarted repeatedly without the cluster noticing: across three full restart cycles the term, the leader, and the leadership-change count all stayed exactly where they were.

The benchmarks earned their keep by finding that every write was getting its own fsync, so write throughput was flat at ~150 ops/s from 1 client to 64 no matter what. Batching the writes that are already waiting into one durable append took 64 clients from 148 ops/s at 424ms to 2,284 ops/s at 27ms, a factor of 15 on both. Every test passed before that fix and every chaos scenario held, which is the argument for measuring a system rather than reasoning about it.

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
