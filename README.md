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

**A gRPC wire protocol.** Defined and generated, with the codec between it and the core fully tested.

---

## The design decision everything else follows from

The consensus core has **no clock, no goroutines, and no network.**

Not "minimal", but none. `internal/raft` doesn't import `time`, doesn't start a goroutine, and never touches a socket. A node advances when you call `Tick()`, receives a message when you call `Step(msg)`, and hands you everything it wants to do (messages to send, entries to apply) from `Ready()`.

This sounds like an inconvenience and it is the single best decision in the project. It means an entire five-node cluster runs inside one goroutine with a fake clock, and a test that fails does so *identically* every time. No sleeps, no polling, no "run it again and see." When I get to the chaos harness in Phase 5, with partitions and message reordering and crashes mid-write, this is what makes those scenarios reproducible instead of a flaky mess I'd learn to ignore.

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
│   └── readonly.go     read-index protocol for linearizable reads (§6.4)
├── storage/        durability
│   ├── record.go       on-disk framing, CRC, torn-vs-corrupt detection
│   ├── wal.go          segmented append-only log
│   ├── snapshot.go     atomic snapshots with fallback recovery
│   └── disk.go         binds WAL + snapshots into a raft.Storage
├── statemachine/   the replicated KV store
│   ├── kv.go           deterministic apply, deterministic snapshots
│   └── session.go      client dedup (§6.3)
├── node/           the driver, where goroutines and real time live
└── transport/      gRPC wire protocol and codec
```

Roughly 5,000 lines of implementation and 6,000 of tests. The ratio is not an accident.

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

- **No chaos harness yet.** This is Phase 5 and it's the centerpiece of the whole project: fault injection, a linearizability checker, and a documented set of adversarial scenarios with pass/fail results. The deterministic core was built specifically to make it possible. It doesn't exist yet.
- **No gRPC server.** The protocol is defined and the codec is tested, but the peer transport and the client-facing service aren't written. Right now the only transport is the in-memory one the tests use.
- **No cluster membership changes.** That's Phase 4. Membership is fixed at startup.
- **No `InstallSnapshot` RPC.** A follower that falls behind a leader's compaction point can't currently be caught up. Lands with Phase 4.
- **Snapshots are held in memory**, capping them at 64 MiB, enforced with a clear error rather than discovered as a corrupt file later. Streaming is the fix.
- **No metrics, no dashboards, no Docker, no Kubernetes.** Phases 5 and 6.
- **No benchmark numbers.** I'm not publishing throughput figures until they're measured on something real. Made-up numbers are worse than no numbers.

---

## Roadmap

- [x] **Phase 1.** Leader election, log replication, commit rules, deterministic test harness
- [x] **Phase 2.** WAL, snapshotting, compaction, crash recovery, KV state machine
- [ ] **Phase 3.** Client API *(read-index ✅, dedup ✅, node driver ✅, wire protocol ✅, gRPC server ⬜)*
- [ ] **Phase 4.** Cluster membership via joint consensus
- [ ] **Phase 5.** Chaos testing, linearizability checking, observability
- [ ] **Phase 6.** Deployment story, writeup, real benchmarks

---

## Running it

Requires Go 1.25+. No system packages needed.

```bash
go test ./...              # everything
go test ./... -race        # with the race detector
go test ./internal/raft/   # just the consensus core (fast, deterministic)
```

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
