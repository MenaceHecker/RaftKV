# RaftKV: design, requirements and guarantees

This is the reference document for the project. The source files carry no prose,
so everything here is the "why" that would otherwise sit above the code: what the
system is for, what it promises, how it is built, and which of its claims are
checked by something other than my word.

Section numbers in the form §5.4.2 refer to the extended version of
*In Search of an Understandable Consensus Algorithm* by Diego Ongaro and John
Ousterhout, which is the specification this implements.

---

## 1. Goals

**Implement Raft from first principles.** No consensus library underneath. The
point of the project is the layer most systems import rather than write, so
importing it would remove the only interesting part.

**Make every safety property falsifiable.** For each property the paper names,
there is a test that goes red when the rule enforcing it is deleted. A test that
passes because the happy path works is not evidence.

**Be honest in public.** Anything this repository claims about itself in prose
should be either checked mechanically or clearly marked as unfinished. Claims
drifting away from the code is the failure mode this project has hit most often,
so several tests exist only to catch it.

**Fail visibly rather than quietly.** Where a choice exists between a wrong
answer and no answer, the system gives no answer. A node that cannot keep its
promises stops and says so.

### Non-goals

Multi-Raft or sharding. A single replication group is the subject.

Geo-distribution. Timeouts assume a datacenter network.

A general-purpose database. The state machine is a key-value map because the
interesting part is the replication, not the data model.

Beating a tuned production store on throughput. The benchmarks exist to find
mistakes in this code, not to win comparisons.

---

## 2. Requirements

### Functional

- Elect exactly one leader per term, and recover a leader after any minority
  failure.
- Replicate a client write to a majority before acknowledging it.
- Serve linearizable reads, meaning a read observes every write that completed
  before it began.
- Survive the crash and restart of any node without losing acknowledged writes.
- Deduplicate a client's retries so a redelivered request is not applied twice.
- Add and remove cluster members while continuing to serve.
- Compact the log so that recovery time and disk use do not grow without bound.
- Catch up a member that has fallen behind the compaction point by transferring
  a state machine image.

### Non-functional

- The consensus core must be deterministic: same inputs in the same order
  produce the same outputs, on every replica and every run.
- No unbounded queue, buffer, message or batch anywhere on a path that a client
  or a peer can drive.
- Every durable write must be reported as durable only once it is.
- Operational state must be observable from outside the process: whether this
  node leads, how far behind it is, whether it can serve.
- The whole thing must build and test with `go test ./...` and no system
  packages.

---

## 3. Architecture

Four layers, each depending only on the one below it.

```
cmd/raftkv-server        process: flags, signals, lifecycle
  └── internal/transport gRPC: peer messages and the client API
        └── internal/node driver: goroutines, timers, the single loop
              └── internal/raft core: pure consensus
                    └── internal/storage durability (via the Storage interface)
```

**`internal/raft` is the consensus core.** It does not import `time`, start a
goroutine, or touch a socket. A node advances when the caller invokes `Tick()`,
receives a message through `Step(msg)`, and surrenders everything it wants done
through `Ready()`. This is the decision the rest of the design follows from, and
section 4 explains what it buys.

**`internal/storage` is durability.** A segmented write-ahead log with
CRC-checked records, a snapshot store with atomic replacement, and a type that
binds the two into the `raft.Storage` interface the core expects. It also holds
an exclusive lock on the data directory for the life of the process.

**`internal/statemachine` is the replicated state.** A key-value map plus the
client session table used for deduplication. It is pure for the same reason the
core is: two replicas applying the same entries must reach byte-identical state.

**`internal/node` is the driver.** It owns the core, the log and the state
machine, and runs the one loop that is allowed to touch them. Everything else
reaches it over channels. All the concurrency in the system lives in this one
file, which is the price paid for the core being pure.

**`internal/transport` is the wire.** A gRPC service for peer traffic, another
for clients, the codec between the wire types and the core's, and a peer
transport that dials members and learns about new ones from configuration
changes.

**`internal/metrics`** is the only package that knows Prometheus exists.
**`internal/determinism`** contains no production code at all; it is the package
that enforces the claims in section 9.

**`chaos`** is the fault-injection harness and the linearizability checker.
**`cmd/raftkv-bench`** is a closed-loop load generator.

---

## 4. The decisions, and what they cost

### A pure consensus core

The core has no clock, no goroutines and no network.

What it buys is that a five node cluster runs inside a single goroutine with a
fake clock, and a failing test fails identically every run. No sleeps, no
polling, no rerunning to see. The chaos harness drives partitions, reordering
and crashes mid-write, and a seed that fails reproduces exactly, which is the
difference between a harness worth debugging and one you learn to ignore.

What it costs is that all the concurrency has to live somewhere, and it lives in
`internal/node`. That loop is the only thing permitted to call into the core.

### Read-index rather than leader leases

A linearizable read has to establish that the node answering it is still the
leader. Leases do that by assuming clocks do not drift more than a bound, and
they are faster because they skip a round trip. Read-index does it by confirming
with a majority, costing one round trip and assuming nothing about clocks.

For a project whose subject is correctness under adversarial conditions, trading
a network assumption for a timing assumption is the wrong direction. The round
trip is also cheaper than it looks, because concurrent reads share one
confirmation round.

### Deduplication in the state machine, not the server

A server-side duplicate check only filters requests arriving at the node that
saw the original. The entry still commits and applies on every other replica, so
the replicas diverge. Divergence is strictly worse than a duplicate, so the
check lives where every replica reaches the same verdict.

### A hand-rolled disk format

The Raft log is the part whose on-disk representation should be explainable byte
by byte, and a fixed little-endian layout is readable in a hex dump when a
record goes bad. Values stored by clients are opaque bytes, which is a different
decision made deliberately: the system does not interpret them, so it does not
need a schema for them.

### Sorted keys in snapshots

Go randomizes map iteration, so an unsorted encoding produces different bytes on
every call for identical state. Sorting makes a snapshot a deterministic
function of the state, which makes two replicas' snapshots directly comparable.
That is the cheapest convergence check available, and the chaos harness uses it.

### `buf` rather than `protoc`

It carries its own protobuf compiler in Go, so the toolchain installs with
`go install` and no system package manager is involved. Generated files are
committed, so a fresh clone builds without any of it.

---

## 5. What the algorithm actually does here

**Elections (§5.2).** Randomized timeouts, one vote per node per term, and the
vote recorded durably before the answer is sent. Pre-vote (§9.6) has a node ask
whether an election would be won before starting one, so a node that restarts or
rejoins does not depose a healthy leader merely by campaigning with a higher
term.

**The election restriction (§5.4.1).** A candidate whose log is behind is
refused, however new its term. Without it a node missing committed entries could
win and overwrite them.

**Replication (§5.3).** AppendEntries with the log matching property. A follower
that disagrees reports the first index of the conflicting term, so the leader
backs up a whole term per round trip rather than one entry at a time.

**Commit advancement (§5.4.2).** An entry is committed once a majority stores it
*and* the entry is from the leader's own term. Replica count alone is not
sufficient: an entry from a previous term can sit on a majority and still be
overwritten, which is Figure 8 in the paper. Every new leader appends a no-op on
election so it has an in-term entry to commit, which transitively secures
everything it inherited.

**Check quorum.** A leader that has not heard from a majority within an election
timeout steps down. Raft is safe without this, but a leader cut off from
everyone otherwise never finds out, so anything routing by leadership keeps
sending traffic to the one node that cannot serve it.

**Linearizable reads (§6.4).** The leader records its commit index, confirms
with a majority that it still leads, then waits until the state machine has
applied through that index before answering. Concurrent reads share one
confirmation round. A read arriving mid-round waits for the next one, because
heartbeats sent before that read existed cannot prove anything about it. A
newly elected leader refuses reads until it has committed an entry in its own
term, since until then its commit index is not known to be current.

**Client sessions (§6.3).** Client ID plus a strictly increasing sequence
number. The state machine remembers the highest sequence applied per client and
ignores anything at or below it. The table is bounded and evicts, because an
unbounded session table is a memory leak and a snapshot that grows forever.

**Snapshots (§7).** Taken when enough entries have accumulated past the last
one, or on demand. The log is compacted behind them. A follower too far behind
to be caught up from the log is sent the image instead, over a streaming RPC in
chunks. The configuration travels with the snapshot, because the configuration
change entries it was derived from are exactly what compaction removed.

**Membership changes (§6).** Joint consensus. A change enters a joint
configuration in which both the old and new majorities must agree, and a second
entry leaves it once the first commits. A leader removed from the configuration
stands down rather than continuing to issue orders to a cluster it has left.

New members' addresses travel in the configuration change, and the transport
learns them from it. This has to happen when the entry is *appended*, not when
it commits: growing a one node cluster produces a joint configuration needing a
majority of both {1} and {1,2}, so the entry admitting node 2 cannot commit
until node 2 answers, and node 2 cannot answer until somebody can reach it.

---

## 6. The statements this project makes

These are the claims. Each one names what checks it.

### Safety

| Statement | Checked by |
|---|---|
| At most one leader per term | asserted continuously by both harnesses, so every test and every chaos scenario checks it |
| A leader never overwrites or deletes its own entries | `TestLeaderNeverOverwritesItsOwnLog` |
| Two logs agreeing at an index agree on everything before it | `TestFollowerWithConflictingLogIsRepaired` |
| A committed entry is present in every future leader's log | `TestCommittedEntrySurvivesLeaderChange` |
| No two replicas apply different entries at the same index | `assertAppliedConsistent`, called throughout |
| An entry is not committed on replica count alone (§5.4.2) | `TestCommitRequiresEntryFromCurrentTerm` |
| A recovered log has no gap in it | `TestALogWithAHoleIsRefused` |
| History is linearizable under faults | the Wing and Gong checker over twenty-three chaos scenarios |

### Durability

| Statement | Checked by |
|---|---|
| A vote is durable before the answer is sent | `TestNoVoteIsAnsweredUntilItIsDurable` |
| A term change is durable before the node acts in that term | `TestAnElectionIsNotStartedIfTheTermCannotBePersisted` |
| An append is durable before it is acknowledged | `TestAnAppendIsNotAcknowledgedUntilItIsDurable` |
| A snapshot is stored before it is acknowledged | `TestASnapshotIsNotAcknowledgedUntilItIsStored` |
| In-memory state never leads the durable state | the "not remembered" tests, one per path |
| Committed entries replay until the caller advances | `TestCommittedEntriesComeBackUntilTheyAreAdvanced` |
| Every core storage write marks its failure distinctly | `TestEveryCoreStorageWriteMarksItsFailure`, over the AST |

### Liveness and availability

| Statement | Checked by |
|---|---|
| A node that cannot write stops rather than continuing | `TestTheLoopStopsWhenTheDiskDies` |
| A leader that cannot append stands down | `TestALeaderThatCannotAppendStandsDown` |
| A stopped node reports itself unhealthy | `TestLivenessFailsOnceTheNodeHasStopped` |
| Liveness does not depend on there being a leader | `TestLivenessDoesNotDependOnALeader` |
| A client is told to retry elsewhere, not that it failed | `TestAStorageFailureIsRetryableRatherThanAFault` |
| A member added at runtime is actually reachable | `TestANodeAddedAtRuntimeReceivesTheLog` |
| A restarted node rediscovers members its flags do not name | `TestARestartedNodeRediscoversARuntimeMember` |
| The process exits cleanly on SIGTERM | `TestTheServerStartsServesAndStopsOnSIGTERM` |

### Determinism

| Statement | Checked by |
|---|---|
| The core and state machine import nothing nondeterministic | `TestPurePackagesImportNothingNondeterministic` |
| Neither starts a goroutine | `TestPurePackagesStartNoGoroutines` |
| Neither creates its own random source | `TestTheCoreNeverCreatesItsOwnRandomSource` |
| A chaos seed reproduces exactly | `TestARunIsReproducible` |

### Documentation

| Statement | Checked by |
|---|---|
| The test and fuzz counts in the README are current | `TestReadmeTestCountIsCurrent` |
| The line counts are within ten percent | `TestReadmeLineCountsAreRoughlyRight` |
| The chaos report's scenario count matches the suite | `TestChaosReportMatchesTheScenarioCount` |
| Every metric named in a dashboard, alert or doc exists | `TestDeployedFilesOnlyNameMetricsThatExist` |
| The Kubernetes manifest keeps its load-bearing settings | the four manifest tests in `deployment_test.go` |
| Every alert rule has a name and an expression | `TestEveryAlertHasAnExpression` |
| No comment defers to a phase that is finished | `TestShippedCodeDoesNotDeferToACompletedPhase` |

---

## 7. Failure model

What the system assumes can go wrong, and what it does.

**A node crashes.** Its log and hard state are on disk. On restart it replays
from the last snapshot, rejoins, and is caught up by the leader. Acknowledged
writes survive because they reached a majority's disk before being
acknowledged.

**The network partitions.** A minority partition cannot elect a leader and
cannot commit. A leader in the minority discovers it through check quorum and
steps down, so nothing keeps routing traffic to it. The majority continues.

**A node falls behind.** It is caught up from the log if the entries still
exist, and by snapshot transfer if they do not.

**A message is lost, delayed, reordered or duplicated.** Raft retransmits, and
every handler is written to be safe against a repeat. The chaos harness injects
all four.

**A disk fails.** The node stops. It does not continue serving with state it
cannot persist, it reports itself unhealthy, the process exits non-zero so an
orchestrator replaces it, and in-flight clients are told to retry elsewhere
rather than that their request failed.

**Two processes open the same data directory.** The second is refused. The
directory is held under an exclusive lock for the life of the process.

**A client retries.** Its sequence number makes the retry a no-op if the
original was applied.

### Not in the model

Byzantine faults. A node is assumed to follow the protocol or to stop.

Disk corruption that preserves a valid CRC. Records are checksummed, which
catches damage but not a deliberate forgery.

Clock correctness. Nothing depends on wall time being accurate or monotonic
across nodes, which is the reason read-index was chosen over leases.

---

## 8. The durability contract

The core calls `Storage` synchronously and treats any error from a write as
fatal, because every safety property above it assumes that state reported as
persisted really is.

There are four write paths: the hard state, a leader's own append, a follower's
append, and installing a snapshot. All four have the same shape. The write
happens first, and the in-memory state is updated only if it succeeded, so the
two can never disagree in the dangerous direction: believing something was
recorded when it was not.

All four wrap their failure in `raft.ErrStorage`. That marker is the whole
decision at the layer above, where the driver stops the node for a failed write
but ignores a malformed message. Those two used to arrive looking identical, and
a node whose disk had died carried on answering nothing, recording nothing and
reporting itself healthy. An AST test now fails the build if a fifth write path
is added without the marker.

Reads are different and deliberately not marked. `Snapshot()` returning
`ErrSnapshotUnavailable` means only that no snapshot exists yet, and treating
that as a device failure would stop every leader that has not taken one. The
other reads are served from memory and cannot fail from hardware at all.

---

## 9. How this is tested

**Deterministic unit tests** drive whole clusters inside one goroutine with a
fake clock. Most of the consensus tests are these.

**Mutation checks.** For any test claiming to enforce a rule, the rule is
deleted and the test is confirmed to go red. Several tests in this project
passed while testing nothing before this was applied consistently, including
the one covering §5.4.2, the most important rule in the codebase. The README
lists the ones that were caught that way.

**Fuzzing.** Eight targets over the decoders. Every input that has ever failed
is committed under `testdata/fuzz`, and each bug found this way also has a
purpose-written regression test, because a corpus entry only catches what the
fuzz body asserts.

**Chaos with a linearizability checker.** Twenty-three scenarios injecting
partitions, crashes, loss, delay, duplication and membership changes, each run
across several seeds, against both in-memory and on-disk storage. Every
operation is recorded with its real time interval and checked by a Wing and
Gong search, split per key and memoized. The checker has its own negative tests,
because a checker that accepts everything makes every scenario vacuous.

**Real-process tests.** The server binary is started, probed over HTTP,
signalled, and restarted on its own data directory, because flag parsing, signal
handling and lock release cannot be tested in-process.

**Benchmarks.** Closed loop, real gRPC, real fsyncs. They have found more real
problems than any other category: an fsync per write that pinned throughput flat
at 150 ops/s regardless of client count, and a missing pre-vote round that cost
31 leadership changes per pod restart.

---

## 10. Operational behaviour

**Configuration** is command-line flags, documented by `--help`. The peer list
is the only one that must agree across nodes at startup.

**`/health`** answers whether the consensus loop is running. It must not depend
on there being a leader: during an election no node has one, and a liveness
probe that checked would fail on every node at once and have the whole cluster
restarted.

**`/ready`** answers whether this node can serve, which means a leader exists. A
node without one is running correctly and cannot answer a linearizable read, so
it should be taken out of the client service rather than sent traffic it will
refuse.

**`/metrics`** exposes the Prometheus registry. The names are listed in
[docs/observability.md](docs/observability.md) and checked against the registry
by a test.

**Shutdown** on SIGINT or SIGTERM stops accepting work, gives in-flight calls a
bounded five seconds, then closes the node's files. The bound exists because a
graceful stop alone is bounded by the *client's* timeout, and measured shutdowns
took 3, 10 and 20 seconds for clients whose timeouts were 3, 10 and 20 seconds.

**Membership** is a runtime operation through `ClusterService`, not a
configuration change. Adding a pod to a StatefulSet means raising `replicas`
*and* calling `AddNode`.

---

## 11. Known limitations

**Snapshots are materialized in memory at both ends.** Allocation is down from
353 MB to 65 MB for a 64 MB image, and chunked transfer means size no longer
breaks the wire, but the sender still holds the whole image and the receiver
still assembles it. Streaming through an `io.Reader` and `io.Writer` would fix
it and would ripple through the storage layer and the core's `Snapshot` type.

**The disk layer's own error returns are untested.** The core's response to a
failed write is tested thoroughly with a storage implementation that fails on
demand. The error paths inside `internal/storage` are not, because reaching them
needs fault injection at the filesystem level and there is no seam for it.

**`main` and `run` do not appear in coverage.** They are exercised end to end by
subprocess tests, which run a separately built binary the coverage tooling
cannot see.

**One replication group.** No sharding, no multi-Raft.

---

## 12. Glossary

**Term** a logical clock that increases on every election. Every message carries
one, and a node seeing a higher term steps down to it.

**Index** a position in the replicated log, starting at 1.

**Commit index** the highest index known to be stored on a majority and safe to
apply.

**Applied index** the highest index this node's state machine has consumed. It
trails the commit index.

**Quorum** a majority of the configuration. During a joint transition, a
majority of *both* configurations.

**Joint configuration** the intermediate state of a membership change, requiring
both the old and new majorities to agree.

**Hard state** the term and vote, which must survive a crash.

**Log matching** the property that two logs agreeing at an index agree on every
entry before it.

**Linearizable** every operation appears to take effect at one instant between
its invocation and its response, consistent with real time.

**Read-index** the protocol for a linearizable read without a disk write.

**Pre-vote** a poll before a real election, so a node cannot disrupt a healthy
leader just by campaigning.

**Check quorum** a leader stepping down when it has not heard from a majority.
