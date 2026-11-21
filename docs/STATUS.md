# RaftKV: current status

Refreshed at `origin/main` `73915fa` (2026-11-15). The file began as the Task 0 audit at commit `ddff529` and was patched
piecemeal after that; this refresh re-grounds it. **How to read the verdicts:** they come from reading the code and task cards at
`73915fa`, plus the test runs listed in section 2a and the targeted runs named in section 2b. Where a row was carried over from the
audit without a re-read, it says "carried". Line numbers from `ddff529` no longer match the code, so evidence now cites function and
test names (each name was matched with `grep -rn "func TestName"`). Test, fuzz and line counts are not restated here: README.md owns
them and `TestReadmeTestCountIsCurrent` in `internal/determinism/readme_test.go` fails when they are wrong.

Summary: the repo is far ahead of the RUNBOOK's assumptions, and the RUNBOOK's tooling track now exists (Makefile, `.golangci.yml`,
`scripts/`, `.githooks/`, CI, `docs/INVARIANTS.md`, `docs/tasks/`, `cmd/chaosrun`, the mutation suite, the admin API). What is still
missing is the AWS/demo track (gateway, `infra/`, `web/`) and the deferred storage-hardening cards T3.12-T3.15. README.md says
"Status: complete"; that is true of its six phases, not of this list (see the PR body for the conflict).

## 1. Phases (README roadmap)
| README phase | Scope per README | Status | RUNBOOK phase it overlaps |
|---|---|---|---|
| P1 | Election, replication, commit rules, deterministic harness | Done: `election.go`, `replication.go`, `log.go` in `internal/raft` | P1+P2 |
| P2 | WAL, snapshots, compaction, crash recovery, KV state machine | Done: `internal/storage`, `internal/statemachine`; crash-recovery tests on a fault-injecting filesystem (`internal/storage/faultfs`, T3.6-T3.11) | P3, part of P4 |
| P3 | Client API: ReadIndex, dedup, node driver, wire protocol, gRPC | Done: `readonly.go`, `session.go`, `internal/node`, `internal/transport` | part of P4 |
| P4 | Membership via joint consensus | Done: `membership.go`, `confchange.go`; hardened by T3.16, T3.17, T3.18, T3.19 (RUNBOOK lists dynamic membership as a non-goal) | none |
| P5 | Chaos testing, linearizability checking, observability | Done against RUNBOOK P5 except the items in section 6: `chaos/`, `cmd/chaosrun` (T5.6), mutation suite (T5.7), `internal/metrics`, admin API (T4.7) | P5 |
| P6 | Deploy, writeup, benchmarks | Done as Docker, compose, k8s and `docs/benchmarks.md`; AWS (`infra/`), gateway (`cmd/gateway`) and web demo (`web/`) do not exist | P6, D0-D9 |

Task cards in `docs/tasks/` with no implementation on `main` (no commit mentions them in `git log`): T3.12, T3.13, T3.14, T3.15 (storage
hardening, deferred past the demo), D5 and D5b (gateway), and T0.2 (README count tooling, in progress).
Merged cards: T0.1, T3.6 to T3.11, T3.16 to T3.20, T4.7, T5.6, T5.7 (`git log`, section headings below).

## 2. Build and run (from README, `Makefile`, `.github/workflows/ci.yml`)
- `go.mod` says Go 1.25.0. Direct deps: grpc, protobuf, prometheus client, yaml.v3.
- Gates (`Makefile`, targets `fmt vet lint test test-flaky cover proto chaos-smoke`):
  - `make fmt` (gofmt, excluding `*.pb.go`), `make vet`, `make lint` (needs `golangci-lint`; config `.golangci.yml`).
  - `make test` is `go test -race -count=1 -timeout 10m ./...`; `make test-flaky` is `-count=20` over `internal/raft`, `internal/storage`, `internal/statemachine`.
  - `make chaos-smoke` is `go run ./cmd/chaosrun -sim -seeds 1-20` (every scenario, virtual time, no `-duration`).
  - `make proto` needs `buf` (generated code is committed).
- `scripts/guard-tests.sh origin/main` flags added `t.Skip`, net removed assertions and large real-time test durations in the diff; `scripts/review-packet.sh <task>` builds the reviewer packet.
- `.githooks/pre-commit` (enable with `git config core.hooksPath .githooks`): gofmt, `go vet`, `go test -race -short ./...`, `gitleaks protect --staged` when installed, then `scripts/guard-tests.sh origin/main`.
- CI (`ci.yml`, on push to main, on pull requests, and nightly at 04:17 UTC): job `build and test` (gofmt, vet, build, `go test`, `GOMAXPROCS=1`, `-race`),
  job `mutation suite` (`go vet -tags faultinject`, the suite, and a diff check that `docs/CHAOS_RESULTS.md` is current), job `fuzz the decoders` (60s each for
  `FuzzDecodeCommand`, `FuzzRestore`, `FuzzReadRecord`, `FuzzDecodeEntry`, `FuzzDecodeHardState`, `FuzzDecodeSnapshotMeta`, `FuzzDecodeConfChange`, `FuzzMessageFromWire`).
  `FuzzDecodeDiscardSuffix` and `FuzzWALReplayTail` exist but are not in that list. `ci.yml` has no lint, gitleaks, buf or guard-tests job; those run only locally.
- Node: `go build -o bin/raftkv-server ./cmd/raftkv-server`. Flags (`cmd/raftkv-server/main.go`, `defineFlags`): `--id --peers --listen --data-dir --tick --election-ticks --heartbeat-ticks --fsync --snapshot-threshold --log-level --metrics-listen`,
  and the optional `--admin-listen --admin-token-file --admin-chaos` (T4.7) and `--cluster-token-file` (T3.18), all off by default. One shared gRPC `--listen` address
  (peer, KV and cluster services), a separate metrics HTTP address, and an optional separate admin gRPC address.
- Other binaries: `cmd/raftkvctl` (get, put, delete, status over `internal/client`; exit 0 ok, 1 not found, 2 failed or usage, 3 outcome unknown), `cmd/raftkv-bench` (load generator; follows NOT_LEADER redirects), `cmd/chaosrun` (`-sim -seeds A-B -seed N -scenario TEXT -list -json -disk DIR`; exit 0 all passed, 1 a failure, 2 usage).
  Containers: `docker compose up --build`. Kubernetes: `deploy/kubernetes/raftkv.yaml`. Protobuf: `buf generate`.
- Build-tagged mutation switches: `go test -tags faultinject ./chaos -run TestMutationSuite` (section 16). The default binary contains only constant-false guards (`TestDefaultBinaryHasNoMutationMachinery`).
- Not present, still: `cmd/gateway`, `infra/`, `web/`, `docs/NOTES.md`, a porcupine cross-check, a JSONL history with `info` ops, a checker shrinker and an HTML report (each checked with `ls` or `grep` at `73915fa`).
  `CODEOWNERS` and a PR template now exist under `.github/`.

## 2a. Verified baseline (run 2026-11-15 at `73915fa`, Go 1.27.3 darwin/arm64)
`go.mod` still says `go 1.25.0`; the toolchain on the machine that ran this is newer than the one the audit used. Commands, run from a clean worktree of `origin/main`:

| Command | Result |
|---|---|
| `go build ./...` | exit 0 |
| `go vet ./...` | exit 0 |
| `gofmt -l` over all `*.go` except `*.pb.go` | no files listed |
| `golangci-lint run` | `0 issues.` |
| `go test -race -count=1 -timeout 15m ./...` | exit 0; 14 packages `ok`, `internal/transport/raftkv/v1` has no test files |
| `go test -tags faultinject -count=1 ./chaos -run 'TestMutationSuite\|TestControlRunIsClean\|TestMutationTableCoversTheNineSwitches'` | `ok` (4.6s); `docs/CHAOS_RESULTS.md` unchanged |

Race-run package times: chaos 36.1s, cmd/chaosrun 29.0s, cmd/raftkv-server 16.7s, internal/storage 18.3s, internal/transport 12.8s, internal/node 12.0s, internal/raft 11.5s,
internal/statemachine 7.0s, internal/determinism 6.9s, internal/metrics 3.1s, internal/faultinject 1.7s, cmd/raftkv-bench 1.6s, faultfs and fstest about 2s each.
Not run for this refresh: `GOMAXPROCS=1`, `-count=20`, fuzz, `make chaos-smoke`, skips under `-race` (skips were not counted). The earlier baseline (2026-10-23, Go 1.25.6) is superseded.

## 2b. Targeted runs behind the verdicts below
The tests cited in sections 4, 5 and 7 to 16 and in `docs/INVARIANTS.md` were run by name with `go test -count=1 -run '<names>' <package>` at `73915fa`
(the commands and results are listed in the PR body). A cited test that is named but not part of that run says so.

## 3. Tests
Counts: see README.md (generated figure owned by `TestReadmeTestCountIsCurrent`; the exact numbers are not repeated here so they cannot go stale).
Layout: `internal/raft`, `internal/storage` (with `faultfs` and `fstest`), `internal/statemachine`, `internal/node`, `internal/transport`, `internal/metrics`,
`internal/determinism` (repo-wide source checks), `internal/faultinject`, `chaos`, `cmd/raftkv-server`, `cmd/raftkv-bench`, `cmd/chaosrun`.
Chaos: 24 scenarios (`go run ./cmd/chaosrun -sim -list` printed 24 lines; one is the seed-generated "generated nemesis and workload"), run in memory and on disk
(`TestChaosOnDisk`); a further set of scenarios used only by the mutation suite lives in `chaos/mutation_scenarios.go`. `docs/chaos-report.md` records 24 of 24 held.

## 4. RUNBOOK §7.2 checklist
OK = code does it. GAP = differs from the RUNBOOK. UNKNOWN = cannot tell by reading. "Re-read" means the code was read again at `73915fa`;
"carried" means the verdict and evidence come from the `ddff529` audit and were not re-read (their line numbers were dropped). Every test named
here was matched with `grep -rn "func TestName"`; section 2b says which were run.

| # | Item | Verdict | Evidence |
|---|---|---|---|
| 1 | Persist term, vote and log before replying, and before the leader counts its own append | OK (default config) | Carried: the core writes to storage synchronously before queuing replies (`internal/node/node.go` `processReady`, `election.go` `handleVoteRequest` persists before granting, `replication.go`, `log.go`); fsync per write under `SyncAlways`. Re-read: `--fsync=false` selects `SyncNever` (`cmd/raftkv-server/main.go`) and chaos uses it (`chaos/scenario.go`, `Sync: storage.SyncNever`). Since T3.9 a failed write or fsync poisons the WAL and stops the node (section 11); since T3.10 compaction no longer rolls the term and vote back under `SyncNever` (section 12). Tests: `TestAFailedAppendDoesNotAdvanceTheCommitIndex`, `TestSteppingUpATermIsNotRememberedIfItCannotBePersisted` (`internal/raft/durability_test.go`) |
| 2 | Metadata via temp file, fsync, rename, dir fsync | GAP (design differs) | Carried. No metadata file. HardState is a WAL record (`internal/storage/wal.go` `SaveHardState`, `record.go`). Dir fsync on segment create and prefix removal (`wal.go` `roll`, `TruncateBefore`); snapshots use temp, fsync, rename, dir fsync (`snapshot.go`). Equivalent in effect, not as written. Directory creation of the data, wal and snap directories themselves is T3.13, not implemented |
| 3 | term > current: update, clear vote, step down | OK | Re-read: `Node.Step` calls `becomeFollower` when `m.Term > n.term`; carried: it persists before changing state. Test `TestSteppingUpATermIsNotRememberedIfItCannotBePersisted` |
| 4 | term < current: reject and reply with current term | GAP (partial) | Re-read, unchanged: in `Node.Step`, `m.Term < n.term` answers only `MsgVoteRequest` (granted=false, current term); a stale AppendRequest, Heartbeat or InstallSnapshot is dropped silently. Liveness only, not safety. Whether the transport replies on its own: UNKNOWN |
| 5 | RequestVote: one vote per term and up-to-date check | OK | Re-read: `handleVoteRequest` calls `isUpToDate` and persists `(term, From)` before granting. Tests `TestOneVotePerTerm`, `TestVoteIsIdempotentForSameCandidate`, `TestStaleLogCandidateIsRejected` (`election_test.go`). Mutation 4 (grant without the check) is detected by the committed-log monitor (section 16) |
| 6 | Reset election timer only on valid AE, grant, or starting an election | GAP (minor) | Re-read: `becomeFollower` calls `reset()` on any higher-term message, including a vote request that is then rejected; carried: also on `voteLost`; the AppendEntries reset follows the term check, so it is valid; PreVote does not reset. No test |
| 7 | AE rejects on prev mismatch with ConflictTerm and ConflictIndex | OK | Carried: `log.go` `maybeAppend` plus `conflictHint`; `replication.go` back-off. Test `TestFollowerWithConflictingLogIsRepaired` |
| 8 | Truncate only on a real conflict | OK (code and a test) | Carried: `maybeAppend` truncates only at `findConflict`; WAL replay resolves overwrites without deleting. Test `TestStaleDuplicateAppendsDoNotRegressProgress` (`replication_loss_test.go`): 20 late duplicate appends do not regress progress, the follower's commit index never goes backwards, and the logs end identical. It does not assert that the follower's last index never dips mid-run. Mutation 2 (truncate on a stale append) is detected by the committed-log monitor (section 16) |
| 9 | Follower commit = min(leaderCommit, last new entry) | OK | Re-read: `l.commitTo(min(leaderCommit, lastNewIdx))` in `log.go`. Carried: read heartbeats carry no CommitIndex. Related test `TestHeartbeatAdvancesFollowerCommitIndex` |
| 10 | Commit only current-term entries by counting (§5.4.2) | OK | Re-read: `maybeCommit` carries a paper-section 5.4.2 comment. Test `TestCommitRequiresEntryFromCurrentTerm`. Mutation 3 (commit a previous-term entry) is detected by the committed-log monitor (section 16) |
| 11 | No-op on election win | OK | Re-read: `becomeLeader` appends an `EntryNoOp` (paper section 8) unless the `SkipNoop` mutation switch is on. Test `TestNewLeaderAppendsNoOp`. Mutation 7 (skip it) is detected by the read-liveness check |
| 12 | nextIndex and matchIndex init; back up with hints; never below snapshot | OK | Carried: `sendAppend` falls back to `sendSnapshot`; back-off clamps to 1. Tests `TestLeaderFallsBackWhenNoSnapshotExists`, `TestLaggingFollowerIsCaughtUpByASnapshot` |
| 13 | Single apply goroutine, in order, never twice | OK | Carried: one `run` goroutine in `internal/node/node.go` applies; the core panics on out-of-order `appliedTo` (`log.go`). Test `TestCommittedEntriesComeBackUntilTheyAreAdvanced`; the chaos monitor now also asserts a state machine never applies past its node's commit index (mutation 9) |
| 14 | Stale responses ignored | OK (code and tests) | Re-read: lower-term messages are dropped in `Node.Step`; carried: response guards in `election.go` and `replication.go`, match is monotonic. Tests `TestStaleDuplicateAppendsDoNotRegressProgress` (leader `next` and `match` never regress while late duplicates are answered), `TestStaleSnapshotDoesNotRegressAFollower` |
| 15 | Clock, Rand, Transport injected; no wall clock in `internal/raft` | OK in spirit, GAP in letter | Re-read: `internal/raft` is tick-driven and has no `Clock`; `Config.Rand` is a concrete `*rand.Rand` (`raft/node.go`). A `Clock` interface exists in `internal/node/clock.go` (nil means the real clock), and `Transport` is `internal/node/node.go`. Enforced by `TestPurePackagesImportNothingNondeterministic`, `TestPurePackagesStartNoGoroutines`, `TestTheCoreNeverCreatesItsOwnRandomSource` (`internal/determinism/determinism_test.go`). The core now also imports `internal/faultinject`; `TestCallSitesAreGuardedOnly` allows it only as `faultinject.Active(...)`, constant false in the default build |
| 16 | Randomized timeout; heartbeat 100ms, election 600-1200ms | GAP | Re-read: defaults are `DefaultTickInterval` 100ms, `DefaultHeartbeatTick` 1, `DefaultElectionTick` 10 (`internal/node/node.go`); the core randomizes to `electionTick + rand.Intn(electionTick)` (`raft/node.go`), so 10-19 ticks, **1.0-1.9 s**. Heartbeat matches. Flags can override |

## 5. RUNBOOK §7.3 checklist
| # | Item | Verdict | Evidence |
|---|---|---|---|
| 1 | Record `[len u32][crc32c u32][type u8][payload]`, ~64 MB segments, monotonic names | GAP (partial) | Carried: framing matches (`record.go`, Castagnoli). Re-read: default segment is **16 MiB** (`DefaultSegmentSize = 16 << 20`, `wal.go`). Names `<seq>-<firstIndex>.wal`. Record type 4 (`recordDiscardSuffix`) was added by T3.11 (section 7) |
| 2 | Torn tail truncated; mid-log corruption refuses start | OK (code and tests); power-loss durability of the repair: `TestRepairIsDurable`, `TestCrashDuringRepairThenSecondCrash` (`crash_test.go`, T3.8) | Carried: in the last segment a bad CRC, oversize length, zeroed or incomplete tail is truncated, file fsync then dir fsync, counted and logged, but only when no valid record follows the damage; otherwise `ErrCorruptWAL` with the file untouched. Older-segment damage and CRC-valid undecodable records also refuse. Tests `TestBadCRCAtTailIsRepaired`, `TestBadTailIsRepairedAtEveryByte`, `TestDamagedLengthBeforeValidRecordIsRefused`, `TestBadRecordFollowedByValidRecordIsRefused`, `TestCRCValidUndecodableRecordIsRefused` (`wal_tail_test.go`), `FuzzWALReplayTail`, `TestTornTailIsRepaired`, `TestCorruptionInsideSegmentIsRejected`, `TestCorruptionInOlderSegmentIsRejected` (`wal_test.go`). Residual risk: a fsynced record that later rots with nothing valid after it looks like a tear and is dropped. `FuzzWALReplayTail` is not in the CI fuzz job |
| 3 | Truncate-suffix and prefix are crash-safe | OK (code and tests) | Suffix: appended records are resolved at replay, nothing is deleted; T3.11 added a durable discard record for a mismatching snapshot install. Prefix: snapshot and meta record are written first; `TruncateBefore` re-records HardState **and syncs it in every fsync mode before the first delete** (T3.10, section 12). Tests `TestCompactionCrashPointsUnderSyncNever`, `TestCompactionCrashPointsUnderSyncAlways`, `TestCompactionDoesNotRollBackHardStateUnderSyncNever` (`compaction_crash_test.go`), `TestDiscardSurvivesCompactionOrdering` (`discard_record_test.go`), `TestCompactionSurvivesCrash` (`disk_test.go`) |
| 4 | Crash at any write boundary satisfies the invariants, tested with a fault-injecting FS | OK (within the model); T3.12 and T3.13 cover what is still not tested | `internal/storage/faultfs` (in-memory `storage.FS` with separate durable and live bytes, entries and directory creation, `Crash`, `CrashAt`/`CrashAfter`, fault injection). Crash before and after every operation of append, suffix rewrite, snapshot write, compaction, `ApplySnapshot` and tail repair under drop, torn, garbage and zero tails (`crash_test.go`); fsync, short-write and ENOSPC faults, `SyncNever`, 200 random seeds (`crashfault_test.go`). Since T3.9, retry after a failed write or fsync is refused (`poison_test.go`, section 11); since T3.10, `SyncNever` HardState rollback via compaction is closed. Limits: the model is conservative but unchecked against a real kernel; no cross-file reordering, no mid-log bit rot, no cross-directory rename. Not covered (cards exist, not implemented): unsynced segment roll then `WAL.Sync` (T3.12), first-start loss of the data directory (T3.13), oversize entries (T3.14), snapshot retention and a failed purge (T3.15) |

## 6. Other items noted, and what does not exist
- OK (carried): dedupe by `(clientID, seq)` inside the state machine, session table in the snapshot, bounded sessions (`internal/statemachine/kv.go`, `session.go`). NOT_LEADER with a leader address (`internal/transport/kvserver.go` `notLeaderError`; `TestFollowerRedirectsWritesAndReads`). ReadIndex requires an entry committed in the leader's term (`readonly.go`). Snapshots are written atomically (`snapshot.go`); InstallSnapshot streams 1 MiB chunks (`SnapshotChunkSize` in `internal/transport/peer.go`).
- OK (re-read): both InstallSnapshot follower branches. `handleInstallSnapshot` errors if the receiver is a leader of that term, turns a candidate into a follower, acknowledges with its own commit index when `snap.Index <= committed`, and otherwise calls `restore` (`ApplySnapshot`, `rebuildConfig`, pending snapshot). Tests `TestStaleSnapshotDoesNotRegressAFollower` (first branch), `TestLaggingFollowerIsCaughtUpByASnapshot` (second), `TestLeaderRejectsASnapshotInItsOwnTerm`.
- Client library: `internal/client` (T4.6, section 18) follows leader hints and retries with the same `(id, seq)`; `cmd/raftkv-bench` still has its own redirect code (`TestNotLeaderFindsTheRedirect`) and the server side is tested (`TestARedirectIsEnoughToFindTheLeader`). Whether `time.Sleep` masks flakiness: 14 `_test.go` files use it (7 at the audit); the one flake recorded since, `TestFollowerCatchesUpFromALargeBacklog`, had a real cause (section 14), not a sleep.
- Not present: `read_mode: log|readindex` switch (ReadIndex is the only path); snapshots are held fully in memory at both ends (README "Things that are honestly not done").
- Absent from the repo: see section 2 (`cmd/gateway`, `infra/`, `web/`, `docs/NOTES.md`, porcupine cross-check, JSONL history with `info` ops, checker shrinker, HTML report, and CI jobs for lint, gitleaks, buf and guard-tests). Present now and no longer absent: Makefile, `.golangci.yml`, `scripts/`, `.githooks/`, CODEOWNERS, PR template, `docs/INVARIANTS.md`, `docs/tasks/`, a nightly schedule inside `ci.yml`, `cmd/chaosrun`, the admin service, the mutation (`faultinject`) suite.
- Checker note: `chaos/checker.go` has pending and unknown op statuses (`StatusPending`, `StatusUnknown`), so indeterminate ops are modelled; correctness not reviewed.

Findings sections: sections 7 to 10 keep their numbers and order because `docs/RUNBOOK.md` (non-goals) and the T3.19 card cite "section 9" for T3.16.
Sections 7 to 10 are the findings as written when each card merged (T3.11, T3.17, T3.16, T3.19), updated only where a later card changed the picture;
sections 11 to 15 are in card order (T3.9, T3.10, T3.18, T3.20, T4.7), 16 covers the chaos tooling and 17 the known limitations.

## 7. T3.11 findings (branch `feat/t3-11-stale-tail`)
- **Fixed here: stale tail after `ApplySnapshot`.** A snapshot whose last included entry disagrees in term with the local entry at that
  index left the old entries above it in the WAL, and `OpenDiskStorage` brought them back after a restart (paper section 7,
  InstallSnapshot rule 6). Effects shown by red tests before the fix: a restarted voter compared candidates against the stale tail
  instead of the snapshot, so it granted a vote it had refused before the restart (`stale_tail_vote_test.go`), and a stale `ConfChange`
  in the tail changed membership and quorum after a restart (`internal/node/applysnapshot_restart_test.go`, 3e). The fix is a new WAL
  record, `recordDiscardSuffix` (type 4), written together with `SnapshotMeta` in one fsynced write, only on a term mismatch.
  Compatibility: a binary without type 4 refuses such a WAL with `ErrCorruptWAL`; there is no downgrade path.
- **Fixed later by T3.16: leaving the joint config before the joint entry commits.** `ProposeConfChange` used to accept
  `ConfChangeLeaveJoint` whenever the configuration was joint, so a client could propose the leave while the joint entry was still
  uncommitted. It now refuses any change while one is uncommitted (section 9).
- **Waiting on T3.12: the `SyncNever` variants.** The crash model's `SyncNever` runs treat a mismatching install as unacknowledged
  until a `WAL.Sync`, so they check only the weaker oracle (states a, b/c, d of `record.go`). The strict variant needs T3.12.
- **What the tests do not cover:**
  - No real kernel or power-loss test; the guarantees hold inside the faultfs model only.
  - A downgrade to a binary without type 4 is documented in `record.go`; no test of it was written here.
  - The unacknowledged-crash window (states b/c) is checked by a weaker oracle: a recovered node may still hold the pre-install tail.
    That is safe by the argument in `record.go`, not by an end-to-end test of every follower reply.
  - A matching snapshot keeps its suffix on disk while memory drops it; the difference is checked benign only at the
    storage and raft-core level (`TestMatchingSnapshotMemoryDiskDifferenceIsBenign`).
  - The "stale leader" question is not settled in general: with fixed membership the counting argument in the card holds; with changing
    membership it relies on the fix removing the stale `ConfChange`, plus the T3.16 fix (section 9).

## 8. T3.17 findings (branch `feat/t3-17-snapshot-conf`)
- **Fixed here: stale configuration base.** `Node.baseConf` was set only in `NewNode` and `restore`, so a snapshot the node made itself
  never advanced it; a later config append or truncation replayed the surviving suffix over the old base and dropped compacted config
  entries. Reachable in ordinary operation: `AddNode(4)`, its automatic `Leave`, an automatic snapshot, then `AddNode(5)` left node 4 out
  of the voters (red tests in `internal/raft/snapshot_conf_test.go`). The base is now the snapshot's `Conf` at its index, or the initial
  configuration at index 0 with no stored snapshot (`refreshBase`, `internal/raft/confchange.go`).
- **Fixed here: the snapshot writer stored the append-effective configuration.** `compact` and `maybeSnapshot` passed `ConfState()`,
  which includes config entries above `applied`, committed or not. They now pass `ConfStateAt(applied)`. Shown by red tests:
  a cut-off leader and a follower compacting with a pending `Add 4` stored the joint configuration, a restart after the entry was
  overwritten kept it, and a leader shipped it to an emptied follower.
- **New behaviour to know:** replay of configuration entries fails (wrapping `ErrStorage`) when a stored snapshot is the base and the log's
  first index is above snapshot index + 1; only storage with no snapshot (`baseIndex == 0`) may report a higher first index. Snapshot
  file and InstallSnapshot encodings are unchanged (golden tests).
- **Snapshots already on disk are not repaired.** A snapshot written by an older binary while a change was pending keeps its joint
  `Conf`; the fix prevents new ones. `CreateSnapshot` still does not reject a joint `Conf` with an empty `Incoming` (out of scope).
- **What the tests do not cover:** tests 2 and 4b assert `ConfState` (and the acknowledgements it implies), not an election or commit
  carried out by the node; those are pinned at the raft level. 4b models the lagging follower as a node with an emptied data directory.
  3b uses a leader removing itself, because the joint phase of `Add 4` needs node 4. Test 3a (Leave already appended) passed before the fix.

## 9. T3.16 findings (branch `fix/t3-16-single-pending-confchange`, stacked on `feat/t3-17-snapshot-conf`)
- **Fixed here: a second configuration change while one is uncommitted.** `ProposeConfChange` refused only when the configuration was
  joint, so a `Leave` before the joint entry committed, or an `Add`/`Remove` while the automatic leave was uncommitted, was accepted.
  `lastConfIndex` (replacing `jointEntryIndex`) is now derived by `rebuildConfig` for every decodable configuration entry, and a change is
  refused with `ErrConfChangePending` while it is above the commit index or before the leader has committed in its term. The automatic
  leave no longer depends on `jointEntryIndex`, so a snapshot with a joint `Conf` and no config entry in the suffix also leaves.
  `TestChainedUncommittedChangesCannotBuildDisjointQuorums` showed two nodes committing different entries at index 2 before the fix.
- **Behaviour change visible to clients:** `AddNode` returns when the joint entry commits; an `AddNode`/`RemoveNode` immediately after is
  refused with `FailedPrecondition` until the automatic leave commits. `ListMembers` reports `joint=false` as soon as the leave is
  *appended*, which is before it commits. Two transport tests now retry on that refusal (human decision).
- **Not done from the card (as recorded at the T3.16 merge; not re-checked in this refresh):** test 5b/5c (pending state across snapshot install, joint snapshot with no entry), the 4(a-c,e) variants of
  test 4, and the additive chaos scenario. The core covers 5b/5c by construction (`lastConfIndex` is assigned unconditionally) but no test
  pins it. T3.17 test 3 (card commit 10) is also not moved.

## 10. T3.19 findings (branch `fix/t3-19-nonmember-campaign`)

- A node whose configuration lacks its own id no longer campaigns: `campaign()` returns without side effects (no term bump, no persist, no
  messages) unless `promotable()`, which is `conf.hasVoter(id)` (voters, plus incoming while joint; the same set as `members()`). During a
  leave, the outgoing node is still in `voters` and stays promotable until the joint config is left. `becomeLeader` returns an error for a
  non-member instead of panicking. Before the fix, `chaos` scenario "the leader removes itself" panicked in `becomeLeader` at seed 7.
- Unchanged and not done (as recorded at the T3.19 merge; the code in `Node.Step` still answers vote requests from any sender): a removed node still *responds* to vote requests and appends until it learns of its removal; rejecting
  non-member vote requests (dissertation §4.2.3 disruption defence) and telling a removed node it was removed are out of scope.
  `TestAtMostOnePendingConfEntry` still does not tick removed nodes; that is no longer required for safety but was not changed.

## 11. T3.9 findings (PR #29, `fix(wal): poison the WAL on the first write error; node stops`)
- **Fixed here: retry after a failed write or fsync.** `WAL.failed` (`internal/storage/wal.go`) is set by the first failed write, sync, segment roll, `TruncateBefore`
  cleanup or `Sync`; every later `AppendEntries`, `SaveHardState`, `SaveSnapshotMeta`, `TruncateBefore` and `Sync` returns `ErrWALFailed` wrapping the cause. `Close` releases the
  descriptor without an fsync and returns the cause, and `Sync` on a failed WAL returns the error without syncing (a success would prove nothing after a failed fsync). Reason: a retry after a failed
  fsync can succeed for data the kernel dropped, and a short write followed by a retry buries garbage mid-log (RUNBOOK 7.2 item 1, 7.3 #3 and #4).
  Tests (`internal/storage/poison_test.go`): `TestWriteFailurePoisonsTheWAL`, `TestSyncFailurePoisonsTheWAL`, `TestCloseAfterFailureReturnsTheCauseWithoutSyncing`,
  `TestShortWriteThenRetryDoesNotBrickTheDirectory`, `TestSyncFailureOnRollPoisons`, `TestSyncFailureInTruncateBeforePoisons`, `TestEveryFailureAfterPoisonKeepsDurableStateIntact`.
- **Fixed here: the node stops on any `CreateSnapshot` error.** `fatal()` (`internal/node/node.go`) matches `ErrWALFailed` as well as `raft.ErrStorage`, and both `maybeSnapshot` and
  `compact` call `n.fail` on a `CreateSnapshot` error (the old code swallowed it). `main` exits non-zero when the node is done. Tests (`internal/node/storage_failure_test.go`):
  `TestNodeStopsWhenTheWALFails`, `TestFailedSnapshotWriteStopsTheNode`, `TestFailedCompactStopsTheNode`, `TestSnapshotCleanupFailuresAreReportedAndStopTheNode`.
- **Controls:** a failed snapshot *file* write does not poison the WAL (`TestSnapshotFileFailureDoesNotPoisonTheWAL`, `TestSnapshotFileFailureLeavesMemoryUnchanged`); the node still stops on it.
- **What the tests do not cover / still open:**
  - `maybeSnapshot` still returns without stopping the node when `kv.Snapshot()` or `ConfStateAt` fails (read in `node.go`); only a `CreateSnapshot` error is fatal.
  - Whether a failed snapshot purge could be survived instead of stopping is T3.15 (the code comment says so); today it stops the node.
  - Faults are injected by `faultfs`, not a real disk; the real-kernel behaviour of a failed fsync is the premise, not something a test shows.

## 12. T3.10 findings (PR #28, `Synced the replacement HardState before deleting segments`)
- **Fixed here: term and vote rollback by compaction under `SyncNever`.** `TruncateBefore` re-records HardState and then deletes older segments, which may hold the only durable
  copy. It now syncs the active segment before the first `Remove` when `policy != SyncAlways` and the HardState is non-empty (`wal.go`; under `SyncAlways` `write` already synced the record).
  A sync failure poisons the WAL and deletes nothing. Cost: one fsync per compaction that deletes segments.
- Tests (`internal/storage/compaction_crash_test.go`): `TestCompactionDoesNotRollBackHardStateUnderSyncNever`, `TestCompactionKeepsNewerUnsyncedHardState`,
  `TestCompactionCrashPointsUnderSyncNever`, `TestCompactionCrashPointsUnderSyncAlways`, `TestNoSyncWhenNothingIsDeleted`, `TestSyncFailureBeforeDeleteDeletesNothing`.
- **Not covered:** the fsync in `roll()` (an unsynced segment roll followed by `WAL.Sync`) is T3.12 and is not implemented. The crash model is `faultfs`, not a real kernel.

## 13. T3.18 findings (commit `f7c6246`, `Protected membership changes and bound snapshot streams`)
- **Added: `--cluster-token-file`.** A bearer-token interceptor on the shared listener for exactly `AddNode` and `RemoveNode` (`newSharedServerWithClusterToken` in `cmd/raftkv-server/main.go`,
  `transport.NewClusterTokenInterceptors`). Off by default, which keeps today's open behaviour; a set but unusable file stops the server from starting (`validateClusterToken`).
  `ListMembers` and `KVService` stay open. Tests (`internal/transport/guard_test.go`): `TestAddRemoveNodeWithoutTokenIsUnauthenticated` (the default), `TestAddRemoveNodeWithTokenWorks`,
  `TestListMembersAndKVStayOpen`, `TestLoadClusterTokenRefusesUnusableFiles`.
- **Added: snapshot stream budgets.** `DeliverSnapshot` takes a stream slot before reading any frame and reserves bytes per chunk against `snapshotBudget`: `DefaultSnapshotBudgetBytes` = 256 MiB
  in flight and `DefaultSnapshotStreams` = 4 concurrent streams per node; the 1 GiB per-stream `MaxSnapshotBytes` is unchanged. Over budget is `ResourceExhausted` and the stream is discarded
  (`internal/transport/peer.go`). Tests (`internal/transport/snapshotbudget_test.go`): `TestSnapshotStreamsRespectTheAggregateByteBudget`, `TestSnapshotStreamCountIsBounded`,
  `TestBudgetIsReleasedOnErrorCancelAndEarlyClose`, `TestOversizeSingleStreamIsStillRejected`, `TestRejectedSnapshotStreamDoesNotBufferPayload`, `TestInstallSnapshotStillWorksUnderBudget`.
- **Not done:** nothing else on the shared listener is authenticated: `Deliver`, `DeliverSnapshot` (a snapshot sender is not checked to be a member), `KVService` and `ListMembers` carry no credentials
  and there is no TLS. The budget stops memory exhaustion; it does not authenticate anyone (`docs/observability.md`).

## 14. T3.20 findings (PR #31, `fix(transport): coalesce queued append requests`)
- **Cause found for the flaky `TestFollowerCatchesUpFromALargeBacklog`** (per the PR's commit message; the card was an investigation): the leader re-sends its current append every heartbeat tick, the
  per-peer outbound queue (256 slots, drops the newest when full, one blocking RPC at a time) filled with stale duplicates, and a fresh append waited behind a whole drain. The two control tests
  (`TestFollowerCatchesUpAfterEveryAppendResponseIsDropped`, `TestStaleDuplicateAppendsDoNotRegressProgress`) pass on the unfixed core: no state exists in the leader that only a response can unblock.
- **Fixed here:** in `peer.enqueue` (`internal/transport/peer.go`) a newer `MsgAppendRequest` replaces a still-queued append of the same term for that peer and goes to the tail; other message types,
  snapshots and appends of a different term are never replaced; a full queue still drops the newest. New counters: `PeerStats.Coalesced`, `PeerStats.Queued`, and `Node.RecvDropped`
  (inbound messages `Node.Step` discards when its 256-slot receive buffer is full). No `internal/raft` change. Tests (`internal/transport/queue_overflow_test.go`):
  `TestFreshAppendReachesPeerAfterQueueOverflow`, `TestHeartbeatWaitsForAtMostOneQueuedAppend`, `TestCoalescingNeverMergesAcrossTermsOrTypes`.
- **Not shown:** that the CI flake is gone (no repeated-run evidence was gathered in this refresh). **Still true:** `RaftServer.Deliver` answers OK after `Step` drops a message, so the sender counts it as sent;
  the drop is now counted on the receiver (`RecvDropped`) but not visible to the sender. Raft tolerates the loss.

## 15. T4.7 findings (PR #19, `Added admin service, fault wrapper and event stream`; docs in `docs/observability.md`)
- **Added: `AdminService`** (`Status`, `Crash`, `Pause`, `SetPartition`, `SetNetem`, `Events`) on its own listener (`--admin-listen`), behind a bearer token (`--admin-token-file`, mode 0600, constant-time compare
  of SHA-256 digests in `internal/transport/admin.go`). `--admin-chaos` additionally allows `Crash`, `Pause`, `SetPartition` and `SetNetem`. Everything is off by default. Constants in code:
  `CrashDelay` 50ms and `CrashExitCode` 137; `MaxPause` 60s; `FaultExpiry` 120s; `MaxDelayQueue` 1024 per peer; `MaxSubscribers` 16 and `SubscriberBuffer` 256 (`internal/node/events.go`).
- Tests: `TestAdminRPCsRejectMissingAndWrongTokenAndChangeNothing`, `TestTokenOKWorks`, `TestChaosRPCsNeedAdminChaos`, `TestServerRefusesAdminListenWithoutToken`, `TestTokenFileMustNotBeGroupOrWorldReadable`,
  `TestCrashExitsAfter50msWith137`, `TestAdminOnlyOnAdminEndpoint`, `TestAdminServiceIsRegisteredOnlyOnTheAdminListener` (`cmd/raftkv-server/admin_flags_test.go`), `TestAdminStatusAndEventsWorkWhilePaused`,
  `TestPauseStopsTicksAndDropsBothDirections`, `TestEveryKVAndClusterMethodIsUnavailableWhilePaused`, `TestPartitionedLeaderLosesLeadership`, `TestEachFaultExpiresIndependentlyAt120s`,
  `TestDelayQueueIsBoundedAndDroppedOnExpiryOrClear`, `TestPauseAutoResumesAtCap` (`internal/node/pause_test.go`), `TestEventsAreMonotonicAndGapFree`, `TestSubscriberCountIsBounded`, `TestSlowSubscriberIsDroppedNotBlocking`.
- **What it is not:** the faults are application-level (they drop or delay Raft messages and stop ticks inside the process; no kernel network stack, no SIGSTOP, and a paused node still applies committed
  entries); events are observed transitions, so a change that starts and ends between two observations can be missed; the admin token does not secure the shared listener (section 13).

## 16. Chaos tooling: `cmd/chaosrun` (T5.6) and the mutation suite (T5.7)
- **`cmd/chaosrun -sim`** runs all 24 scenarios (the 23 hand-scripted ones plus the seed-generated "generated nemesis and workload") over a seed range, prints a per-run verdict and a reproduce line on failure,
  exits 1 on any failure and 2 on a usage error; `-json` prints one object per run, `-disk DIR` uses disk-backed storage. Tests: `TestRunExitCodes`, `TestRunFailureExitsOneWithReproLine`, `TestJSONOutputOneObjectPerRun`,
  `TestChaosrunAcceptance` (`cmd/chaosrun/main_test.go`), `TestScheduleIsPureFunctionOfSeed`, `TestGeneratedScenarioHolds`, `TestAllScenariosCatalog` (`chaos/generated_test.go`). `make chaos-smoke` runs it over seeds 1-20.
- **What the chaos oracles check** (read from `chaos/cluster.go` and `chaos/monitor.go`): after every simulated tick, `checkElectionSafety` and then the committed-log monitor. The monitor checks (a) every node
  that reports an entry committed holds the same term at that index as every earlier report (State Machine Safety, approximated: equal terms mean equal commands only if Log Matching holds, which is assumed),
  (b) a state machine never applies past its node's commit index, (c) a node's log never ends before its commit index and its newest committed entries keep their recorded terms, (d) every leader of a later term
  holds every entry recorded committed in an earlier term with the recorded term (Leader Completeness). **It does not check Log Matching directly or Leader Append-Only**, and it skips entries a node has compacted.
  Besides the monitor, runs check replica convergence after healing (`Converged`), per-key linearizability of the client history (`chaos/checker.go`; the simulated clients issue writes and reads only, `OpKind` in `chaos/cluster.go`) and read liveness.
  Tests: `TestCommittedLogMonitorAcceptsHealthyRuns`, `TestCommittedLogMonitorFlagsADifferentTermAtACommittedIndex`, `TestCommittedLogMonitorFlagsApplyingPastCommit`,
  `TestCommittedLogMonitorFlagsALeaderThatLacksACommittedEntry`, `TestCommittedLogMonitorSurvivesARestartThatRewindsACommitIndex`, `TestElectionSafetyIsCheckedContinuously`.
- **Mutation suite** (`go test -tags faultinject ./chaos -run TestMutationSuite`; `docs/CHAOS_RESULTS.md`, regenerated by CI and diffed): nine injected bugs behind the `faultinject` build tag (`internal/faultinject`;
  call sites are `faultinject.Active(...)` guards in `internal/raft`, `internal/storage` and `internal/statemachine`), each detected by an oracle: 9 of 9, with a control (every switch off, 670 runs over 17 scenario and mutation pairs) clean. Oracles that fired: linearizability (bugs 1, 5, 6),
  committed-log monitor (2, 3, 4, 9), liveness (7), convergence (8). Tests: `TestMutationSuite`, `TestControlRunIsClean`, `TestMutationTableCoversTheNineSwitches`, `TestEveryMutationSwitchChangesADigest`,
  `TestSuiteReportsInertMutationUndetected`, `TestDetectRestoresTheSwitches`, `TestChaosrunReproducesAMutation`, `TestLaggingNodeScenarioAloneDetectsSkippedVoteCheck`, one `TestMutation_*` per bug,
  and `TestCallSitesAreGuardedOnly`, `TestDefaultBinaryHasNoMutationMachinery` (`internal/determinism/faultinject_test.go`).
- **Limits:** scenarios are scripted, so a seed varies message fate and election timeouts only; the seed range is a search, not a proof. Bug 4 (grant votes without the up-to-date check) was not found in the
  scenarios first named for it: 152 runs, first detected at seed 2 in "leader crashes during a membership change" (`docs/CHAOS_RESULTS.md`). Bug 3 (commit a previous-term entry) rests on one scripted scenario ("figure 8") whose
  roles depend on which node the seed elects and which is inconclusive if its partition never happens (`RequireFaults` in `chaos/mutation_scenarios.go`). The mutation results prove these nine bugs are caught,
  not that other bugs would be.

## 17. Known limitations (what the tests and tooling do not cover)
- **The shared listener is still open.** `Deliver`, `DeliverSnapshot`, `KVService` and `ListMembers` have no authentication and there is no TLS; `AddNode` and `RemoveNode` are protected only when `--cluster-token-file` is
  set. A snapshot sender is not checked to be a member. The snapshot budget (256 MiB, 4 streams) bounds memory, not trust. Run on a trusted network.
- **Admin faults are application-level.** They act inside the process (message drop and delay, tick stop, `os.Exit(137)`); they say nothing about kernel, NIC or scheduler behaviour.
- **The chaos monitor is partial.** It does not check Log Matching directly or Leader Append-Only (only approximations: committed-term-per-index and leader completeness; `TestLeaderNeverOverwritesItsOwnLog` covers
  one three-node leader in the raft package, not chaos runs). Its State Machine Safety check assumes Log Matching.
- **Mutation detection depends on seeds.** Bug 4 needed 152 runs across several scenarios and bug 3 depends on role assignment by the seed (section 16); a changed election-timeout draw can move a detection.
- **Any `CreateSnapshot` error now stops the node** (T3.9). That is deliberate and safe (crash-stop is in the failure model) but it means a transient snapshot-file error takes the node down; surviving a failed purge is T3.15.
  `maybeSnapshot` still ignores errors from `kv.Snapshot()` and `ConfStateAt`.
- **Storage hardening is deferred, not done:** T3.12 (fsync in `roll()`), T3.13 (durable creation of data, wal and snap directories), T3.14 (oversize WAL entries and proposals), T3.15 (`SnapshotsKept`, snapshot vs purge race).
- **Crash testing is a model.** `faultfs` is stricter than POSIX requires but has not been compared with a real kernel or a real power cut; no cross-file reordering, no mid-log bit rot.
- **No real-time or multi-process chaos run has happened yet.** The simulator runs on virtual ticks in one process. The admin API, the gateway (core, live adapter, chaos controller, verify runner, TLS) and the Terraform to run them on AWS are built and tested against stubs and an in-memory cluster, but nothing has been applied or run on real infrastructure; the live verification run (D8) is the remaining step.
- **Removed nodes** still answer vote requests and appends until they learn of their removal; non-member vote rejection (dissertation section 4.2.3) is not implemented (section 10).
- **Election timeout** defaults to 1.0-1.9 s, not the RUNBOOK's 600-1200ms (section 4, item 16).

## 18. T4.6 findings (branch `feat/t4-6-client`): `internal/client` and `cmd/raftkvctl`
- **What it is.** A Go client over `KVService` (Get, Put, Delete, Status): rotation over a static node list, NotLeader hint following with a per-operation loop guard, full-jitter exponential backoff (20 ms to 1 s,
  never zero), 2 s per attempt, 30 s total when the caller's context has no deadline, injected `Clock` and `Rand`, one `seq` per write reused by every retry. `Close` is idempotent and unblocks waiters.
  `raftkvctl` is a thin CLI over it. `cmd/raftkv-bench` is not migrated (out of scope).
- **Outcomes.** A write ends `nil`, `*DefinitiveError` (cannot have been applied: nothing sent, or `InvalidArgument`, `PermissionDenied`, `Unauthenticated`, `Unimplemented`, `ResourceExhausted`) or
  `*IndeterminateError` (an attempt was sent and no definite answer came back). Every NotLeader or lost-leadership reply to a write is indeterminate, because the node can send them after proposing; an election gap
  therefore reports "unknown" although nothing may have been proposed. A read never reports unknown.
- **Restart hazard (read this before reusing a client id).** The server drops any write with `seq <= lastSeq` for that id and still reports success (`internal/statemachine/session.go` `shouldApply`), with no expiry and no
  RPC to read the mark. A process that restarts with a fixed client id and seq 1 has its writes silently dropped until the seq passes the old mark. The client therefore draws a random non-zero id per process; a caller
  that sets `ClientID` owns `StartSeq`. Smaller hazard: the table holds 4096 sessions and evicts the least recently used, so a session evicted between an attempt and its retry makes the retry apply twice; every
  restart with a random id adds one session. No server mechanism was added.
- **Serialised writes.** A `Client` takes a write mutex, so one id never has two writes in flight (opposite-order application would lose the lower seq while reporting success). Use several Clients for parallel writes.
- **Tests.** `internal/client` (fake clock and scripted nodes, plus `TestRetryAppliesOnce` and `TestKillLeaderMidWorkloadClientCompletes` on a real 3-node loopback cluster, both under `-short`), `cmd/raftkvctl`
  (`TestCtlExitCodesAndOutput`). Leak check counts goroutines after `Close`; there is no `goleak` dependency.
- **Unverified.** The integration tests use `SyncNever` and 10 ms ticks. `internal/transport` `TestInvalidMembershipRequestsAreRejected` failed twice during the full-repo run (FailedPrecondition instead of InvalidArgument,
  unrelated to this task) and passed on a retry and alone; it looks load-sensitive.

## 19. D3 findings (branch `feat/d3-release`): reproducible release build
- **What it is.** `make release` (`scripts/release.sh`) builds `raftkv-server` and `gateway` for linux/arm64 into `dist/<version>/` (`raftkv-server-linux-arm64`, `gateway-linux-arm64`, `*.sha256`, `MANIFEST`) and runs
  `scripts/verify-release.sh`. It is build only and needs no AWS. `make release-upload RELEASE_DIR=dist/<version>` (`scripts/release-upload.sh`) is a separate, human-run step; RUNBOOK 6.1 still says `release` uploads.
  `raftkv-server -version` and `gateway -version` print `internal/buildinfo.Line` (first argument only).
- **Reproducibility.** `-trimpath -buildvcs=false -ldflags "-s -w -buildid= -X ..."`, the commit date (not the build time), `-mod=readonly`, `GOWORK=off`, no tags, and `GOTOOLCHAIN` taken from the `toolchain go1.25.13`
  line added to `go.mod`. Verified: the same commit built from two clone paths gives identical bytes. A different compiler gives different bytes. `-s -w` strips symbols (crash traces lose them).
- **Dirty trees.** Refused (tracked changes, or untracked `*.go`/`go.mod`/`go.sum`) unless `ALLOW_DIRTY=1`, which marks version and commit `-dirty`; `release-upload.sh` never uploads those.
- **Verify details.** Under `-trimpath`, `go version -m` records GOOS/GOARCH/CGO/-trimpath but NOT `-ldflags`, so the stamped version/commit/date are checked by searching the binary (`grep -a`, not `strings`, which missed them).
  fakecluster is checked twice: `go list -deps ./cmd/gateway` and the package path in the gateway binary (the tagged-build test proves the second check can fail).
- **Upload.** Refuses before any aws call when `CI`/`GITHUB_ACTIONS` is set, `RELEASE_AWS_PROFILE` or `RELEASE_BUCKET` is empty, `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` is set, the manifest is
  dirty or verification fails. It then asks `head-object` for all six keys (only "Not Found" means absent; anything else refuses) and uploads under `artifacts/raftkvd/<v>/` and `artifacts/gateway/<v>/`, MANIFEST last.
- **Tests.** `scripts/release_test.sh`, `verify-release_test.sh`, `release-upload_test.sh` (not run by `ci.yml`; they do real go builds), plus Go tests in `internal/buildinfo`, `cmd/raftkv-server`, `cmd/gateway`.
- **Not done.** No real upload was run; no `--config` flag (RUNBOOK 10.4/10.5 text for the node differs from the repo, see the PR); `Dockerfile:19` still passes a dead `-X main.version`.
