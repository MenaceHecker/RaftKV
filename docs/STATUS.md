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
- Other binaries: `cmd/raftkv-bench` (load generator; follows NOT_LEADER redirects), `cmd/chaosrun` (`-sim -seeds A-B -seed N -scenario TEXT -list -json -disk DIR`; exit 0 all passed, 1 a failure, 2 usage).
  Containers: `docker compose up --build`. Kubernetes: `deploy/kubernetes/raftkv.yaml`. Protobuf: `buf generate`.
- Build-tagged mutation switches: `go test -tags faultinject ./chaos -run TestMutationSuite` (section 16). The default binary contains only constant-false guards (`TestDefaultBinaryHasNoMutationMachinery`).
- Not present, still: `cmd/gateway`, `cmd/raftkvctl`, `infra/`, `web/`, `docs/NOTES.md`, a porcupine cross-check, a JSONL history with `info` ops, a checker shrinker and an HTML report (each checked with `ls` or `grep` at `73915fa`).
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
- UNKNOWN: whether a client library that follows leader hints exists. None is shipped; `cmd/raftkv-bench` follows NOT_LEADER redirects (`TestNotLeaderFindsTheRedirect`) and the server side is tested (`TestARedirectIsEnoughToFindTheLeader`). Whether `time.Sleep` masks flakiness: 14 `_test.go` files use it (7 at the audit); the one flake recorded since, `TestFollowerCatchesUpFromALargeBacklog`, had a real cause (section 14), not a sleep.
- Not present: `read_mode: log|readindex` switch (ReadIndex is the only path); snapshots are held fully in memory at both ends (README "Things that are honestly not done").
- Absent from the repo: see section 2 (`cmd/gateway`, `cmd/raftkvctl`, `infra/`, `web/`, `docs/NOTES.md`, porcupine cross-check, JSONL history with `info` ops, checker shrinker, HTML report, and CI jobs for lint, gitleaks, buf and guard-tests). Present now and no longer absent: Makefile, `.golangci.yml`, `scripts/`, `.githooks/`, CODEOWNERS, PR template, `docs/INVARIANTS.md`, `docs/tasks/`, a nightly schedule inside `ci.yml`, `cmd/chaosrun`, the admin service, the mutation (`faultinject`) suite.
- Checker note: `chaos/checker.go` has pending and unknown op statuses (`StatusPending`, `StatusUnknown`), so indeterminate ops are modelled; correctness not reviewed.

## 7. T3.11 findings (branch `feat/t3-11-stale-tail`)
- **Fixed here: stale tail after `ApplySnapshot`.** A snapshot whose last included entry disagrees in term with the local entry at that
  index left the old entries above it in the WAL, and `OpenDiskStorage` brought them back after a restart (paper section 7,
  InstallSnapshot rule 6). Effects shown by red tests before the fix: a restarted voter compared candidates against the stale tail
  instead of the snapshot, so it granted a vote it had refused before the restart (`stale_tail_vote_test.go`), and a stale `ConfChange`
  in the tail changed membership and quorum after a restart (`internal/node/applysnapshot_restart_test.go`, 3e). The fix is a new WAL
  record, `recordDiscardSuffix` (type 4), written together with `SnapshotMeta` in one fsynced write, only on a term mismatch.
  Compatibility: a binary without type 4 refuses such a WAL with `ErrCorruptWAL`; there is no downgrade path.
- **Not fixed, candidate card T3.16: leaving the joint config before the joint entry commits.** `ProposeConfChange`
  (`internal/raft/confchange.go:10-13`) accepts `ConfChangeLeaveJoint` whenever the configuration is in joint state, and
  `handleConfChange` (`internal/node/node.go:645`) passes the proposal through, so a client can propose the leave while the joint entry
  is still uncommitted. Not changed here.
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
    membership it relies on the fix removing the stale `ConfChange`, plus the T3.16 gap above.

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
- **Not done from the card:** test 5b/5c (pending state across snapshot install, joint snapshot with no entry), the 4(a-c,e) variants of
  test 4, and the additive chaos scenario. The core covers 5b/5c by construction (`lastConfIndex` is assigned unconditionally) but no test
  pins it. T3.17 test 3 (card commit 10) is also not moved.

## 10. T3.19 findings (branch `fix/t3-19-nonmember-campaign`)

- A node whose configuration lacks its own id no longer campaigns: `campaign()` returns without side effects (no term bump, no persist, no
  messages) unless `promotable()`, which is `conf.hasVoter(id)` (voters, plus incoming while joint; the same set as `members()`). During a
  leave, the outgoing node is still in `voters` and stays promotable until the joint config is left. `becomeLeader` returns an error for a
  non-member instead of panicking. Before the fix, `chaos` scenario "the leader removes itself" panicked in `becomeLeader` at seed 7.
- Unchanged and not done: a removed node still *responds* to vote requests and appends until it learns of its removal; rejecting
  non-member vote requests (dissertation §4.2.3 disruption defence) and telling a removed node it was removed are out of scope.
  `TestAtMostOnePendingConfEntry` still does not tick removed nodes; that is no longer required for safety but was not changed.
