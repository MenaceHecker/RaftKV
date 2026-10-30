# Raft invariants

Mirrors RUNBOOK §7.1-7.5. Verdicts come from docs/STATUS.md §4-6 (a read of the code at `ddff529`); the test names below
were found by name and file:line only, and their bodies were **not** reviewed here. OK = code does it. GAP = differs from the
RUNBOOK. UNKNOWN = not determined. "Test" cells name an existing test or say none was found.

## 7.1 Safety properties
| Property | Verdict | Check / test |
|---|---|---|
| Election Safety | OK | Checked after every simulated tick: `chaos/cluster.go:391` (`checkElectionSafety`); test `chaos/cluster_test.go:343` `TestElectionSafetyIsCheckedContinuously` |
| Leader Append-Only | UNKNOWN | No dedicated check found |
| Log Matching | UNKNOWN | No dedicated check found. Related: `internal/raft/replication_test.go:157` `TestFollowerWithConflictingLogIsRepaired` |
| Leader Completeness | UNKNOWN | No dedicated check found. Related: `internal/raft/replication_test.go:210` `TestCommittedEntrySurvivesLeaderChange` |
| State Machine Safety | partial | Replicas compared after healing: `chaos/cluster.go:786` (`Converged`), `chaos/cluster_test.go:308` `TestConvergenceComparesActualState`; histories checked for linearizability by `chaos/checker.go` (tests in `chaos/checker_test.go`) |

## 7.2 Implementation checklist
| # | Item | Verdict | Test |
|---|---|---|---|
| 1 | Persist term, vote, log before replying and before counting own append | OK (default config; `--fsync=false` disables) | `internal/raft/durability_test.go:228` `TestAFailedAppendDoesNotAdvanceTheCommitIndex`, `:175` |
| 2 | Metadata via temp file, fsync, rename, dir fsync | GAP: HardState is a WAL record (`internal/storage/wal.go:363-378`) | none for the RUNBOOK mechanism |
| 3 | term > current: update, clear vote, step down | OK | `internal/raft/durability_test.go:175` `TestSteppingUpATermIsNotRememberedIfItCannotBePersisted` |
| 4 | term < current: reject and reply with current term | GAP: only vote requests are answered (`internal/raft/node.go:368-377`) | none found |
| 5 | RequestVote: one vote per term, up-to-date check | OK | `internal/raft/election_test.go:85` `TestOneVotePerTerm`, `:132` `TestVoteIsIdempotentForSameCandidate`, `:156` `TestStaleLogCandidateIsRejected` |
| 6 | Reset election timer only on valid AE, grant, or own election | GAP (minor): resets on any higher-term message (`internal/raft/node.go:359-366`) | none found |
| 7 | AE rejects on prev mismatch with conflict hints | OK | `internal/raft/replication_test.go:157` `TestFollowerWithConflictingLogIsRepaired` |
| 8 | Truncate only on a real conflict | OK (code), test UNKNOWN | no test found that a stale duplicate AE does not truncate |
| 9 | Follower commit = min(leaderCommit, last new entry) | OK | `internal/raft/readonly_test.go:371` `TestHeartbeatAdvancesFollowerCommitIndex` (related only) |
| 10 | Commit only current-term entries by counting (§5.4.2) | OK | `internal/raft/replication_test.go:273` `TestCommitRequiresEntryFromCurrentTerm` |
| 11 | No-op on election win | OK | `internal/raft/replication_test.go:252` `TestNewLeaderAppendsNoOp` |
| 12 | nextIndex/matchIndex init, back up with hints, never below snapshot | OK | `internal/raft/snapshot_test.go:249` `TestLeaderFallsBackWhenNoSnapshotExists`, `:46` `TestLaggingFollowerIsCaughtUpByASnapshot` |
| 13 | Single apply goroutine, in order, never twice | OK | `internal/raft/replay_test.go:21` `TestCommittedEntriesComeBackUntilTheyAreAdvanced` |
| 14 | Stale responses ignored | OK (code), test UNKNOWN | none found by name |
| 15 | Clock, Rand, Transport injected; no wall clock in `internal/raft` | OK in spirit, GAP in letter: no `Clock` interface, concrete `*rand.Rand` | `internal/determinism/determinism_test.go:30,51,71` |
| 16 | Randomized timeout; heartbeat 100ms, election 600-1200ms | GAP: election is 1.0-1.9 s by default (`internal/node/node.go:63-65`) | none |

## 7.3 Persistence and recovery
| # | Item | Verdict | Test |
|---|---|---|---|
| 1 | WAL record format, ~64 MB segments | GAP (partial): framing matches, segments are 16 MiB (`internal/storage/wal.go:36`) | `internal/storage/record_test.go` |
| 2 | Torn tail truncated; mid-log corruption refuses start | OK (T3.7); durability of the repair under power loss: `crash_test.go` `TestRepairIsDurable`, `TestCrashDuringRepairThenSecondCrash` (T3.8) | `internal/storage/wal_tail_test.go` `TestBadCRCAtTailIsRepaired`, `TestBadTailIsRepairedAtEveryByte`, `TestDamagedLengthBeforeValidRecordIsRefused`, `TestBadRecordFollowedByValidRecordIsRefused`, `TestCRCValidUndecodableRecordIsRefused`; `wal_tail_fuzz_test.go` `FuzzWALReplayTail`; `internal/storage/wal_test.go:199` `TestTornTailIsRepaired`, `:293` `TestCorruptionInsideSegmentIsRejected`, `:320` `TestCorruptionInOlderSegmentIsRejected` |
| 3 | Truncate-suffix and prefix crash-safe | OK (code), tests UNKNOWN | `internal/storage/disk_test.go:243` `TestCompactionSurvivesCrash` reopens the store; it is not a crash at each syscall |
| 4 | Crash at any write boundary, with a fault-injecting FS | OK for the cases below (`internal/storage/faultfs`); the cases listed in T3.9-T3.13 are not covered | Model: `internal/storage/faultfs/faultfs_test.go` `TestFaultFSModel`. Crash before and after every filesystem operation under drop, torn, garbage and zero tails: `internal/storage/crash_test.go` `TestCrashAtEveryOpDuringAppend`, `TestCrashAtEveryOpDuringSuffixTruncate`, `TestCrashAtEveryOpDuringSnapshotWrite`, `TestCrashAtEveryOpDuringCompaction`, `TestApplySnapshotCrashPoints`, `TestRenameBeforeDataIsNotDurable`, `TestRepairIsDurable`, `TestCrashDuringRepairThenSecondCrash`. Faults and `SyncNever`: `internal/storage/crashfault_test.go` `TestFsyncFailureIsNotAcked`, `TestShortWriteAndENOSPC`, `TestSyncNeverContract`, `TestRandomWorkloadWithCrashes`. Earlier: torn tail at every cut point `wal_test.go:223`; on-disk chaos `chaos/scenarios_test.go:1202` `TestChaosOnDisk` |

## 7.4 Client semantics
| Item | Verdict | Test |
|---|---|---|
| Dedupe by `(clientID, seq)`, session table in snapshot | OK | `internal/statemachine/session_test.go:71` `TestExactDuplicateIsIgnored`, `:156` `TestSessionsSurviveSnapshotAndRestore`, `:232` `TestSessionTableIsBounded`; full stack `internal/node/cluster_test.go:372` `TestStaleRetryIsDeduplicatedThroughTheFullStack` |
| Linearizable reads (ReadIndex; current-term commit first) | OK (ReadIndex is the only path; no `read_mode` switch) | `internal/raft/readonly_test.go:200` `TestLeaderCanServeReadsOnceNoOpCommits`, `:131` `TestReadIndexNeedsAMajority` |
| Deposed leader never answers a read from local state | OK (code) | `internal/raft/readonly_test.go:300` `TestDeposedLeaderAbandonsInFlightReads`, `:50` `TestReadIndexRequiresLeadership` |
| NOT_LEADER with a hint; client follows hints | server OK (`internal/transport/kvserver.go:67-76`); client library UNKNOWN | none found by name |

## 7.5 Snapshots
| Item | Verdict | Test |
|---|---|---|
| Snapshot contents: index, term, KV state, session table | OK | `internal/statemachine/session_test.go:156` `TestSessionsSurviveSnapshotAndRestore`, `internal/statemachine/kv_test.go:339` `TestSnapshotRestoreRoundTrip` |
| Written atomically (temp, fsync, rename, dir fsync) | OK (code) (`internal/storage/snapshot.go:113-133`) | `internal/storage/snapshot_test.go` (not reviewed) |
| WAL prefix truncated only after the snapshot is durable | OK (code) | `internal/storage/disk_test.go:243` `TestCompactionSurvivesCrash` |
| InstallSnapshot chunked; both follower branches | UNKNOWN: branches not traced; streaming in `internal/transport/peer.go:259-278` | `internal/raft/snapshot_test.go:46` `TestLaggingFollowerIsCaughtUpByASnapshot`, `:104` `TestStaleSnapshotDoesNotRegressAFollower` |
| Recovery = latest snapshot + WAL replay | OK | `internal/node/cluster_test.go:575` `TestSnapshotAndCompactionAcrossRestart` |
