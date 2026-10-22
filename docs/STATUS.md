# RaftKV: current status (Task 0 audit)

Audited at commit `ddff529` by reading code only. **No build, test, or lint was run**, so every pass/fail
claim in the README is unverified here. Line references are to that commit.

Summary: the repo is far ahead of the RUNBOOK's assumptions. The README claims all six phases are complete.
What is missing is the RUNBOOK's tooling (Makefile, lint, scripts, INVARIANTS.md) and its AWS/demo track,
not the Raft core.

## 1. Phases (README:233-238)
| README phase | Scope per README | Status (by reading code) | RUNBOOK phase it overlaps |
|---|---|---|---|
| P1 | Election, replication, commit rules, deterministic harness | Done: `election.go`, `replication.go`, `log.go` | P1+P2 |
| P2 | WAL, snapshots, compaction, crash recovery, KV state machine | Done: `internal/storage`, `internal/statemachine` | P3, part of P4 |
| P3 | Client API: ReadIndex, dedup, node driver, wire protocol, gRPC | Done: `readonly.go`, `session.go`, `internal/node`, `internal/transport` | part of P4 |
| P4 | Membership via joint consensus | Done: `membership.go`, `confchange.go` (RUNBOOK treats it as a non-goal) | none |
| P5 | Chaos testing, linearizability checking, observability | Partial against RUNBOOK P5: `chaos/` and `internal/metrics` exist; see section 6 | P5 |
| P6 | Deploy, writeup, benchmarks | Done as Docker, compose, k8s and `docs/benchmarks.md`; AWS, gateway and web demo absent | P6, D0-D9 |

## 2. Build and run (from README and `ci.yml`; not executed)
- `go.mod` says Go 1.25.0 (local toolchain 1.25.6). Direct deps: grpc, protobuf, prometheus client, yaml.v3.
- `go build ./...`, `go vet ./...`, `go test ./...`, `go test -race ./...`, `GOMAXPROCS=1 go test -count=1 ./...`. CI also checks `gofmt -l` (excluding `.pb.go`).
- Fuzz, 60s each in CI: `FuzzDecodeCommand`, `FuzzRestore` (statemachine); `FuzzReadRecord`, `FuzzDecodeEntry`, `FuzzDecodeHardState`, `FuzzDecodeSnapshotMeta` (storage); `FuzzDecodeConfChange` (raft); `FuzzMessageFromWire` (transport).
- Node: `go build -o bin/raftkv-server ./cmd/raftkv-server`. Flags: `--id --peers --listen --data-dir --tick --election-ticks --heartbeat-ticks --fsync --snapshot-threshold --log-level --metrics-listen` (`cmd/raftkv-server/main.go:53-72`). One gRPC `--listen` address plus a separate metrics HTTP address.
- Load generator: `cmd/raftkv-bench`. Containers: `docker compose up --build`. Kubernetes: `deploy/kubernetes/raftkv.yaml`. Protobuf: `buf generate` (generated code is committed).
- **There is no Makefile, `scripts/`, `.golangci.yml` or `.githooks/`.** Every `make ...` command in `CLAUDE.md`, RUNBOOK §6.1 and `.claude/runner-settings.json` fails today.

## 3. Test counts (static grep of 60 `_test.go` files)
538 `func Test*`, 8 `func Fuzz*`, 2 `func Example*`, 0 benchmarks. This matches README:11, which `internal/determinism/readme_test.go` enforces.

| Package | Test | Fuzz | Example |
|---|---|---|---|
| internal/raft | 169 | 1 | 2 |
| internal/storage | 85 | 4 | 0 |
| chaos | 80 | 0 | 0 |
| internal/transport | 69 | 1 | 0 |
| internal/statemachine | 51 | 2 | 0 |
| internal/node | 29 | 0 | 0 |
| cmd/raftkv-server | 18 | 0 | 0 |
| internal/metrics | 14 | 0 | 0 |
| internal/determinism | 13 | 0 | 0 |
| cmd/raftkv-bench | 10 | 0 | 0 |

About 8,816 non-test lines (excluding `.pb.go`) and 16,955 test lines. Pass/fail: **UNKNOWN**.
Chaos: 23 named scenarios (`chaos/scenarios_test.go:87-1135`), run in memory and again on disk (`TestChaosOnDisk`, :1202).

## 4. RUNBOOK §7.2 checklist
OK = code does it. GAP = differs from the RUNBOOK. UNKNOWN = cannot tell by reading. Test names were matched by name only.

| # | Item | Verdict | Evidence |
|---|---|---|---|
| 1 | Persist term, vote and log before replying, and before the leader counts its own append | OK (default config) | Core writes to storage synchronously before queuing replies: `node.go:506-513`, `election.go:50` then `:57`, `replication.go:24-28`, `log.go:82-107`. Fsync per write: `wal.go:402`. Caveat: `--fsync=false` selects `SyncNever` (`main.go:124-126`) and chaos uses it (`chaos/scenario.go:139`). Tests: `durability_test.go:92-310` |
| 2 | Metadata via temp file, fsync, rename, dir fsync | GAP (design differs) | No metadata file. HardState is a WAL record (`wal.go:363-378`, `record.go:26`). Dir fsync on segment create (`wal.go:295-303`) and prefix removal (`wal.go:463`); snapshots use temp, fsync, rename, dir fsync (`snapshot.go:113-133`). Equivalent in effect, not as written |
| 3 | term > current: update, clear vote, step down | OK | `node.go:359-366`, `430-445` (persists before changing state). Test `durability_test.go:175` |
| 4 | term < current: reject and reply with current term | GAP (partial) | Only `MsgVoteRequest` is answered (`node.go:368-377`). Stale AppendRequest, Heartbeat and InstallSnapshot are dropped silently. Liveness only, not safety. Whether the transport replies: UNKNOWN |
| 5 | RequestVote: one vote per term and up-to-date check | OK | `election.go:42-47`, `log.go` `isUpToDate`. Tests `election_test.go:85,132,156` |
| 6 | Reset election timer only on valid AE, grant, or starting an election | GAP (minor) | `becomeFollower` calls `reset()` on any higher-term message, including a vote request that is then rejected (`node.go:359-366`, `441-443`, `515-523`), and on `voteLost` (`election.go:80`). The AE reset at `replication.go:103-104` follows the term check, so it is valid. PreVote does not reset (`election.go:115-137`) |
| 7 | AE rejects on prev mismatch with ConflictTerm and ConflictIndex | OK | `log.go:82-84` plus `conflictHint`; `replication.go:111-121`, `176-196` |
| 8 | Truncate only on a real conflict | OK (code) | `log.go:86-104`, `findConflict` at `:109`; WAL replay resolves overwrites without deleting (`wal.go:259-265`). A test that a stale duplicate AE does not truncate: UNKNOWN |
| 9 | Follower commit = min(leaderCommit, last new entry) | OK | `log.go:105`. Read heartbeats carry no CommitIndex (`readonly.go:97-108`) |
| 10 | Commit only current-term entries by counting (§5.4.2) | OK | `replication.go:237-240`. Test `replication_test.go:273` |
| 11 | No-op on election win | OK | `node.go:489-497` |
| 12 | nextIndex and matchIndex init; back up with hints; never below snapshot | OK | `node.go:484-486`; `replication.go:64-75` falls back to `sendSnapshot`; backoff clamps to 1 (`:190`) |
| 13 | Single apply goroutine, in order, never twice | OK | `go n.run()` at `internal/node/node.go:248`, `processReady` `:497-537`; core panics on out-of-order `appliedTo` (`log.go:150-160`) |
| 14 | Stale responses ignored | OK | Lower term dropped (`node.go:368-377`); response guards `election.go:67`, `139-145`; match is monotonic (`replication.go:157-162`) |
| 15 | Clock, Rand, Transport injected; no wall clock in `internal/raft` | OK in spirit, GAP in letter | No `time` import; core is tick-driven (`node.go:308`); real ticker only in `internal/node/node.go:359`. Rand is `Config.Rand *rand.Rand` (`node.go:50`), a concrete type, defaulting to `rand.New(rand.NewSource(int64(cfg.ID)))` (`:176-179`). `Transport` lives in `internal/node/node.go:27`. No `Clock` interface. Enforced by `internal/determinism/determinism_test.go:30,51,71` |
| 16 | Randomized timeout; heartbeat 100ms, election 600-1200ms | GAP | Tick 100ms, heartbeat 1 tick, election 10 ticks randomized to 10-19 ticks, so **1.0-1.9 s** (`internal/node/node.go:63-65`, `raft/node.go:526`). Flags can override (`main.go:63-65`) |

## 5. RUNBOOK §7.3 checklist
| # | Item | Verdict | Evidence |
|---|---|---|---|
| 1 | Record `[len u32][crc32c u32][type u8][payload]`, ~64 MB segments, monotonic names | GAP (partial) | Framing matches (`record.go:53-64`, Castagnoli `:51`). Default segment is **16 MiB** (`wal.go:36`). Names `<seq>-<firstIndex>.wal` (`wal.go:158-160`) |
| 2 | Torn tail truncated; mid-log corruption refuses start | GAP (partial) | Short tail of the last segment: truncate plus dir fsync (`wal.go:196-212`). Mid-log or older-segment damage: `ErrCorruptWAL`. **A bad CRC or oversize length at the tail of the last segment also refuses to start** (only `ErrTornRecord` is repaired, `wal.go:200`), which is stricter than the RUNBOOK. Tests `wal_test.go:199,223,293,320` |
| 3 | Truncate-suffix and prefix are crash-safe | OK (code), UNKNOWN (tests) | Suffix: appended records resolved at replay, nothing deleted (`wal.go:259-265`). Prefix: snapshot and meta record written first (`disk.go:257-271`); `TruncateBefore` re-records HardState before deleting (`wal.go:431-469`). `disk_test.go:243` reopens the store, which is not a crash at each syscall |
| 4 | Crash at any write boundary satisfies the invariants, tested with a fault-injecting FS | GAP | README:227 admits there is no filesystem seam. Partial: torn tail at every cut point (`wal_test.go:223`), on-disk chaos (`chaos/scenarios_test.go:1202`). No fsync-error or lose-unsynced-data test |

## 6. Other items noted, and what does not exist
- OK: dedupe by `(clientID, seq)` inside the state machine, session table in the snapshot, bounded sessions (`statemachine/kv.go:109-142`, `:236`, `:291`). NOT_LEADER with a leader address (`transport/kvserver.go:67-76`). ReadIndex requires an entry committed in the leader's term (`readonly.go:57`, `:111-117`). Snapshots are written atomically (`snapshot.go:113-133`); InstallSnapshot streams 1 MiB chunks (`transport/peer.go:259-278`).
- UNKNOWN: both InstallSnapshot follower branches (not traced); a client library that follows leader hints (only generated stubs and `cmd/raftkv-bench` found); whether `time.Sleep` in 7 test files masks flakiness.
- Not present: `read_mode: log|readindex` switch (ReadIndex is the only path); snapshots are held fully in memory at both ends (README:225).
- Absent from the repo: Makefile, `.golangci.yml`, `scripts/`, `.githooks/`, CODEOWNERS, PR template, `docs/INVARIANTS.md`, `docs/NOTES.md`, `docs/tasks/`, nightly workflow, gitleaks/buf/guard CI jobs, `cmd/chaosrun`, `cmd/gateway`, `cmd/raftkvctl`, admin service, porcupine cross-check, mutation (`faultinject`) suite, JSONL history with `info` ops, checker shrinker and HTML report, `infra/`, `web/`.
- Checker note: `chaos/checker.go` has pending and unknown op statuses (`:82-83`, `:122`, `:205`), so indeterminate ops are modelled; correctness not reviewed.
