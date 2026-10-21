# RaftKV: Agent Runbook

> Save as `docs/RUNBOOK.md`. Copy Appendix A to `CLAUDE.md` (Claude agents) and Appendix F-3 to `AGENTS.md` (ChatGPT/Codex reviewer) at the repo root.
> Owner: Tushar (GitHub: MenaceHecker). Written 2026-10-21. Target finish: 2026-12-08.
> Audience: Claude agents (planner, executor, runner) with a ChatGPT reviewer, all under human review.

---

## 0. How to use this runbook

1. Start every agent session with its role prompt from Appendix B or F, naming the task ID. One role per session.
2. One task = one branch = one PR. Task IDs (`T3.1`, `D4`, ...) are in §9 and §10.
3. If this runbook conflicts with the README or the code, **stop and ask**. Do not silently pick one.
4. Every task ends with a report in the format in §5.6.
5. Anything marked **[HUMAN]** is not for agents.

---

## 1. Context and goals

**Project:** a distributed key-value store implementing Raft from scratch in Go with gRPC: leader election, log replication, WAL persistence, snapshotting and log compaction, linearizable client semantics.

**Why it exists:** it fills the portfolio gap around consensus, replication, and fault tolerance. The headline artifact is the **chaos harness plus a self-written linearizability checker, proven by bug injection**.

**Goals**
- G1. Finish the core (WAL, snapshots, KV semantics), green under `go test -race`.
- G2. A public live demo on AWS: a 3-AZ cluster, a gateway API, and a web dashboard showing leader kill, election, partition, and catch-up.
- G3. Chaos harness and linearizability verifier that **catches deliberately injected bugs**.
- G4. README with real results, architecture diagrams, a 2-3 minute demo video, and a LinkedIn write-up.

**Success criteria (all measurable)**
- `go test -race -count=1 ./...` is green; `-count=50` on `internal/raft` is green nightly.
- 200+ seeded simulated chaos runs pass the verifier; every injected bug in §8.6 is caught.
- Live demo URL up; the "kill leader" flow elects a new leader in under 3 seconds and the dashboard shows it.
- Cost stays under the monthly ceiling in §10.9.

**Non-goals:** dynamic membership change (stretch only), multi-Raft sharding, multi-region, Kubernetes, authentication for KV data beyond the demo guards.

### 1.1 Task 0: reconcile reality (do this first, read-only)

The phase map below is an **assumption**. The README is authoritative.

| Phase | Assumed scope |
|---|---|
| P1 | Leader election |
| P2 | Log replication |
| P3 | Persistence: WAL, crash recovery (status: mostly done) |
| P4 | KV state machine, client semantics, snapshotting |
| P5 | Chaos harness and linearizability verifier |
| P6 | Docs, demo, polish |

**Task 0 for the agent:**
1. Read README, `go.mod`, the directory tree, and the test list.
2. Write `docs/STATUS.md`: actual phases, what exists, what is missing, test counts, how to run everything, known gaps vs. §7 invariants.
3. Propose edits to the phase map and task IDs in this runbook as a diff in the PR description. Do **not** change code.

---

## 2. Non-negotiable rules for agents

1. **Never commit to `main`.** Branch, commit, push the branch, open a PR. A human merges.
2. **Never weaken a test to make it pass.** No deleting assertions, no `t.Skip`, no widening tolerances, no raising timeouts more than 2x, no retry loops that mask failures. If a test fails, the bug is in the code or the test is wrong, and you must explain which in the PR.
3. **Test changes and Raft-core changes go in separate commits.** A commit that touches both `internal/raft/**` non-test code and `*_test.go` is rejected.
4. **Run the gates (§6) before every commit.** No `--no-verify`. No `git push --force`. No rewriting pushed history. No `git reset --hard` outside your own worktree.
5. **Persist before responding.** Any change to `currentTerm`, `votedFor`, or the log must be durably persisted (fsync) before replying to the RPC that caused it (§7).
6. **No flaky tests.** A flaky test is a real bug in the code or the test. Fix the root cause. Never `t.Skip` or add sleeps to hide it.
7. **Determinism first.** Raft logic must not call `time.Now()`, `time.Sleep`, `rand` or the network directly. Use the injected `Clock`, `Rand`, and `Transport` interfaces so the simulator can control them.
8. **Secrets never enter git.** No keys, tokens, `.tfvars`, `.tfstate`, `.env`. Use SSM Parameter Store and GitHub secrets.
9. **AWS safety:** agents may run `terraform fmt`, `validate`, and `plan`. Agents may **not** run `terraform apply` or `destroy`, and may not create IAM users or keys. **[HUMAN]** applies after reading the plan.
10. **Small commits.** One logical change each, under ~300 changed lines where possible.
11. **Do not touch files outside your task card's scope or your role's permissions (§3)** without asking.
12. **If unsure about Raft semantics, cite the paper section** (Raft extended paper, Figure 2, §5.4.2, §6, §7, §8) in the code comment or PR. Do not guess.
13. **Report honestly.** If a gate failed, a task is partial, or you skipped something, say so at the top of the report.

---

## 3. Agent roles (Claude: planner, executor, runner. ChatGPT: reviewer)

| Role | Model | Can do | Must not do | Produces |
|---|---|---|---|---|
| **Planner** | Claude (read-only session) | Read repo, paper notes, INVARIANTS; write task cards under `docs/tasks/` | Edit code, tests, infra | `docs/tasks/<ID>.md` (template in Appendix F) |
| **Executor** | Claude Code (own git worktree) | Implement the task card: tests first, commits, push branch, open PR | Run `terraform apply/destroy`; merge; edit outside the card's scope; declare its own gates "green" as final | Commits and PR |
| **Runner** | Claude Code (fresh clone or detached worktree of the **pushed** branch, edit tools disabled) | Run gates, `test-flaky`, `chaos-smoke`, mutation suite, `terraform validate/plan`; capture raw output and exit codes | Edit any file; "fix" failures; summarize away failures | `RUN_REPORT.md` (format in Appendix F) |
| **Reviewer** | ChatGPT (separate account/session; read-only) | Review the review packet (§5.5); propose failing scenarios and missing tests | Rewrite code; approve without listing what it could not verify | Structured review (schema in §5.5) |
| **Human** | You | Merge, apply infra, DNS, credentials, final read of raft-core, verifier, IAM/SG, gateway auth | | |

**Rules for the setup**
- **Separate sessions, fresh context per role.** The planner, executor, and runner never share a conversation. If they did, the runner would inherit the executor's assumptions and stop being an independent check.
- **The runner's report, not the executor's summary, is the evidence.** CI is the final source of truth.
- **Restrict the runner via Claude Code permission settings** (deny the edit and write tools for that session, allow only the Makefile targets and `terraform plan`).
- **Honest limitation:** planner, executor, and runner are one model family, so they share blind spots. The reviewer is your only cross-vendor check, and the real independent oracles are the invariant monitor in the simulator, the mutation suite (§8.6), and your own reading of the raft core. Do not treat three Claude agreeing as three opinions.
- **Parallelism:** the planner can write the next task card while the executor works on the current one. Each executor task gets its own worktree: `git worktree add ../raftkv-exec -b feat/p4-install-snapshot`.

**Per-task pipeline**
1. Planner writes the task card, human skims it and approves.
2. Executor implements it (tests first; separate commits for tests and core), pushes the branch.
3. Runner checks out the pushed branch and produces `RUN_REPORT.md`.
4. `scripts/review-packet.sh` builds the packet; the reviewer returns findings.
5. Executor fixes BLOCKER and MAJOR findings with new commits. Runner re-runs. Reviewer re-reviews the delta. Repeat until no blockers.
6. Human reads raft-core, verifier, IAM/SG, and gateway-auth diffs, then merges.

**Human review is required** for: `internal/raft`, `internal/wal`, `internal/snapshot`, `internal/verify`, all IAM and security groups, gateway auth and rate limiting, and any test modified (guard script).

---

## 4. Repository layout (target; reconcile with the real tree in Task 0)

```
raftkv/
├── CLAUDE.md                  # Appendix A
├── AGENTS.md                  # reviewer rules (Appendix F-3), for ChatGPT/Codex
├── README.md
├── Makefile
├── go.mod
├── .golangci.yml
├── .github/
│   ├── workflows/ci.yml, nightly.yml
│   ├── CODEOWNERS
│   └── pull_request_template.md
├── api/                       # .proto: raft.proto, client.proto, admin.proto
├── cmd/
│   ├── raftkvd/               # node binary
│   ├── raftkvctl/             # CLI client
│   ├── gateway/               # public API + chaos controller + verifier runner
│   └── chaosrun/              # standalone chaos+verify runner (also used in CI)
├── internal/
│   ├── raft/                  # core: election, replication, commit, apply
│   ├── wal/                   # segmented write-ahead log
│   ├── snapshot/              # snapshot files, chunking
│   ├── kv/                    # state machine, sessions/dedupe
│   ├── transport/             # gRPC transport + fault-injecting wrapper
│   ├── sim/                   # deterministic simulator (virtual clock, net, disk faults)
│   ├── chaos/                 # nemesis, workload generator, history recorder
│   ├── verify/                # linearizability checker, report generator
│   └── admin/                 # admin service, event stream, metrics
├── web/                       # dashboard (Vite + React + TypeScript)
├── infra/
│   ├── bootstrap/             # state bucket
│   └── env/demo/              # main stack
├── scripts/
│   ├── guard-tests.sh
│   ├── rolling-update.sh
│   └── demo-up.sh / demo-down.sh
└── docs/
    ├── RUNBOOK.md, STATUS.md, ARCHITECTURE.md, INVARIANTS.md, CHAOS_RESULTS.md, DEMO_SCRIPT.md
```

**Ports:** Raft peer gRPC `:7000`, client gRPC `:7001`, admin gRPC `:9000` (gateway only), metrics HTTP `:2112`.

---

## 5. Git and PR workflow

### 5.1 Branches
`feat/<phase>-<slug>`, `fix/<slug>`, `test/<slug>`, `infra/<slug>`, `docs/<slug>`, `chore/<slug>`. Example: `feat/p4-install-snapshot`.

### 5.2 Commits (Conventional Commits)
```
<type>(<scope>): <imperative summary, <=72 chars>

<why this change, what invariant it protects, paper section if relevant>

Task: T4.5
Agent: claude-code
Gates: fmt=ok vet=ok lint=ok race=ok
```
- **types:** `feat fix test refactor perf docs chore ci infra`
- **scopes:** `raft wal kv snapshot transport admin gateway verify chaos sim web infra`
- Commit early and often within a task. Never batch an entire task into one commit.

### 5.3 Merge policy
Squash-merge per PR; the PR title is a conventional commit message. Branch protection on `main`: PR required, CI green required, no force pushes, linear history.

### 5.4 Pre-commit hooks (`.githooks/pre-commit`, enable with `git config core.hooksPath .githooks`)
1. `gofmt -l` must output nothing. 2. `go vet ./...` 3. `go test -race -short ./...` 4. `gitleaks protect --staged` 5. `scripts/guard-tests.sh`

### 5.5 Independent review by ChatGPT (the reviewer)
Build the packet with `scripts/review-packet.sh` and paste it (or attach it) into a fresh ChatGPT session using the reviewer prompt in Appendix F-2. If you use Codex CLI as the reviewer so it can read the repo and run tests, run it in its read-only mode.

**Packet contents:** the task card, `git diff main...HEAD`, `docs/INVARIANTS.md`, `RUN_REPORT.md`, the guard-tests output, and the full text of every changed file under `internal/raft`, `internal/wal`, `internal/snapshot` (the diff alone hides context).

**Required output schema:**
```
VERDICT: approve | changes-requested
FINDINGS:
- [BLOCKER|MAJOR|MINOR|NIT] file:line | invariant (§7 ref) | evidence | failing scenario or test to add
TEST REVIEW: vacuous or weakened tests, missing negative cases, nondeterminism
NOT VERIFIED: everything the reviewer could not check (it cannot run code unless using Codex)
```
For any suspected safety bug, the reviewer must supply a concrete interleaving (a short sequence of events across nodes) that would violate an invariant. "Looks suspicious" is not a finding.

`scripts/review-packet.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
task="$1"; base="${2:-origin/main}"
out="review-packet-$task.md"
{
  echo "# Review packet: $task"; echo; echo "## Task card"; cat "docs/tasks/$task.md"
  echo; echo "## Invariants"; cat docs/INVARIANTS.md
  echo; echo "## Run report"; cat RUN_REPORT.md
  echo; echo "## guard-tests"; scripts/guard-tests.sh "$base" 2>&1 || true
  echo; echo "## Diff"; echo '```diff'; git diff "$base"...HEAD; echo '```'
  echo; echo "## Full text of changed core files"
  for f in $(git diff --name-only "$base"...HEAD | grep -E '^internal/(raft|wal|snapshot)/.*\.go$' | grep -v _test.go); do
    echo "### $f"; echo '```go'; cat "$f"; echo '```'
  done
} > "$out"
echo "$out"
```

### 5.6 Agent task report (end of every task)
```
STATUS: done | partial | blocked
GATES: fmt/vet/lint/race/test results (exact commands + pass/fail)
CHANGED: files and why (grouped)
INVARIANTS TOUCHED: list, with how each is tested
TESTS: added/changed; confirm none weakened
RISKS / UNVERIFIED: be specific
QUESTIONS FOR HUMAN:
```

### 5.7 `scripts/guard-tests.sh`
```bash
#!/usr/bin/env bash
# Flags test-weakening patterns in the diff vs main. Exit 1 if found.
set -euo pipefail
base="${1:-origin/main}"
diff=$(git diff "$base"...HEAD -U0 -- '*_test.go' || true)
bad=0
if echo "$diff" | grep -E '^\+.*\bt\.Skip(Now)?\(' ; then echo "ERROR: added t.Skip"; bad=1; fi
if echo "$diff" | grep -E '^-\s*(require|assert)\.' | head -1 >/dev/null; then
  removed=$(echo "$diff" | grep -cE '^-\s*(require|assert)\.|^-.*t\.(Fatal|Error)f?\(' || true)
  added=$(echo "$diff" | grep -cE '^\+\s*(require|assert)\.|^\+.*t\.(Fatal|Error)f?\(' || true)
  if [ "$removed" -gt "$added" ]; then echo "WARN: net assertions removed ($removed removed, $added added)"; bad=1; fi
fi
if echo "$diff" | grep -E '^\+.*(time\.(Second|Millisecond)\s*\*\s*[0-9]{3,})' ; then echo "WARN: large timeout added"; bad=1; fi
exit $bad
```
A failure means the PR needs the label `tests-modified-reviewed`, set by the human only.

---

## 6. Quality gates

### 6.1 Makefile targets
```makefile
fmt:        ; gofmt -l -w .
vet:        ; go vet ./...
lint:       ; golangci-lint run
test:       ; go test -race -count=1 -timeout 10m ./...
test-flaky: ; go test -race -count=20 -timeout 30m ./internal/raft/... ./internal/wal/... ./internal/kv/...
cover:      ; go test -race -coverprofile=cover.out ./... && go tool cover -func=cover.out
proto:      ; buf generate
chaos-smoke:; go run ./cmd/chaosrun -sim -seeds 1-20 -duration 30s
release:    ; scripts/release.sh   # cross-compiles linux/arm64, uploads to S3 (D3)
```

### 6.2 `.golangci.yml` enable
`errcheck govet staticcheck unused ineffassign gocritic gosec bodyclose misspell revive` and `-race`-aware `copyloopvar`. `gosec` on; no blanket `//nolint` without a reason comment.

### 6.3 CI (`.github/workflows/ci.yml`)
Jobs on every PR:
- **go:** setup-go with `go-version-file: go.mod`; `go vet`; `golangci-lint`; `go test -race -count=1 -timeout 10m ./...`; `go test -race -count=3 ./internal/raft/...`; coverage artifact.
- **guard:** run `scripts/guard-tests.sh` (fail unless label `tests-modified-reviewed`).
- **secrets:** gitleaks.
- **proto:** `buf lint` and `buf breaking --against main`.
- **terraform** (path filter `infra/**`): `terraform fmt -check`, `terraform init -backend=false`, `terraform validate`, `tflint`, `trivy config infra/`.
- **web** (path filter `web/**`): `npm ci`, `npm run lint`, `npm run build`.

`nightly.yml`: `go test -race -count=50 ./internal/raft/...` and `chaosrun -sim -seeds 1-200`; uploads failing seeds and histories as artifacts; opens an issue on failure.

### 6.4 Flakiness policy
A test that fails once in `-count=20` is a P0 bug. Reproduce with the failing seed, find the root cause, fix, and add a regression test. Log the root cause in the PR.

---

## 7. Raft correctness invariants (`docs/INVARIANTS.md` must mirror this; every item needs a test)

### 7.1 Safety properties (checked continuously in the simulator after every step)
- **Election Safety:** at most one leader per term.
- **Leader Append-Only:** a leader never overwrites or deletes its own log entries.
- **Log Matching:** if two logs contain an entry with the same index and term, the logs are identical up to that index.
- **Leader Completeness:** a committed entry is present in the logs of all leaders of later terms.
- **State Machine Safety:** no two nodes apply different commands at the same index.

### 7.2 Implementation checklist (paper Figure 2 plus known pitfalls)
**Persistent state:** `currentTerm`, `votedFor`, `log[]` are fsynced **before** replying to any RPC that changed them, and before a leader counts its own append toward commit.
- [ ] Metadata (`currentTerm`, `votedFor`) written via temp file, fsync, rename, then **fsync the directory**.
- [ ] Any RPC or response with `term > currentTerm`: update term, clear `votedFor`, step down to follower.
- [ ] RPC with `term < currentTerm`: reject and reply with the current term.
- [ ] **RequestVote:** grant only if not voted for another candidate this term **and** the candidate's log is at least as up-to-date (higher `lastLogTerm`, or equal and `lastLogIndex >=` ours).
- [ ] Reset the election timer **only** on: a valid AppendEntries from the current leader, granting a vote, or starting an election. Not on stale or rejected RPCs.
- [ ] **AppendEntries:** reject if `prevLogIndex/prevLogTerm` do not match; return `ConflictTerm`, `ConflictIndex` hints for fast backup.
- [ ] **Truncate the log only on an actual conflict** (same index, different term). Never truncate on a stale or duplicated AppendEntries that arrives late. This is the classic bug.
- [ ] Follower `commitIndex = min(leaderCommit, index of last new entry)`.
- [ ] **Leader commits only entries from the current term by counting replicas** (§5.4.2). Earlier-term entries commit indirectly.
- [ ] On winning an election, the leader appends a **no-op entry** in its own term so earlier entries can commit and reads become safe (§8).
- [ ] On becoming leader: `nextIndex = lastLogIndex+1` for all, `matchIndex = 0`. On rejection, back up using hints, never below `snapshotIndex+1` (otherwise send InstallSnapshot).
- [ ] Apply entries in order from a single apply goroutine: `lastApplied <= commitIndex`; never apply out of order or twice.
- [ ] Stale responses (from an older term or a previous round) are ignored.
- [ ] The `Clock`, `Rand`, and `Transport` interfaces are injected (see rule 7). No wall-clock calls in `internal/raft`.
- [ ] Election timeout randomized per attempt; heartbeat interval well below election timeout (defaults: heartbeat 100ms, election 600-1200ms, configurable).

### 7.3 Persistence and recovery
- [ ] WAL record: `[len u32][crc32c u32][type u8][payload]`, segment files ~64 MB, monotonically increasing names.
- [ ] On open: scan segments; a bad CRC or short record at the **tail** of the last segment is a torn write, so truncate there. A bad CRC **mid-log** is corruption: refuse to start and report loudly.
- [ ] Truncate-suffix (conflict resolution) and truncate-prefix (compaction) are crash-safe: persist the intent before deleting data.
- [ ] After a crash at any write boundary, restart yields a state that satisfies the invariants (tested with the fault-injecting filesystem).

### 7.4 Client semantics
- [ ] Every client request carries `(clientID, seq)`. The state machine dedupes by session and returns the cached result for replays, which gives exactly-once effect under retries. The session table is part of the snapshot.
- [ ] **Linearizable reads:** either (a) route reads through the log, or (b) **ReadIndex**: the leader must have committed an entry in its current term, records `readIndex = commitIndex`, confirms leadership with a heartbeat quorum round, then waits for `lastApplied >= readIndex`. Config `read_mode: log | readindex`, default `log` until ReadIndex passes the verifier. Leader leases are out of scope (they rely on clocks).
- [ ] A deposed leader must never answer a read from local state.
- [ ] Non-leaders return `NOT_LEADER` with a leader hint; the client library follows hints and retries with backoff.

### 7.5 Snapshots
- [ ] Snapshot = `{lastIncludedIndex, lastIncludedTerm, kv state, session table}`, written atomically (temp, fsync, rename, directory fsync).
- [ ] Only then truncate the WAL prefix up to `lastIncludedIndex`.
- [ ] **InstallSnapshot:** chunked; follower discards the whole log if no entry matches `(lastIncludedIndex, lastIncludedTerm)`, otherwise keeps the suffix; resets the state machine from the snapshot.
- [ ] Recovery = latest valid snapshot plus WAL replay.

---

## 8. Test architecture

### 8.1 Layers
| Layer | What | Where |
|---|---|---|
| L0 | Unit tests (log ops, WAL codec, state machine) | each package |
| L1 | **Deterministic simulation**: N nodes, virtual clock, seeded RNG, in-memory network (drop, duplicate, reorder, delay, partition), fault-injecting disk (fsync failure, torn write, crash at write boundary). Invariants from §7.1 asserted after every simulated step. | `internal/sim` |
| L2 | Integration: real processes, real gRPC, real disk | `test/integration` |
| L3 | Chaos: nemesis against L1 or L2 clusters with concurrent clients | `internal/chaos` |
| L4 | Linearizability verification of recorded histories | `internal/verify` |

Every simulator run prints its **seed**. Any failure must be reproducible with `chaosrun -sim -seed N`.

### 8.2 History format (JSONL, one event per line)
```json
{"id":17,"client":3,"type":"invoke","op":"cas","key":"k1","arg":{"old":"a","new":"b"},"t":1234567}
{"id":17,"client":3,"type":"ok","op":"cas","key":"k1","ret":{"swapped":true},"t":1234990}
{"id":18,"client":4,"type":"info","op":"put","key":"k2","t":1235000}
```
- `ok` = completed successfully, `fail` = definitely did not take effect, `info` = **indeterminate** (timeout, connection lost). Indeterminate ops may or may not have taken effect and must be treated as concurrent with everything after their invoke. Mishandling this is the most common checker bug.

### 8.3 Checker
- Model: per-key register with `put`, `get`, `delete`, `cas`. **Partition the history by key** (keys are independent), which makes checking tractable.
- Algorithm: Wing and Gong with Lowe's optimizations (memoize on `(linearized-set, state)`), implemented from scratch in `internal/verify`.
- **Oracle cross-check:** in tests only, import `github.com/anishathalye/porcupine` and assert both checkers agree on a corpus of histories (hand-written good and bad ones plus randomly generated). Porcupine is a test oracle, not a dependency of the shipped binary.
- On failure, output a **minimal counterexample** (shrink the history) and an HTML visualization.

### 8.4 Nemesis (fault injectors)
`kill` (crash and restart, WAL recovery), `pause` (freeze process, then resume), `partition` (symmetric, asymmetric, isolate-leader, majority/minority split), `delay`, `drop`, `duplicate`, `reorder`, `disk` (fsync error, torn tail), `restart-all` (full cluster power loss). Schedules are randomized from the seed and always include periods of full healing so liveness is also checked ("a leader is elected and a write succeeds within X seconds of heal").

### 8.5 Workload
N concurrent clients (default 5-10), small key space (5-20 keys) to force contention, op mix 40% get / 40% put / 10% cas / 10% delete, unique values per put so reads are attributable, client library retries with the same `(clientID, seq)`.

### 8.6 Mutation (bug-injection) suite: the proof the checker works
Behind build tag `faultinject`, add switches that introduce **known bugs**. The verifier or the invariant monitor **must** flag each. CI runs the suite and fails if any bug goes undetected.
1. Skip fsync before acking AppendEntries.
2. Truncate the log on stale AppendEntries.
3. Commit previous-term entries by counting replicas.
4. Grant votes without the up-to-date log check.
5. Serve reads from a deposed leader's local state.
6. Skip client dedupe (retries double-apply).
7. Leader skips the no-op after election.
8. Snapshot omits the session table.
9. Apply entries beyond `commitIndex`.

Record the detection result for each in `docs/CHAOS_RESULTS.md`. This table is the headline of the project.

---

## 9. Core phase plan

Each task: **agent / branch / acceptance**. Acceptance always includes all gates in §6 passing.

### Phase 3 (finish): persistence
| ID | Task | Agent | Acceptance |
|---|---|---|---|
| T3.0 | Audit current persistence against §7.2-7.3, write `docs/audit-p3.md` (read-only) | Claude (executor) | Every checklist item marked OK / GAP with file:line evidence |
| T3.1 | Atomic metadata persistence | Claude (executor) | Crash between any two syscalls never loses or corrupts `currentTerm`/`votedFor` (fault-fs test) |
| T3.2 | Segmented WAL with CRC, torn-tail handling, truncate-suffix/prefix | Claude (executor) | Codec fuzz test (`go test -fuzz`) 60s clean; torn-tail and mid-log corruption tests |
| T3.3 | fsync policy and group commit (batch appends, one fsync) | Claude (executor) | No ack before fsync verified by fault-fs "lose unsynced data" test |
| T3.4 | Crash-recovery simulation at every write boundary | Claude (executor) | Invariant monitor green over 500 seeds |
| T3.5 | Close remaining README Phase 3 items | Claude (executor) | README Phase 3 checklist fully checked |

### Phase 4: state machine, client semantics, snapshots
| ID | Task | Agent | Acceptance |
|---|---|---|---|
| T4.1 | KV state machine: protobuf command encoding; `Put/Get/Delete/CAS`; deterministic apply | Claude (executor) | Same log yields byte-identical state on all nodes (test) |
| T4.2 | Client sessions and dedupe `(clientID, seq)` | Claude (executor) | Retried CAS applies once; session table has bounded size with expiry policy documented |
| T4.3 | Read path: log reads first, then ReadIndex behind `read_mode` | Claude (executor) | Deposed-leader read test; ReadIndex only becomes default after passing the verifier (T5.x) |
| T4.4 | Snapshot create/restore, size or index trigger, WAL prefix truncation | Claude (executor) | Restart from snapshot + WAL equals full replay (property test) |
| T4.5 | InstallSnapshot RPC, chunked, both follower branches | Claude (executor) | Lagging follower and fresh follower both catch up in sim; crash mid-install recovers |
| T4.6 | Client library (leader hints, backoff, same-seq retry) and `raftkvctl` | Claude (executor) | Integration test: kill leader mid-workload, client completes |
| T4.7 | **Admin service, metrics, event stream** (needed by the demo): `Status`, `Crash(restartDelay)`, `Pause(d)`, `SetPartition(blockedPeers)`, `SetNetem(dropPct, delayMs)`, `Events` (server stream: role changes, term, commit/applied index, snapshot events, elections). Faults are applied in a wrapper around `Transport`. | Claude (executor) | Admin RPCs callable only on `:9000`; fault wrapper unit-tested; events monotonic and complete |
| T4.8 | Stretch: PreVote and CheckQuorum | Claude (executor) | Rejoining partitioned node does not disrupt a healthy leader (sim test) |

### Phase 5: chaos and linearizability
| ID | Task | Agent | Acceptance |
|---|---|---|---|
| T5.1 | Simulator completeness (virtual clock, net faults, disk faults) if not already present | Claude (executor) | Same seed gives identical event trace (test) |
| T5.2 | History recorder with `info` semantics | Claude (executor) | Format per §8.2; round-trip test |
| T5.3 | Linearizability checker (from scratch) with key partitioning and shrinker | Claude (executor) | Passes hand-written good/bad corpus (30+ histories) |
| T5.4 | Porcupine cross-check (test-only) | Claude (executor) | Both checkers agree on corpus plus 1000 random histories |
| T5.5 | Nemesis library | Claude (executor) | Each fault has a test proving it takes effect |
| T5.6 | Workload generator and `chaosrun` CLI | Claude (executor) | `chaosrun -sim -seeds 1-200` passes |
| T5.7 | Mutation suite (§8.6) | Claude (executor) | All 9 bugs detected; CI enforces |
| T5.8 | Report generator (Markdown + HTML: ops, faults, availability timeline, verdict, counterexample) | Claude (executor) | Report renders for pass and fail runs |
| T5.9 | Nightly CI with failing-seed artifacts | Claude (executor) | Nightly green for 7 consecutive days before launch |

### Phase 6: polish
| ID | Task | Agent | Acceptance |
|---|---|---|---|
| T6.1 | `ARCHITECTURE.md` with diagrams (Mermaid): node internals, RPC flows, snapshot flow | Either | Human approves |
| T6.2 | `CHAOS_RESULTS.md` real numbers: seeds run, ops checked, mutation table | Either | Numbers reproducible via one documented command |
| T6.3 | README: what it is, how to run locally in 3 commands, demo link, results, honest limitations, "built with Claude agents and reviewed with ChatGPT, under my design and review" | Human | |
| T6.4 | Demo script and video | **[HUMAN]** | 2-3 min, §10.10 |

### Interview-readiness gate (human, after each phase)
Agents write fast; you must be able to explain every line. After each phase, write 5 sentences per §7.2 item you touched, in your own words, in `docs/NOTES.md`. If you cannot explain a design choice, ask the agent to walk you through it, then verify against the paper.

---

## 10. Demo and AWS deployment track

### 10.1 Architecture
```
Internet
   │ HTTPS 443
   ▼
[ Gateway  t4g.micro, public subnet, Elastic IP, api.<domain> ]
   │  gRPC client :7001 and admin :9000 (private)
   ├───────────────┬───────────────┐
   ▼               ▼               ▼
[node1 AZ-a]   [node2 AZ-b]   [node3 AZ-c]   private subnets, no public IP, no NAT
 10.20.1.10     10.20.2.10     10.20.3.10     raft :7000 between nodes
   │ S3 Gateway Endpoint (free): binary download only

Dashboard: static site on Vercel (live mode + recorded replay mode)
```
Why this shape: no NAT gateway (about $32/month) and no public IPs on nodes (public IPv4 costs about $3.65/month each). Nodes pull their binary from S3 through a free gateway endpoint. Debug access uses EC2 Instance Connect Endpoint (no public IP needed).

### 10.2 Cost model (us-east-1, on-demand, approximate; **verify with the AWS Pricing Calculator**)
| Item | Monthly |
|---|---|
| 3 × t4g.micro (nodes) | ~$18.40 |
| 1 × t4g.micro (gateway) | ~$6.10 |
| 1 Elastic IP (public IPv4) | ~$3.65 |
| 4 × 8 GB gp3 | ~$2.60 |
| S3, SSM, data transfer, misc | ~$1-2 |
| **Total** | **~$31-35/month** |

Set `credit_specification { cpu_credits = "standard" }` on every t4g instance. The default is *unlimited*, which can bill surplus CPU. **Do not `terraform apply` the compute stack until the demo is ready (D4)**: stopped work costs nothing.

### 10.3 Tasks
| ID | Task | Who |
|---|---|---|
| **D0** | Account hygiene: root MFA, SSO/IAM Identity Center profile for daily use, no long-lived keys, set region `us-east-1`, check **credit balance and expiry date** under Billing, create AWS Budgets (see D1) | **[HUMAN]** |
| **D1** | `infra/bootstrap`: S3 state bucket (versioning, encryption, public access blocked), backend with native locking (`use_lockfile`, Terraform >= 1.10). Budgets: monthly cost budget with credits **excluded** (so you see gross burn) alerting at $20/$30/$40 actual and 100% forecast to your email; Cost Anomaly Detection. | Claude (executor) writes, **[HUMAN]** applies |
| **D2** | Network module: VPC `10.20.0.0/16`; public subnet `10.20.0.0/24` (AZ-a, IGW route) for the gateway; three private subnets `10.20.1.0/24`, `10.20.2.0/24`, `10.20.3.0/24` in three distinct AZs (use `data.aws_availability_zones`), **no NAT, no IGW route**; S3 gateway endpoint on private route tables; EC2 Instance Connect Endpoint in one private subnet. | Claude (executor), **[HUMAN]** reviews SGs |
| **D3** | `make release`: cross-compile `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, embed version via `-ldflags`, upload `raftkvd` and `gateway` to a versioned S3 artifacts bucket with sha256 files. | Claude (executor) |
| **D4** | Node compute: one instance per AZ with **fixed private IPs** (`.10` in each subnet), AL2023 arm64, instance profile limited to `s3:GetObject` on the artifacts prefix, encrypted gp3, IMDSv2 required, user-data that fetches binary, verifies sha256, installs config and systemd unit, starts service. | Claude (executor), **[HUMAN]** applies |
| **D5** | Gateway: Go service in `cmd/gateway`, spec in §10.5; instance profile can read its own SSM parameter and the artifacts prefix only. | Claude (executor) |
| **D6** | DNS and TLS: Elastic IP to `api.raftkv.tushar25.com` (A record at your DNS provider **[HUMAN]**); TLS via `golang.org/x/crypto/acme/autocert` in the gateway (80 for redirect, 443 for TLS-ALPN). | Claude (executor) + **[HUMAN]** |
| **D7** | Dashboard in `web/` (§10.6), deployed to Vercel at `raftkv.tushar25.com` **[HUMAN]** links the project. | Claude (executor) |
| **D8** | Verification run on the live cluster, record video, capture the report into the README. | **[HUMAN]** + agents |
| **D9** | Ops: `scripts/rolling-update.sh`, `demo-down.sh` (terraform destroy), `demo-pause.sh` (stop instances), incident notes in `docs/OPS.md`. | Claude (executor) |

### 10.4 Security groups and IAM (human must read these before apply)
- `sg_nodes` ingress: 7000/tcp from `sg_nodes` and `sg_gateway`; 7001 and 9000 only from `sg_gateway`; 22 only from `sg_eice`; 2112 only from `sg_gateway`. Egress: VPC CIDR and S3 prefix list.
- `sg_gateway` ingress: 80 and 443 from `0.0.0.0/0`; no SSH. Egress: all (needs ACME, SSM).
- `sg_eice` egress: 22 to `sg_nodes` and `sg_gateway` only.
- Node role: `s3:GetObject` on `artifacts/raftkvd/*` only. Gateway role: same for `gateway/*` plus `ssm:GetParameter` on `/raftkv/admin-token`. No `*` actions or resources anywhere.
- Instances: IMDSv2 required, hop limit 1, EBS encrypted.
- Admin token: SSM Parameter Store `SecureString` `/raftkv/admin-token`, created by the human, never in Terraform state (reference by name; use a data source with care or create out of band).

### 10.5 Node runtime
`/etc/raftkv/config.yaml` (written by user-data from Terraform templatefile): `id`, `peers` (static list of 3 ids and `10.20.x.10:7000`), `client_addr :7001`, `admin_addr :9000`, `data_dir /var/lib/raftkv`, `heartbeat_ms 100`, `election_ms [600,1200]`, `snapshot_threshold` entries, `read_mode`, `log_level`.

`/etc/systemd/system/raftkvd.service`:
```ini
[Unit]
Description=RaftKV node
After=network-online.target
[Service]
User=raftkv
ExecStart=/usr/local/bin/raftkvd --config /etc/raftkv/config.yaml
Restart=always
RestartSec=8
LimitNOFILE=65536
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/raftkv
[Install]
WantedBy=multi-user.target
```
`RestartSec=8` makes a crashed node visibly down for 8s in the demo. `Admin.Crash` performs a real `os.Exit` so recovery exercises the genuine WAL path. Partition, delay, and drop are applied in the transport wrapper (application level), and docs must say so honestly.

### 10.6 Gateway spec (`cmd/gateway`)
Endpoints (JSON over HTTPS, CORS allowed only for the dashboard origin):
- `GET /healthz`
- `GET /v1/cluster`: per node `{id, az, up, role, term, commitIndex, lastApplied, logLen, snapshotIndex, lastContactMs}`
- `GET /v1/events`: Server-Sent Events: elections, role changes, commits, snapshots, chaos actions
- `GET|PUT|DELETE /v1/kv/{key}`, `POST /v1/kv/{key}/cas`
- `POST /v1/chaos/{kill-leader|kill/{id}|pause/{id}|partition|delay|heal}`
- `POST /v1/verify/run` and `GET /v1/verify/last`: runs a bounded (60s) workload plus nemesis against the live cluster, checks the history, stores the report.

Abuse controls (all required, tested):
- Global token bucket for chaos actions (1 per 20s); at most one active fault at a time; **auto-heal after 45s**; never allow more than one node down at once (preserves quorum so the demo cannot be bricked).
- KV limits: key ≤ 64 B, value ≤ 1 KB, ≤ 10,000 keys, per-IP rate limit (e.g. 20 req/s); periodic reset of the keyspace.
- `verify/run`: single-flight mutex, 15-minute cooldown for the public, no cooldown with the admin token; public KV writes are locked during a run (the dashboard shows "verification in progress").
- Admin token (Bearer) for anything beyond read and the rate-limited public actions; constant-time compare.
- Structured request logs without IPs persisted beyond 24h; no secrets in logs.

### 10.7 Dashboard spec (`web/`)
Vite, React, TypeScript, no heavy UI kit. Panels:
1. **Cluster view:** three AZ lanes, leader highlighted, term, commit/applied, up/down state.
2. **Log view:** per-node log strip colored by term, commit marker, snapshot boundary.
3. **Event timeline:** SSE-driven (election started, vote granted, leader elected, node down/up, catch-up, snapshot installed).
4. **Controls:** Kill leader, Kill random, Isolate leader, Heal. Show cooldown and rate-limit state.
5. **KV playground:** put/get/cas with latency display.
6. **Verifier panel:** run, progress, verdict, counterexample viewer, link to latest report.
7. **Replay mode:** if the gateway is unreachable (or credits have run out), play back `public/recording.jsonl` (a captured session) with a visible "REPLAY" badge. The demo link must keep working after the AWS infra is torn down.

### 10.8 Rolling update (`scripts/rolling-update.sh`)
For each node in turn: SSH through EICE; download new binary and verify sha256; `systemctl restart raftkvd`; **wait until `Status` shows the node as follower or leader with `lastApplied` within N of the leader** before touching the next node. Never take down two nodes. Update followers first, the leader last.

### 10.9 Cost ceiling and teardown
Ceiling: $40/month gross. If the $30 alarm fires: run `demo-pause.sh` (stops instances; the Elastic IP and EBS still bill a few dollars) and investigate. When the credits near expiry, run `demo-down.sh` (full destroy) and leave the dashboard in replay mode. Update README to say so.

### 10.10 Three-minute demo script
1. (0:00) One sentence: what RaftKV is. Show the healthy 3-AZ cluster.
2. (0:20) Write some keys; show replication on the log view.
3. (0:50) **Kill the leader.** Narrate the election as the timeline fills; writes resume in seconds.
4. (1:30) Node returns, catches up (show snapshot install if the log was compacted).
5. (1:50) **Isolate the leader** (partition): minority leader steps down or stalls, majority elects, then heal and reconcile.
6. (2:20) Run verification: "N operations, M faults, linearizable: PASS."
7. (2:40) Show the mutation table: "I injected 9 known bugs; the checker caught all 9."
8. (2:55) Link and repo.

---

## 11. Schedule (7 weeks)

| Week | Dates | Focus |
|---|---|---|
| 1 | Oct 21-27 | Task 0, CI, hooks, guard script; T3.0-T3.5 |
| 2 | Oct 28-Nov 3 | T4.1-T4.4; D0-D2 (no running infra yet) |
| 3 | Nov 4-10 | T4.5-T4.7; D3-D5; **first deploy** |
| 4 | Nov 11-17 | D6-D7; demo v1 and first recording; T5.1-T5.3 |
| 5 | Nov 18-24 | T5.4-T5.6 |
| 6 | Nov 25-Dec 1 | T5.7-T5.9; wire verifier into the gateway (Thanksgiving week; keep scope light) |
| 7 | Dec 2-8 | D8, T6.x, README, video, LinkedIn post |

Slip rule: if behind, cut T4.8 and ReadIndex first. Never cut the mutation suite.

---

## 12. Definition of done

- [ ] All §6 gates green on `main`; nightly green 7 days.
- [ ] `docs/INVARIANTS.md` complete, each invariant linked to a test.
- [ ] Mutation table: 9/9 detected.
- [ ] Live demo works end-to-end; replay mode works with the backend off.
- [ ] Budget alarms verified; teardown script tested once.
- [ ] README has real numbers and honest limitations; no secrets in git history (`gitleaks detect`).
- [ ] You can explain every §7.2 item without notes.

---

## Appendix A: `CLAUDE.md` (repo root, read by all Claude roles)

```markdown
# RaftKV: agent instructions
Read docs/RUNBOOK.md. Hard rules (RUNBOOK §2):
- Never commit to main; branch, push, open PR. Human merges.
- Never weaken tests. Test changes and raft-core changes are separate commits.
- Run `make fmt vet lint test` before every commit. No --no-verify, no force push.
- Raft logic must use injected Clock/Rand/Transport (no time.Now/rand/net).
- Persist (fsync) before replying to any state-changing RPC.
- No terraform apply/destroy, no secrets in git, no IAM user/key creation.
- Stay inside your task card and role permissions (RUNBOOK §3). Ask when unsure.
- Finish every task with the report format in RUNBOOK §5.6.
- Know your role (RUNBOOK §3): planner writes cards only; executor implements; runner runs gates and never edits.
Commit format: `type(scope): summary` plus trailers `Task:`, `Agent:`, `Gates:`.
```

## Appendix B: kickoff prompts

**Task 0**
> Read CLAUDE.md and docs/RUNBOOK.md. Do Task 0 (§1.1): read-only. Produce docs/STATUS.md and list phase-map corrections in the PR description. No code changes. Branch `docs/status-audit`.

**T3.x (Claude executor)**
> Read CLAUDE.md, docs/RUNBOOK.md §7.2, §7.3, and docs/STATUS.md. Work only on task T3.<n>. Write tests first using the fault-injecting filesystem in internal/sim, show them failing, then implement. Cite paper sections in comments. Separate commits for tests and core code. Run `make fmt vet lint test` and `go test -race -count=20` on touched packages. Finish with the §5.6 report.

**T4.x / T5.x (Claude executor)**
> Read CLAUDE.md and docs/RUNBOOK.md (§7, §8, and the row for T<id> in §9). Implement only T<id> on branch `feat/<slug>`. Acceptance criteria are in the table; list how each is demonstrated. Do not modify existing tests except to add new ones. Finish with the §5.6 report.

**D2-D5 (Claude executor)**
> Read CLAUDE.md and docs/RUNBOOK.md §10. Implement task D<n> in infra/ only. Run `terraform fmt`, `init -backend=false`, `validate`, and `tflint`. You may run `terraform plan` but **never apply or destroy**. Output the SG and IAM resources in a table in the PR description for human review. No wildcard IAM actions or resources.

**Gateway and dashboard (Claude executor)**
> Read CLAUDE.md and docs/RUNBOOK.md §10.6 and §10.7. Implement D5 (or D7). Every abuse control in §10.6 needs a test. Mock the cluster behind an interface so the gateway is testable without AWS. Finish with the §5.6 report.

**Planner, runner, reviewer:** see Appendix F.

## Appendix C: PR template (`.github/pull_request_template.md`)
```markdown
## Task
<ID and link to RUNBOOK row>

## What and why
## Invariants touched (RUNBOOK §7) and how each is tested
## Gates (paste outputs)
- [ ] make fmt vet lint test
- [ ] go test -race -count=20 on touched packages
- [ ] guard-tests.sh clean (or label tests-modified-reviewed with justification)
## Tests weakened? (must be "no")
## Risks / unverified
## Cross-review result (paste)
## Human checklist
- [ ] I can explain the change without notes
- [ ] IAM/SG/rate-limit changes read line by line
```

## Appendix D: Terraform snippets (starting points, not final)

```hcl
# credits excluded so you see gross spend
resource "aws_budgets_budget" "monthly" {
  name         = "raftkv-monthly"
  budget_type  = "COST"
  limit_amount = "40"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  cost_types { include_credit = false }
  dynamic "notification" {
    for_each = [20, 30, 40]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "ABSOLUTE_VALUE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }
}

resource "aws_instance" "node" {
  for_each               = local.nodes # {1={az=..,ip="10.20.1.10"}, ...}
  ami                    = data.aws_ssm_parameter.al2023_arm64.value
  instance_type          = "t4g.micro"
  subnet_id              = aws_subnet.private[each.key].id
  private_ip             = each.value.ip
  vpc_security_group_ids = [aws_security_group.nodes.id]
  iam_instance_profile   = aws_iam_instance_profile.node.name
  credit_specification { cpu_credits = "standard" }
  metadata_options { http_tokens = "required", http_put_response_hop_limit = 1 }
  root_block_device { volume_type = "gp3", volume_size = 8, encrypted = true }
  user_data = templatefile("${path.module}/userdata.sh.tftpl", {
    node_id = each.key, peers = local.peers_csv, artifact_bucket = var.artifact_bucket, version = var.version
  })
}
```

## Appendix E: Human-only checklist
- [ ] AWS: root MFA, SSO profile, check credit expiry, create budgets, read every `plan` before `apply`
- [ ] Create `/raftkv/admin-token` in SSM; add DNS A record; link Vercel project
- [ ] GitHub: branch protection, CODEOWNERS (`/internal/raft/ /internal/wal/ /internal/verify/ @MenaceHecker`), enable secret scanning
- [ ] Merge PRs yourself after reading raft-core and verifier diffs
- [ ] Record the video, publish the write-up, add to resume only what you can fully explain

---

## Appendix F: Role prompts, templates, and reviewer rules

### F-1. Planner (Claude, read-only session)
> You are the PLANNER for RaftKV. Read CLAUDE.md, docs/RUNBOOK.md, docs/STATUS.md, docs/INVARIANTS.md, and the code relevant to task <ID>. You may write exactly one file: `docs/tasks/<ID>.md`, using the task card template. Do not edit code, tests, or infra. Be specific: name files, functions, invariants (§7 refs), the failing tests to write first, edge cases and crash points, and what is out of scope. If the runbook and the code disagree, list the conflict under OPEN QUESTIONS instead of choosing. Keep the card under 150 lines.

**Task card template (`docs/tasks/<ID>.md`)**
```markdown
# <ID>: <title>
## Goal (2-3 sentences)
## Invariants touched (RUNBOOK §7 refs)
## Files to create or change (paths, with what changes)
## Tests to write FIRST (names, what each asserts, seeds or crash points)
## Implementation notes (paper sections, pitfalls from §7.2)
## Acceptance criteria (copied from RUNBOOK §9/§10 plus any added)
## Out of scope
## Commit plan (ordered; tests and core in separate commits)
## OPEN QUESTIONS for the human
```

### F-2. Runner (Claude Code, edit tools disabled, fresh checkout of the pushed branch)
> You are the RUNNER for RaftKV. You never edit files and never fix failures. Check out branch `<branch>` at its pushed head (`git fetch && git worktree add --detach ../raftkv-run origin/<branch>`). Run, in order, from a clean state: `make fmt vet lint`, `make test`, `make test-flaky`, `scripts/guard-tests.sh origin/main`, `make chaos-smoke`, and, if infra changed, `terraform fmt -check`, `init -backend=false`, `validate`, `tflint`, and `plan` (plan only, never apply). Write `RUN_REPORT.md` with the exact commands, exit codes, and the raw failing output (not paraphrased) for anything that failed. If a flaky test fails once, report the seed and the command to reproduce. Do not interpret failures beyond one line. Do not retry to get green; report each failure.

**`RUN_REPORT.md` format**
```
BRANCH @ COMMIT:
ENV: go version, OS, date
| Command | Exit | Duration | Notes |
FAILURES (raw output, trimmed to the failing test and 40 lines of context):
FLAKE CHECK: count=20 results per package
GUARD-TESTS: output
TERRAFORM PLAN SUMMARY (if applicable): adds/changes/destroys, plus every IAM and SG resource listed verbatim
NOT RUN / SKIPPED (and why):
```

### F-3. Reviewer (ChatGPT; also paste into `AGENTS.md` if you use Codex CLI as reviewer in read-only mode)
> You are an adversarial distributed-systems REVIEWER for RaftKV, an implementation of Raft in Go. You are independent of the authors; do not assume good intent or correctness. You will receive a review packet: task card, diff, invariants, run report, guard output, and full core files. Review only; never rewrite code.
>
> Check, in this order:
> 1. **Safety invariants** (§7.1) and the checklist in §7.2-7.5. For each suspected violation give a concrete interleaving across nodes (events in order) that breaks an invariant. No interleaving, no BLOCKER.
> 2. **Persistence:** is anything acknowledged, voted on, or counted toward commit before it is fsynced (including directory fsync after create/rename)?
> 3. **Classic Raft bugs:** truncating on a stale AppendEntries, committing previous-term entries by counting, resetting the election timer on a rejected or stale RPC, granting votes without the up-to-date check, applying beyond commitIndex, reading from a deposed leader, stale RPC responses from an old term, missing no-op on election, snapshot/log-compaction off-by-ones around `lastIncludedIndex`.
> 4. **Concurrency:** data races, lock ordering, goroutine leaks, blocking while holding the main lock.
> 5. **Determinism:** direct use of `time`, `rand`, or the network inside `internal/raft` (must be injected).
> 6. **Tests:** vacuous tests (assert nothing meaningful), weakened or deleted assertions, missing negative cases, tests that cannot fail, nondeterminism, tests that pass because of the mock. Also check that the failing test was written before the fix, if the commit history shows it.
> 7. **Security (infra and gateway only):** wildcard IAM, open security groups, missing rate limits, token comparison, unbounded inputs.
>
> Output exactly this schema: `VERDICT`, `FINDINGS` (`[BLOCKER|MAJOR|MINOR|NIT] file:line | invariant | evidence | failing scenario or test to add`), `TEST REVIEW`, `NOT VERIFIED`. List everything you could not verify because you cannot execute code. Do not praise. Do not approve if any BLOCKER or MAJOR is open.

### F-4. Re-review loop
After fixes, give the reviewer only: the previous findings, the new diff since the last review (`git diff <reviewed-sha>...HEAD`), and a new `RUN_REPORT.md`. Ask: *"For each prior finding, state resolved, unresolved, or wrong-finding (with justification). Then review the new delta using the same schema."*

### F-5. Where each role gets which model (summary)
| Step | Model | Session |
|---|---|---|
| Plan | Claude | read-only, fresh |
| Execute | Claude Code | own worktree, fresh per task |
| Run gates | Claude Code | edit tools off, clean checkout |
| Review | ChatGPT | fresh, packet only |
| Merge, apply, secrets | Human | |