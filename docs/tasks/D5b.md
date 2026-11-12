# D5b: gateway `cmd/gateway`, part 2 of 2 (chaos controller, `/v1/events`, `/v1/verify/*`)

Planner: Claude, read-only. Based on `origin/main` 8e00a77. **No test was run**; (unverified) marks unchecked claims. Continues `docs/tasks/D5.md` (read it first: it
holds the split rationale, the facts, the runbook-vs-code table, the `Cluster` interface, the key registry, auth, CORS, logging and T4.6). Same branch and PR as D5; these
commits follow D5's commits 1-6. Out of scope: Terraform, IAM, DNS, TLS (D6), the dashboard (D7); the executor must never run `terraform apply`/`destroy`.

## Goal
Public chaos buttons that cannot brick the demo, a single SSE stream fanned out from the nodes' `Events`, and a bounded verification run whose recorded history is checked by the
existing `chaos.Check`, all tested against the fake cluster from D5 with a fake clock.

## Invariants touched
RUNBOOK §7.4 and §8.2/§8.3 consumed: the checker is `chaos.Check(history []chaos.Op)` (`chaos/checker.go:72`), which models only write and read (`chaos/cluster.go:15-21`; no delete, no CAS) and returns
`Linearizable`, `NotLinearizable` or `Undecided` (`checker.go:10-18`). `Undecided` is a failure to prove, never a pass. No Raft change. §2.8: the admin token is never logged or returned.

## Facts from the code (cite; re-check)
- Faults are **per node and application-level**: `SetPartition(blocked)` cuts that node's Raft traffic both ways (`admin.go:196`, `faults.go:185-215,442-457`), so isolating one node needs one RPC to that node;
  it does not touch that node's `KVService` port, so an isolated leader still **accepts** client requests and cannot commit them (they hang until the deadline). `SetNetem(peer,delay,drop)` affects
  **messages sent by that node** only; peer 0 = all (`admin.go:217`, `faults.go:389-418`). There is no RPC for `ClearFaults`; clear = `SetPartition([])` and `SetNetem(0,0,0)` for peer 0.
- `Pause(d)` is clamped to 60 s (`node/pause.go:10`, `admin.go:191`), and ticks, Raft peer traffic and every `KVService`/`ClusterService` method go dark; `Status` and `Events` still answer (`admin.go:131,357-381`).
  Partition and netem expire **at the node** after 120 s, each on its own timer (`faults.go:30,330-361`). `Crash` replies, then `os.Exit(137)` after 50 ms; the reply is best effort (`admin.go:164-183`).
  The process only comes back if systemd restarts it (RUNBOOK §10.5 `RestartSec=8`; not in this repo, unverified).
- `Events` is at most 16 subscribers per node, 256 buffered, over-limit or slow subscriber -> `ResourceExhausted` (`node/events.go:12,16`, `admin.go:242-268`); events carry (term, role, leader, commit, applied,
  snapshot index, `seq`, `unix_nano`), `seq` +1 per event per node (`proto:395-410`). Only `EVENT_KIND_STATE` exists (`proto:403-407`). Admin gRPC is plaintext inside the VPC.
- The shared listener still accepts **unauthenticated** `AddNode`/`RemoveNode` and 1 GiB snapshot streams (`admin.go:3-16`, `docs/tasks/T3.18.md`, card open).

## Chaos controller (`internal/gateway/chaos.go`)
Mapping (target = leader from the 1 Hz cache, re-confirmed by a fresh `Status` of that node; id must be in `--nodes`; all bodies tiny JSON, 1 KB cap):
| Endpoint | Admin RPCs (token on every call) |
|---|---|
| `kill-leader`, `kill/{id}` | `Crash` on the node. `EOF`/`Unavailable` right after the call counts as success (`admin.go:167-169`). |
| `pause/{id}` | `Pause(min(requested, 45 s))` on the node (default 20 s). The node resumes itself at that time even if the gateway dies. |
| `partition` | `SetPartition(blocked = every other node)` on the **one** target node (default leader). Heals via `SetPartition([])`. |
| `delay` | `SetNetem(peer 0, delay_ms, drop_percent)` on one node. Caps (proposed, Q3): delay <= 500 ms, drop <= 50. |
| `heal` | `SetPartition([])`, `SetNetem(0,0,0)`, `Pause(0)` on every reachable node; cannot undo `Crash`. Always allowed, not rate limited, public. |
Rules (all enforced by one mutex-guarded state machine; each has a test): global token bucket 1 per 20 s (fake clock; `429` + `Retry-After`); **one active fault slot**; **auto-heal at 45 s** (the gateway timer
runs `heal`; node expiry at 120 s and the `Pause` cap at 60 s are only backstops for a dead gateway, so the gateway never relies on them); **at most one node impaired**: impaired = crashed and not yet seen up,
paused, or isolated; a new fault is allowed only when the slot is free **and** a fresh `Status` (<= 2 s old) succeeds on all 3 nodes and a leader is known, so a node still restarting after a `Crash` blocks the next
one. The slot is released only after heal or recovery is **observed**, not when the timer fires. With 3 nodes this keeps a quorum of 2. A rejected request changes nothing and calls no admin RPC.
Caller: any visitor may use the five endpoints at the rate limit; **bearer token** bypasses the bucket (not the one-fault and one-down rules) and is required for `kill/{id}` with an id other than the leader (proposed, Q3).

## Restart safety: what needs state that survives a gateway restart (Q4)
Persisted (`--state-dir`, atomic temp+fsync+rename+dir fsync): the key registry (D5), `verify.json` (last report, **last finished-run wall time** for the 15-minute cooldown). In memory only, with a **safe start**:
the chaos bucket and slot (on start the gateway runs `heal` on all nodes, treats the bucket as just consumed, and refuses chaos until all nodes answer `Status`; a fault set by a dead gateway lasts <= 120 s
at the node anyway); per-IP limiter (a restart only resets counts); single-flight verify (a restart kills a run; on start a leftover `running` marker in `verify.json` is recorded as `aborted`, and
the cooldown starts from that marker so a crash loop cannot be used to dodge it); the keyspace-reset timer (restarts its period).

## `/v1/events` fan-out (`internal/gateway/events.go`)
One goroutine per node holds **one** `Events` stream (3 streams, far under the node cap of 16, so browsers never consume node slots); reconnect with jittered backoff (injected Rand/Clock), including after
`ResourceExhausted`, `Unauthenticated` (logged once, no token) and a `Crash`. Hub: per-browser buffered channel (64); a full channel closes that SSE connection (never blocks the hub or a node stream); global max
100 browsers, 3 per IP (`503`/`429`); SSE comment keepalive every 15 s; hub-assigned `id:` with a 256-event ring and `Last-Event-ID` replay, a request older than the ring gets a `resync` event followed by a full
`/v1/cluster` snapshot. A per-node `seq` gap emits `resync` too. Event types: `state` (node events as sent), `node_down`/`node_up` (stream or poll break and recovery), `snapshot` (the poller sees
`snapshot_index` rise), `commit` (not per entry: at most 1 per second per node from the poller), `chaos` (action, target, expiry), `verify` (phase, counts). **Not available:** per-vote events,
election-start-and-abort (`proto:378-380`); the dashboard's "vote granted" row cannot be served (Q6).

## `/v1/verify/run` and `/v1/verify/last` (`internal/gateway/verify.go`)
Single-flight (`TryLock`: second caller gets `409` with the running run's id). Public cooldown 15 min from the **end** of the last run (persisted); bearer token skips it. Writes locked (D5 test 7). Run: 5 workers, each its own
`KV` (own client id, serial ops), keys `__verify/k0..k4`, 40% get / 60% put with unique values (`c<w>-<n>`), per-op deadline 2 s, hard wall limit 60 s and max 5,000 ops, nemesis from a fixed seeded schedule
(seed echoed in the report): isolate leader at ~10 s (heal at ~25 s), kill leader at ~35 s, through the same controller (so the one-fault and one-down rules hold) but without the public bucket. Recording: `Invoked`/`Returned` from a monotonic
counter (`chaos.Op` exported fields only: `Kind, Client, Key, Value, Found, Invoked, Returned, Status`). **Outcome mapping:** read ok -> `StatusOK` with `Found`/`Value`; read error -> `StatusFailed` (a read
changes nothing); write ok -> `StatusOK`; **any write error -> `StatusUnknown`** (`NotLeader` and `Aborted` included: `node.go:586,616`). Never `StatusFailed` for a write. After the run, heal all, wait for all nodes up, call
`chaos.Check`, store `{id, seed, startedAt, endedAt, ops, ok, failed, unknown, faults[], verdict, counterexample = Result.String(), explored}`; the history itself is not stored (no §8.2 JSONL recorder exists; T5.2 has no card).
`Undecided` and a run that could not finish report `inconclusive`, not pass. An HTML report is T5.8, not here.

## Tests to write FIRST (stub first; fake clock; `-race`)
Chaos: 18 `TestChaosTokenBucketOnePerTwentySeconds`. 19 `TestOnlyOneActiveFault` (concurrent requests: exactly one wins, no admin RPC for losers). 20 `TestAutoHealAt45s` (fake clock; heal issues `SetPartition([])`,
`SetNetem(0,0,0)`, `Pause(0)`; slot still held until recovery is observed). 21 `TestNeverTwoNodesDown` (table: crash+pause, crash+isolate, crashed node not yet back, two nodes unreachable -> refused). 22 `TestKillLeaderMapsToCrashOnLeader` and a table test for
each endpoint's exact RPC arguments. 23 `TestPauseIsCappedBelowNodeCap` (never sends > 45 s). 24 `TestCrashConnectionLossIsSuccess`. 25 `TestNetemCaps`. 26 `TestHealAlwaysAllowedAndUnthrottled`. 27 `TestRejectedChaosCallsNoAdminRPC`.
28 `TestAdminTokenSentOnEveryNodeRPCAndNeverLogged` (spy sees metadata; log capture clean). 29 `TestBearerBypassesBucketNotSafetyRules`. 30 `TestStartupHealsAndBlocksChaosUntilNodesUp`.
Events: 31 `TestOneUpstreamStreamPerNodeRegardlessOfBrowsers` (200 connect attempts -> 3 streams). 32 `TestSlowBrowserDroppedOthersUnaffected`. 33 `TestMaxBrowsersAndPerIP`. 34 `TestLastEventIDReplayAndResync`. 35 `TestUpstreamResourceExhaustedReconnects`.
36 `TestNodeSeqGapEmitsResync`. 37 `TestCrashedNodeYieldsNodeDownThenUp`.
Verify: 38 `TestVerifySingleFlight`. 39 `TestVerifyPublicCooldownPersistsAcrossRestart` (state dir reopened) and `TestVerifyTokenSkipsCooldown`. 40 `TestVerifyAbortedMarkerOnRestart`. 41 `TestVerifyWallLimit60sAndOpCap`.
42 `TestVerifyLinearizableFakePasses`. 43 `TestVerifyDetectsLyingCluster` (fake read-mode returns a stale value: verdict `NotLinearizable`, counterexample present). 44 `TestVerifyWriteErrorsAreUnknownNeverFailed` (including `NotLeader`).
45 `TestVerifyUndecidedIsInconclusive` (tiny budget through `chaos.CheckWithBudget`). 46 `TestVerifyHealsEverythingAfterRun` (also on panic and on a cancelled context). 47 `TestVerifyKeysAreDisjointFromPublic`. 48 `TestVerifyLastReturnsPersistedReport`.

## Acceptance criteria (RUNBOOK §10.6, Appendix B)
Each abuse control of §10.6 has a named test above or in D5 (token bucket 18, one fault 19, auto-heal 20, one node down 21, KV limits D5 1-3, per-IP D5 5, keyspace reset D5 6, verify single-flight 38, cooldown 39, writes locked D5 7, Bearer D5 8,
no IPs/secrets D5 10); cluster mocked behind an interface; no AWS; `make fmt vet lint test`; `-race -count=20` on `internal/gateway`.

## Commit plan (continues D5 numbering; tests and core separate)
7a `chore(gateway): stub chaos controller, hub and verify runner`. 7b `test(gateway): red chaos, events and verify tests` (18-48, each fails on its assertion). 7c `feat(gateway): chaos controller and safety rules`. 7d `feat(gateway): events hub and SSE`.
7e `feat(gateway): verify runner over chaos.Check`. 7f `feat(gateway): wire cmd/gateway`. 7g `docs(gateway): chaos mapping, 45 s vs 120 s expiry, what survives a restart, T3.18 exposure`.

## OPEN QUESTIONS for the human
1. **T3.18 stays open**: until it lands, the nodes' shared port accepts unauthenticated membership changes from anything that can reach it, and in the code Raft, client and `ClusterService` share one port, so `sg_gateway -> :7000/7001` also reaches them. The gateway has no code path to them (D5 test 11) but a
   compromised gateway or any host in `sg_nodes` does. Land T3.18 before a public demo, or accept the risk in writing?
2. Node-side timers: gateway heals at 45 s; nodes expire at 120 s; `Pause` at 60 s. Gateway pause is capped at 45 s so it never depends on the 60 s cap. Confirm the 45 s auto-heal is also the limit for `Crash` recovery (a node not back by 45 s keeps the slot held and blocks further chaos; alerts are out of scope).
3. Numbers and policy not in the runbook: netem caps (500 ms, 50%), bearer-only `kill/{id}` for non-leaders, default pause 20 s. Confirm.
4. Restart safety as above (persist registry and verify cooldown; safe-start for the rest). Confirm; the alternative is persisting the chaos bucket too.
5. Verify nemesis schedule (isolate leader, then kill leader) and whether a kill during verify is allowed when a node `RestartSec=8` is unverified in this repo.
6. Dashboard spec rows needing data the nodes do not emit (vote granted, election start, per-entry commit): drop from D7, or add a raft observer hook (T4.7 backlog, raft-core review)?
