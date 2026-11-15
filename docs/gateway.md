# Gateway (`cmd/gateway`): API, limits, restarts, and what is not exposed

Spec: RUNBOOK 10.6. Card: `docs/tasks/D5.md`. This covers part 1 (core, KV, abuse
controls, security) and the live adapter; chaos, `/v1/events` and `/v1/verify/*` are
part 2 (D5b), described below.

## Running it

```
gateway --listen 127.0.0.1:8080 \
  --nodes 1=us-east-1a=10.0.1.10:7000=10.0.1.10:9000,2=us-east-1b=10.0.2.10:7000=10.0.2.10:9000,3=us-east-1c=10.0.3.10:7000=10.0.3.10:9000 \
  --admin-token-file admin.token --node-token-file node.token \
  --state-dir ./state --dashboard-origin https://dash.example.com
```

`--nodes` is `id=az=addr=adminaddr,...`: `addr` is the node's `--listen` gRPC
address (KVService), `adminaddr` its `--admin-listen` address. It fails closed:
an empty list or entry, a wrong field count, id 0, a repeated id, a repeated
address, or a port outside 1..65535 exits non-zero. The nodes must run with
`--admin-listen` and `--admin-token-file`, and `--admin-chaos` for the chaos
RPCs; `--node-token-file` holds that same machine token.

### The live adapter (`internal/gateway/live.go`)

- KV: 8 `internal/client` clients, one per pooled session, each with its own
  random client id (never a fixed one) and serialised writes; retries with the
  same sequence number are the client's job. `IndeterminateError` becomes
  `ErrIndeterminate` (`504`, `"outcome":"unknown"`); no leader reached, or a
  write that was never sent, becomes `ErrNoLeader` (`503`); a refusal by a node
  (`InvalidArgument`, `ResourceExhausted`, ...) becomes `ErrRejected` (`502`,
  `"outcome":"not applied"`, the key is released from the registry).
- Admin: one `AdminServiceClient` per node, plaintext gRPC inside the VPC, the
  machine token sent as `authorization: Bearer` on every call and never logged.
  `ClearFaults(id)` is `SetPartition([])` then `SetNetem(0,0,0)`: the nodes have
  no clear RPC. A wrong token gives `ErrNodeUnauthenticated`, a node without
  `--admin-chaos` gives `ErrChaosDisabled`, a lost connection gives
  `ErrNodeUnavailable` (what a `Crash` normally produces: the node exits about
  50 ms after replying).
- Events: the adapter holds one upstream `Events` stream per node for its whole
  life and fans it out to local subscribers, so browsers never use the nodes'
  16 subscriber slots. It reconnects with jittered exponential backoff (200 ms
  doubling to 10 s, drawn from the upper half), also after `ResourceExhausted`
  or `Unauthenticated`. A subscriber that falls behind loses events; the node's
  `seq` on each event exposes the gap.
- Only `KVService` and `AdminService` clients are built.

### Demo build

`--fake-cluster` (an in-memory demo cluster) exists only in a binary built with
`go build -tags fakecluster ./cmd/gateway`. The production build has no such
flag and does not link the fake. CI vets and tests the tagged variant.

Both token files must exist, be mode 0600, be non-empty and differ from each
other; any missing flag exits non-zero. `--public-keyspace-reset` defaults to 6h.
The server is plain HTTP; TLS is D6.

## Endpoints

| Route | Answer |
|---|---|
| `GET /healthz` | `{"status":"ok"}`. Never touches the cluster. Not rate limited. |
| `GET /v1/cluster` | `{"leaderId":N,"nodes":[{id,az,up,role,term,commitIndex,lastApplied,logLen,snapshotIndex,lastContactMs,paused}]}` from a 1 Hz cache. `logLen = last - first + 1`; `lastContactMs` is milliseconds since the gateway's last successful poll of that node (`null` if never); a node that did not answer is `up:false`, `role:"unknown"`. |
| `GET /v1/kv/{key}` | `200 {"key","value"}` or `404`. |
| `PUT /v1/kv/{key}` | Body `{"value":"<string>"}`, `Content-Type: application/json`. `200 {"key"}`. |
| `DELETE /v1/kv/{key}` | `200`; deleting a missing key succeeds. |
| `POST /v1/kv/{key}/cas` | `501`: the cluster has no compare-and-swap. |
| `POST /v1/admin/reset` | Admin Bearer token. Runs the keyspace reset now. |
| `GET /v1/events` | Server-sent events, see below. |
| `POST /v1/chaos/{kill-leader,kill/{id},pause/{id},partition,delay,heal}` | Fault buttons, see below. |
| `POST /v1/verify/run`, `GET /v1/verify/last` | Verification run, see below. |

Errors are JSON `{"error": "..."}`. Statuses: `400` bad key, body or JSON;
`403` reserved key prefix; `405` with `Allow`; `413` body over 2 KiB or value
over 1024 bytes; `415` wrong content type; `429` with `Retry-After`; `503` with
`Retry-After` when no leader is available or while a verification run or reset
locks writes (`verification in progress`); `504` on the 3 s deadline, and for a
write whose outcome is unknown the body carries `"outcome":"unknown"`; `507`
when the registry holds 10,000 keys and the key is new; `502` for any other
cluster error (its text is never returned).

## Limits

- Key: 1 to 64 bytes, valid UTF-8, no control characters, no `/` (use `%2F`
  and it is refused). `__verify/` is reserved for the verification run. `.` and
  `..` are cleaned away by the HTTP mux before the handler and cannot be used.
- Value: at most 1024 bytes, checked after parsing. The request body is capped
  at 2 KiB with `http.MaxBytesReader` (a 1 KiB cap could not hold a 1024 byte
  value inside JSON).
- At most 10,000 keys. Overwriting a key is free; deleting frees a slot.
- Per client address: 20 requests per second with a burst of 20. Only the
  connection address counts; `X-Forwarded-For` is ignored. IPv6 is limited per
  /64. The table holds at most 10,000 addresses; idle entries are dropped and
  an arbitrary one is evicted if it is full.
- 8 pooled cluster sessions, one per request in flight, so no client id has two
  writes in flight. A request that hits the 3 s deadline gets `504`, but its
  session returns to the pool only when its cluster call has really returned.
- Admin: `Authorization: Bearer <token>`. The token is hashed with SHA-256 and
  compared with `crypto/subtle`; a missing, malformed or wrong token gets the
  same `401`.
- CORS: only the one `--dashboard-origin` is echoed back. `*` is never sent.

## What survives a restart

| State | Survives? |
|---|---|
| Key registry (`--state-dir/keys.log`, one `put <key>` or `del <key>` line per change, fsync per append, compacted when it doubles, a torn last line is dropped) | Yes. The key cap and the reset depend on it, because the cluster has no scan. |
| The keys and values in the cluster | As the cluster keeps them. |
| Time until the next keyspace reset | No: the interval restarts at process start. |
| Rate limiter buckets, status cache, verify lock | No, memory only. |
| Client addresses | Never persisted anywhere, never logged. |

A write that returns `504` with `outcome:"unknown"` may have been applied, so its
key stays in the registry. A write refused because there was no leader is
removed from it. The reset deletes every registered key through the cluster,
then empties the registry; if a delete fails the key stays and the reset is
retried on the next tick. It is skipped while a verification run holds the write
lock.

## Logs

One JSON line per request: method, route template (`/v1/kv/{key}`), status,
`latency_ms`, `request_id`. No path, no address, no header, no body. The
`net/http` server error log is discarded because it prints remote addresses.

## What is not exposed

The `Cluster` interface has no `AddNode`, `RemoveNode` or `ListMembers`, and a
test parses the package to keep the membership client out of it. The gateway
never constructs a `ClusterServiceClient`. Two secrets are kept apart: the HTTP
admin token (typed into a browser) and the machine token for node admin RPCs
(`--node-token-file`, used only by the live adapter).

## Chaos controller (D5b)

All rules live in one mutex-guarded state machine (`internal/gateway/chaos.go`).
Bodies are tiny JSON (1 KB cap, unknown fields refused, all optional).

| Endpoint | Node calls |
|---|---|
| `kill-leader`, `kill/{id}` | `Crash` on the leader. A lost connection right after the call is success (the node replies, then exits). A public caller may only kill the leader; any other id needs the admin Bearer token. |
| `pause/{id}` `{"seconds":n}` | `Pause`; default 20 s, never more than 45 s. |
| `partition` `{"node":id}` | `SetPartition(every other node)` on one node (default the leader). |
| `delay` `{"delayMs":n,"dropPercent":n,"node":id}` | `SetNetem(peer 0, ...)`; delay at most 500 ms, drop at most 50 %. |
| `heal` | Clears partition, netem and pause on every node (cannot undo a crash). Public, never throttled. |

Rules, each with a test: public token bucket of one fault per 20 s for everybody
(`429` + `Retry-After`; the Bearer token skips it); one active fault at a time
(`409`); a new fault needs a fresh `Status` from all 3 nodes, none paused, and a
leader, otherwise `503`, so never more than one node is down; the gateway heals
by itself at 45 s. The slot is released only when the cluster is seen healthy
again, not when the timer fires: a node that is still down after the heal keeps
the slot held, `/v1/cluster` reports `chaos.state: "recovering"`, and no new chaos
is accepted. While a verification run is active, all chaos except the run's own
nemesis is refused; heal is still allowed. A refused request changes nothing and
sends no fault call.

45 s versus 120 s: the nodes expire partition and netem faults on their own after
120 s and clamp `Pause` at 60 s. Those are backstops for a dead gateway only; the
gateway heals at 45 s and caps pause at 45 s so it never depends on them.

## `/v1/events`

One `Events` stream per node (3 in all, far below a node's 16 subscribers) feeds
a hub; browsers never hold a node slot. Upstream streams reconnect with jittered
backoff after `ResourceExhausted`, `Unauthenticated` (logged once) and a crash.
Each browser has a 64 event buffer; one that falls behind is disconnected. At
most 100 browsers, 3 per address. A `: keepalive` comment goes out every 15 s.
Every event has a hub `id`; a reconnect with `Last-Event-ID` replays from a
256-event ring, and an id older than the ring (or unknown) gets a `resync` event
carrying the `/v1/cluster` document. A gap in a node's `seq` emits `resync` too.
Event types: `state`, `node_down`, `node_up`, `snapshot`, `commit` (polled, at
most one per node per second, never per entry), `chaos`, `verify`, `resync`.
Not available: vote granted, election start, per-entry commit (no node emits
them; a raft observer hook is backlog).

## Verification run

`POST /v1/verify/run` starts a run and answers `202 {id, seed, status}`; `GET
/v1/verify/last` returns the last report (or `{status:"running"}`). Only one run
at a time (`409` with the running id). A public caller must wait 15 minutes after
the end of the last run (`429`); the admin Bearer token skips the cooldown, not
the single flight. Public KV writes are locked for the duration.

Five workers each with their own session write and read `__verify/k0..k4`
(60 % put with unique values, 40 % get), 2 s per operation, at most 60 s and
5,000 operations. The nemesis is the only chaos: isolate the leader at about
10 s, heal at about 25 s, kill the leader at about 35 s (offset by the seed).
Every key is first seeded with a known value so leftovers of an earlier run
cannot look like a lie. A read error is `failed`; any write error is `unknown`,
never `failed`. After the run every node is healed, and the recorded history is
checked by `chaos.Check`. Verdicts: `linearizable`, `not_linearizable` (with a
counterexample), `inconclusive` (undecided, no successful operation, or aborted).
The history is not stored.

## What survives a restart (D5b)

| State | Survives? |
|---|---|
| `verify.json` (last report, end time for the cooldown, a marker while a run is going) | Yes. A leftover marker becomes an `aborted` report and the cooldown restarts, so a crash loop cannot dodge it. |
| Chaos bucket and slot | No. Safe start: heal every node, count the bucket as just used, hold the slot until every node answers `Status`. |
| Event ring, browsers | No. |

## Deployment and residual risk

Nodes must be started with `--cluster-token-file` (T3.18) so `AddNode` and
`RemoveNode` are authenticated; the deployment must set it. Raft peer traffic and
the shared listener stay unauthenticated inside the private subnets. That residual
risk is accepted: a compromised host in the node subnets can still talk to the
peer port. The gateway has no code path to membership calls, and uses
`--node-token-file` (separate from the admin Bearer token) for node AdminService
calls.
