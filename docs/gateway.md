# Gateway (`cmd/gateway`): API, limits, restarts, and what is not exposed

Spec: RUNBOOK 10.6. Card: `docs/tasks/D5.md`. This covers part 1 (core, KV, abuse
controls, security). Chaos, `/v1/events` and `/v1/verify/*` are D5b and answer
`501` today. The live adapter to a real cluster needs the client library (T4.6)
and is not in this build: `cmd/gateway` only serves with `--fake-cluster`, an
in-memory demo cluster.

## Running it (demo)

```
gateway --fake-cluster --listen 127.0.0.1:8080 \
  --admin-token-file admin.token --node-token-file node.token \
  --state-dir ./state --dashboard-origin https://dash.example.com
```

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
| `/v1/events`, `/v1/chaos/*`, `/v1/verify/*` | `501` (D5b). |

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
(`--node-token-file`, used only by the live adapter, which is not built yet).
