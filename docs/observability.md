# Observability

A Raft cluster fails in ways that a process-level health check cannot see. The
node is up, the port is open, the logs are quiet, and nothing is committing
because a majority is unreachable. Everything here exists to make that
distinction visible.

Metrics are served on a separate port from the data plane:

```bash
raftkv-server --id 1 --peers 1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 \
  --data-dir /tmp/n1 --metrics-listen 127.0.0.1:9101
```

That is deliberate. Metrics matter most exactly when the cluster is unhealthy,
and sharing a server with the traffic that is failing is how a monitoring
endpoint becomes unavailable during the incident it exists to explain. Omitting
the flag disables the endpoint entirely.

Three paths are served:

| Path | Purpose |
| --- | --- |
| `/metrics` | Prometheus exposition format |
| `/health` | Liveness. Returns this node's view of the cluster as JSON |
| `/ready` | Readiness. 503 while this node sees no leader |

`/health` goes through the Raft loop rather than answering from a cached value,
so a reply means the loop is actually running, not merely that the process
accepted a connection. `/ready` fails while there is no leader: such a node is
running correctly but cannot serve a linearizable read, so a load balancer
should route around it rather than send traffic it will refuse.

## The four questions

The metrics are chosen to answer, in order, the questions an operator actually
has during an incident. A metric that does not help answer one of them is not
worth its cardinality.

**Is there a leader?** `raftkv_is_leader` is 1 on the leader and 0 elsewhere,
so summed across the cluster it should be exactly 1. Zero means nothing can
commit. `raftkv_leader_id` is what each node believes, and nodes disagreeing is
what a partition looks like from outside.

**Is the cluster committing?** `raftkv_proposals_total` and `raftkv_reads_total`
are labelled by outcome. The labels are the interesting part. `not_leader` is
usually a client talking to the wrong node and is harmless.
`lost_leadership` means a write was accepted and then orphaned by an election,
so the client must retry, which deduplication makes safe.

**Is any node falling behind?** `raftkv_commit_index` and
`raftkv_applied_index` per node, and `raftkv_apply_lag_entries` for the gap
between them. Sustained lag means the state machine is the bottleneck rather
than consensus. `raftkv_snapshots_received_total` is the sharper signal: a node
that had to be caught up by snapshot fell behind the leader's compaction point
and could not have recovered from the log alone.

**Is the disk keeping up?** `raftkv_persist_duration_seconds` measures the
durable log write including the fsync. Every write in the system waits on this,
so it is the floor under write latency and the first thing to check when
latency rises. On a local three-node cluster it sits around 4ms, against a
7ms end-to-end write, so most of what a client waits for is the disk.

## Counted, not sampled

Leadership changes are recorded as they happen rather than read from a gauge at
scrape time.

This is not a detail. An election can begin and finish between two scrapes, and
a sampled gauge would show the same term before and after and report nothing at
all. That is precisely the event worth alerting on, so
`raftkv_leader_changes_total` is incremented from the Raft loop the moment the
term or the leader changes.

Indexes are sampled, because unlike an election they cannot move and move back
unnoticed. Mirroring every commit into a gauge would put work on the Raft loop
to produce a number read once per scrape interval.

## Where the instrumentation lives

The consensus core has no clock, no network, and no goroutines, and the driver
above it has no dependency on any metrics library. The driver describes what
happened through a small `Recorder` interface, and only `internal/metrics`
knows about Prometheus. That keeps both layers testable without a registry and
keeps the boundary the whole project is built on intact.

Durable writes are the one measurement that cannot be taken in the driver: the
core writes through the `Storage` interface synchronously, so the fsync happens
inside a call the Raft loop is blocked on. It is measured by wrapping that
interface, which puts the timer exactly where the latency is without giving the
storage layer any knowledge of metrics.

## Dashboard and alerts

`deploy/grafana/raftkv-dashboard.json` imports into Grafana and reads top to
bottom in that same order: is there a leader, is the cluster serving, how slow
is it, and which node is the problem.

`deploy/alerts.yml` has seven rules. They are deliberately few, because Raft
recovers from most things by design and alerting on those would train an
operator to ignore the page that matters. Each rule corresponds either to a
state the cluster cannot recover from on its own, or one where it is running
correctly but not doing what a client asked.

`RaftKVMultipleLeaders` is the one worth explaining. Two leaders at once would
violate Election Safety, which every other guarantee rests on. It fires only
after 30 seconds, because a briefly deposed leader that has not yet learned it
lost is normal and expected.

```bash
prometheus --config.file=deploy/prometheus.yml
```

## Verifying it

`internal/metrics` tests run a real single-node cluster and assert on what
comes out of the registry, rather than testing the collectors in isolation.
That is the only thing that catches a hook which was never called, and it is
the failure mode that matters: an instrumented system whose instrumentation
silently reports nothing is worse than one with no metrics at all, because it
looks healthy.

Each hook was checked by removing it and confirming a test goes red. Dropping
the storage decorator, for instance, leaves the node working perfectly and
breaks only the test that asserts the fsync was observed.
