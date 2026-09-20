# Deployment

Running a consensus system is not like running a stateless service, and most of
what follows is a consequence of one fact: **a RaftKV node is not
interchangeable with its replacement.** Its identity is what its votes and its
log are attributed to. Anything that treats nodes as fungible will produce a
cluster that looks healthy and has quietly lost the ability to make decisions.

## The image

```bash
docker build -t raftkv:dev .
```

Two stages. The build stage compiles a static binary with CGO off and runs
`go test ./...`, because an image that ships a binary whose tests were never
run is an image nobody should deploy. The runtime stage is Alpine with a
non-root user, a read-only root filesystem in Kubernetes, and nothing in it but
the two binaries and `wget` for the compose healthcheck.

## Docker Compose

```bash
docker compose up --build
curl -s localhost:9101/health
```

Five nodes, Prometheus, and Grafana with the dashboard already provisioned on
[localhost:3000](http://localhost:3000). Five rather than three because it is
the smallest cluster that survives two failures.

Each node exposes its client and Raft port on `900N` and its metrics on
`910N`. The healthcheck probes `/ready`, which fails until the node can
actually serve, so compose reports a node healthy only once a leader exists
rather than the instant the process starts listening.

Run the benchmark from inside the network:

```bash
docker compose exec raftkv-1 raftkv-bench --endpoints raftkv-1:9001 --duration 20s
```

**Inside the network, not from the host.** Nodes advertise themselves by
container name, so a redirect sends a client to `raftkv-3:9001`, which does not
resolve on your machine. A client on the host can read and write against
whichever node it happens to hit, but it cannot follow a redirect. This is not
a bug in the client, it is what advertising an unroutable address means, and
the same care applies to any real deployment where the advertised address must
be reachable by the clients that will be redirected to it.

## Kubernetes

```bash
kubectl apply -f deploy/kubernetes/raftkv.yaml
kubectl -n raftkv get pods
```

A StatefulSet of five, a headless service for peer identity, a ClusterIP
service for clients, and a PodDisruptionBudget. Pod `raftkv-N` is always node
`N+1`, always resolves to the same DNS name, and always reattaches to the same
volume. Node IDs are one-based because zero means "no node" in the protocol.

Four things in that manifest are load bearing.

**`podManagementPolicy: Parallel`.** This is the most important line in the
file. The default, `OrderedReady`, starts pod N+1 only once pod N reports
ready. No pod here becomes ready until the cluster has elected a leader, and no
leader can be elected until a majority is running. Ordered startup deadlocks on
the first pod, forever.

That is measured rather than reasoned. Deploying this manifest with
`OrderedReady` leaves exactly one pod in existence after two minutes, stuck at
`0/1 Running`, reporting:

```json
{"id":1,"state":"PreCandidate","term":0,"leader":0,"commit":0,"applied":0}
```

It is asking permission to campaign, from a majority that Kubernetes is waiting
for it to become ready before creating. The same manifest with `Parallel`
reaches five of five ready in about thirty seconds with a leader elected.

**`publishNotReadyAddresses: true`** on the headless service, for the same
reason from the other direction. Peers must be able to resolve each other
before any of them is ready, or DNS withholds exactly the addresses they need
to become ready.

**Readiness and liveness ask different questions.** Readiness is `/ready`,
which fails when this node sees no leader: such a node is running correctly and
cannot serve a linearizable read, so it should be pulled out of the client
service. Liveness is `/health`, which asks only whether the Raft loop is
answering. Liveness must never depend on there being a leader. If it did, every
node would fail it simultaneously during an election and Kubernetes would
restart the entire cluster, turning a recoverable outage into a much longer
one.

**`minAvailable: 3` in the PodDisruptionBudget.** A quorum of five is three. A
node drain that took three pods would stop the cluster, and voluntary
disruption is the one kind of outage that is entirely within your control.

### Verified behaviour

Deleting the leader pod on a five node cluster:

- A new leader was elected and the cluster kept committing within four seconds.
- The pod came back as a **new pod object** with a different UID, but the
  **same name, the same node ID, and the same PersistentVolumeClaim**.
- It rejoined and caught up to the cluster's commit index.

That is the StatefulSet guarantee doing its job, and it is why a Deployment
would be wrong here.

One thing measured during that test was worth fixing: a single pod deletion
produced 31 leadership changes and drove the term from 3 to 21 before
settling. A restarting node campaigned immediately, raising the term and
deposing a healthy leader, which made a rolling restart cost far more
availability than it should.

Pre-vote (§9.6) has since been implemented, so a node now asks whether an
election would be won before starting one and a follower still hearing from
its leader answers no. Restarting a follower repeatedly now leaves the term,
the leader, and the leadership-change counter unmoved. See
[benchmarks.md](benchmarks.md).

## Snapshots and size

A follower that falls behind the leader's compaction point can only be caught
up by a state machine image, and that image is the one message in the protocol
whose size follows your data rather than the protocol. It is streamed in 1 MiB
chunks over its own RPC for that reason. Sending it as a single message worked
fine until the data outgrew the receiver's message size limit, at which point
the follower simply never recovered and nothing in the symptom mentioned size.

The receiver refuses a stream larger than 1 GiB, refuses data arriving before
the header that describes it, and hands nothing to the consensus layer until
the whole image has arrived. A partial snapshot is not a smaller snapshot, it
is a corrupt one, and restoring from it would leave the state machine silently
wrong rather than merely behind.

Both ends still hold the whole image in memory while this happens, so plan for
a node's peak memory to exceed its state machine size during a transfer.

Applying is bounded for a related reason. Committed entries reach the state
machine a thousand at a time, because that work shares a goroutine with
ticking the clock and reading messages: a node applying a large backlog in one
pass stops answering heartbeats for as long as it takes, and a long enough
pass costs it leadership.

Ordinary replication is bounded the same way and for the same reason. A leader
sends a lagging follower at most a megabyte of entries at a time rather than
the whole backlog, since a follower can fall arbitrarily far behind and a
single message carrying all of it would be undeliverable. The remaining slices
follow each acknowledgement immediately, so catching up runs at network speed
rather than at one slice per heartbeat.

## One process per data directory

A node takes an exclusive lock on its data directory and refuses to start if
another process holds it. Two processes sharing one used to be accepted in
silence, and it is quietly the worst thing in this document: both append to
the same write-ahead log and the last hard state written wins, so each node
records a vote in the same term and the survivor inherits the other's. A node
restarting there believes it voted for a candidate it never heard from, and
that record is the only thing standing between a term and two leaders.

The lock lives on a file descriptor rather than in a file, so the kernel drops
it when the process dies however it dies. A crashed node restarts without
anyone deleting anything, which a lock file holding a process ID would not
allow.

## Shutting down

A SIGTERM on an idle node releases its files in about a sixth of a second. A
node under load used to take as long as its slowest client was prepared to
wait, which is worth understanding because the cause is not obvious.

Stopping the gRPC server gracefully refuses new calls on every service at
once, and one of those services is Raft. A leader that can no longer receive
follower responses cannot commit, so the client writes already in its hands
can never finish, and they sit there until the client gives up. Measured,
shutdown took 3, 10 and 20 seconds against clients whose request timeouts were
3, 10 and 20 seconds: the server was not in control of its own shutdown at
all.

It is now bounded at five seconds, after which remaining connections are
closed and those clients get an error to retry against the new leader. That
matters for the `terminationGracePeriodSeconds: 30` above: a client patient
enough to outlast the grace period would otherwise turn an orderly stop into a
kill, and a killed node comes back with a torn log tail to recover rather than
a clean one.

## Getting the peer list wrong

Every node learns the cluster from `--peers`, and the ways that list can be
wrong mostly fail late rather than at startup. The parser refuses each of them
with a reason: a missing or unreadable ID, the reserved ID zero, a member
listed twice, an address that is blank.

Two members sharing an address is the one worth calling out, because it used
to be accepted. The second node to start cannot bind the port and dies, while
everyone else dialing it reaches the first node's process, so the cluster
believes it has a member it does not. Quorum is still met by the survivors,
which is exactly the problem: nothing looks broken and the fault tolerance you
paid for is gone. The error now names both IDs and the address they collide
on.

## Configuration that matters

| Flag | Default | Notes |
| --- | --- | --- |
| `--fsync` | `true` | Turning it off takes writes from 598 to 2,980 a second on a local three node cluster, because the durable write drops from 6.8ms to 0.06ms. It also voids the durability guarantee Raft's correctness assumes, which is the whole reason the write was slow. It exists for benchmarking, not production. |
| `--election-ticks` | `10` | In ticks, so the real timeout is this times `--tick`. Too low on a slow network causes elections during normal operation. |
| `--snapshot-threshold` | `10000` | Entries applied past the last snapshot before another is taken. Lower means faster recovery and more disk work. |
| `--metrics-listen` | empty | Disabled unless set. See [observability.md](observability.md). |

## Scaling the cluster

Membership is a runtime operation through `ClusterService`, not a
configuration change. Adding a node to a StatefulSet means raising `replicas`
*and* calling `AddNode`, because the new pod is not a member until the existing
cluster has agreed to admit it through joint consensus. A pod that is running
but not a member will sit there campaigning and never win, which is the correct
behaviour and looks alarming if you were expecting the replica count alone to
do the work.
