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

## Configuration that matters

| Flag | Default | Notes |
| --- | --- | --- |
| `--fsync` | `true` | Turning it off makes writes far faster and voids the durability guarantee Raft's correctness assumes. It exists for benchmarking, not production. |
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
