import type { ChaosView, Cluster, NodeView, VerifyReport } from "../api/types";

export function node(id: number, over: Partial<NodeView> = {}): NodeView {
  return {
    id,
    az: `us-east-1${"abc"[id - 1] ?? "a"}`,
    up: true,
    role: "follower",
    term: 3,
    commitIndex: 120,
    lastApplied: 120,
    logLen: 40,
    snapshotIndex: 80,
    lastContactMs: 300,
    paused: false,
    ...over,
  };
}

export function cluster(over: Partial<Cluster> = {}): Cluster {
  return {
    leaderId: 1,
    nodes: [node(1, { role: "leader" }), node(2), node(3)],
    chaos: { state: "idle" },
    ...over,
  };
}

export function withChaos(c: Cluster, chaos: ChaosView): Cluster {
  return { ...c, chaos };
}

export function downNode(id: number): NodeView {
  return node(id, {
    up: false,
    role: "unknown",
    term: 0,
    commitIndex: 0,
    lastApplied: 0,
    logLen: 0,
    snapshotIndex: 0,
    lastContactMs: null,
  });
}

export function report(over: Partial<VerifyReport> = {}): VerifyReport {
  return {
    id: "v1a2b",
    seed: "12345",
    status: "done",
    verdict: "linearizable",
    startedAt: "2026-11-01T10:00:00Z",
    endedAt: "2026-11-01T10:01:00Z",
    elapsedMs: 60000,
    ops: 812,
    reads: 330,
    writes: 482,
    ok: 790,
    failed: 2,
    unknown: 20,
    explored: 9000,
    recovered: true,
    faults: [
      { action: "partition", target: 1, atMs: 10000, ok: true },
      { action: "heal", atMs: 25000, ok: true },
      { action: "kill", target: 2, atMs: 35000, ok: true },
    ],
    ...over,
  };
}
