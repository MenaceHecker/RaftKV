// Shapes of the gateway API. Each cites the Go source it mirrors (internal/gateway/).
// Nothing here is invented: if the gateway does not send a field, it is not here.

export type Role = "leader" | "follower" | "candidate" | "unknown" | (string & {});

// poll.go nodeView
export interface NodeView {
  id: number;
  az: string;
  up: boolean;
  role: Role;
  term: number;
  commitIndex: number;
  lastApplied: number;
  logLen: number;
  snapshotIndex: number;
  lastContactMs: number | null;
  paused: boolean;
}

// chaos.go chaosView
export type ChaosState = "idle" | "active" | "recovering";
export interface ChaosView {
  state: ChaosState;
  action?: string;
  target?: number;
  expiresAt?: string;
}

// poll.go clusterView (0 = no leader)
export interface Cluster {
  leaderId: number;
  nodes: NodeView[];
  chaos: ChaosView;
}

// chaos.go chaosHandler success
export interface ChaosAck {
  action: string;
  target: number;
  expiresAt: string;
  autoHealSeconds: number;
}

// chaos.go handleHeal
export interface HealResult {
  healed: number[];
  failed: number[];
  state: ChaosState;
}

// verify.go verifyFault
export interface VerifyFault {
  action: string;
  target?: number;
  atMs: number;
  ok: boolean;
  error?: string;
}

export type Verdict = "linearizable" | "not_linearizable" | "inconclusive";

// verify.go Report (status done | aborted)
export interface VerifyReport {
  id: string;
  seed: string;
  status: "done" | "aborted";
  verdict: Verdict;
  startedAt: string;
  endedAt: string;
  elapsedMs: number;
  ops: number;
  reads: number;
  writes: number;
  ok: number;
  failed: number;
  unknown: number;
  explored: number;
  counterexample?: string;
  recovered: boolean;
  faults: VerifyFault[];
}

// verify.go handleVerifyRun (202) and handleVerifyLast while running
export interface VerifyRunAck {
  id: string;
  seed: string;
  status: "running";
}
export interface VerifyRunning {
  id: string;
  status: "running";
  startedAt: string;
}
export type VerifyLast = VerifyReport | VerifyRunning;

// events.go: the 8 SSE event names
export const EVENT_NAMES = [
  "state",
  "node_up",
  "node_down",
  "snapshot",
  "commit",
  "chaos",
  "verify",
  "resync",
] as const;
export type EventName = (typeof EVENT_NAMES)[number];

// kv.go handleGet / handlePut
export interface KvValue {
  key: string;
  value: string;
}
export interface KvKey {
  key: string;
}

// api.go writeError and friends
export interface ApiFailure {
  ok: false;
  status: number; // 0 = network error or timeout
  error: string;
  retryAfterSec?: number;
  outcome?: string; // kv.go kvError: "unknown" | "not applied"
  detail?: string; // kv.go handleCAS
}
export type ApiResult<T> = { ok: true; data: T } | ApiFailure;

// The exact text of kv.go handleCAS, pinned by a test against the Go source.
export const CAS_DETAIL =
  "compare-and-swap has no backing: the cluster supports only get, put and delete";
