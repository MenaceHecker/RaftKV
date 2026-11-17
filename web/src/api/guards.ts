import type {
  ChaosAck,
  ChaosState,
  ChaosView,
  Cluster,
  HealResult,
  NodeView,
  VerifyFault,
  VerifyLast,
  VerifyReport,
  VerifyRunning,
} from "./types";

// Hand-written parsers: tolerant of extra fields, strict about missing or mistyped ones.
// Each returns null instead of throwing so a drifting gateway degrades the UI, not crashes it.

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);
const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
const isStr = (v: unknown): v is string => typeof v === "string";
const isBool = (v: unknown): v is boolean => typeof v === "boolean";
const isNumArray = (v: unknown): v is number[] => Array.isArray(v) && v.every(isNum);

function node(v: unknown): NodeView | null {
  if (!isObj(v)) return null;
  const { id, az, up, role, term, commitIndex, lastApplied, logLen, snapshotIndex } = v;
  const { lastContactMs, paused } = v;
  if (
    !isNum(id) ||
    !isStr(az) ||
    !isBool(up) ||
    !isStr(role) ||
    !isNum(term) ||
    !isNum(commitIndex) ||
    !isNum(lastApplied) ||
    !isNum(logLen) ||
    !isNum(snapshotIndex) ||
    !isBool(paused) ||
    !(lastContactMs === null || isNum(lastContactMs))
  ) {
    return null;
  }
  return {
    id,
    az,
    up,
    role,
    term,
    commitIndex,
    lastApplied,
    logLen,
    snapshotIndex,
    lastContactMs,
    paused,
  };
}

const CHAOS_STATES: readonly string[] = ["idle", "active", "recovering"];

function chaos(v: unknown): ChaosView | null {
  if (!isObj(v) || !isStr(v.state) || !CHAOS_STATES.includes(v.state)) return null;
  const out: ChaosView = { state: v.state as ChaosState };
  if (v.action !== undefined) {
    if (!isStr(v.action)) return null;
    out.action = v.action;
  }
  if (v.target !== undefined) {
    if (!isNum(v.target)) return null;
    out.target = v.target;
  }
  if (v.expiresAt !== undefined) {
    if (!isStr(v.expiresAt)) return null;
    out.expiresAt = v.expiresAt;
  }
  return out;
}

export function parseCluster(v: unknown): Cluster | null {
  if (!isObj(v) || !isNum(v.leaderId) || !Array.isArray(v.nodes)) return null;
  const nodes: NodeView[] = [];
  for (const n of v.nodes) {
    const p = node(n);
    if (!p) return null;
    nodes.push(p);
  }
  const c = chaos(v.chaos);
  if (!c) return null;
  return { leaderId: v.leaderId, nodes, chaos: c };
}

function fault(v: unknown): VerifyFault | null {
  if (!isObj(v) || !isStr(v.action) || !isNum(v.atMs) || !isBool(v.ok)) return null;
  const out: VerifyFault = { action: v.action, atMs: v.atMs, ok: v.ok };
  if (v.target !== undefined) {
    if (!isNum(v.target)) return null;
    out.target = v.target;
  }
  if (v.error !== undefined) {
    if (!isStr(v.error)) return null;
    out.error = v.error;
  }
  return out;
}

const VERDICTS: readonly string[] = ["linearizable", "not_linearizable", "inconclusive"];

export function parseVerifyReport(v: unknown): VerifyReport | null {
  if (!isObj(v)) return null;
  if (!isStr(v.id) || !isStr(v.seed)) return null;
  if (v.status !== "done" && v.status !== "aborted") return null;
  if (!isStr(v.verdict) || !VERDICTS.includes(v.verdict)) return null;
  if (!isStr(v.startedAt) || !isStr(v.endedAt)) return null;
  const nums = [
    "elapsedMs",
    "ops",
    "reads",
    "writes",
    "ok",
    "failed",
    "unknown",
    "explored",
  ] as const;
  for (const k of nums) if (!isNum(v[k])) return null;
  if (!isBool(v.recovered) || !Array.isArray(v.faults)) return null;
  const faults: VerifyFault[] = [];
  for (const f of v.faults) {
    const p = fault(f);
    if (!p) return null;
    faults.push(p);
  }
  const out: VerifyReport = {
    id: v.id,
    seed: v.seed,
    status: v.status,
    verdict: v.verdict as VerifyReport["verdict"],
    startedAt: v.startedAt,
    endedAt: v.endedAt,
    elapsedMs: v.elapsedMs as number,
    ops: v.ops as number,
    reads: v.reads as number,
    writes: v.writes as number,
    ok: v.ok as number,
    failed: v.failed as number,
    unknown: v.unknown as number,
    explored: v.explored as number,
    recovered: v.recovered,
    faults,
  };
  if (v.counterexample !== undefined) {
    if (!isStr(v.counterexample)) return null;
    out.counterexample = v.counterexample;
  }
  return out;
}

function running(v: unknown): VerifyRunning | null {
  if (!isObj(v) || v.status !== "running" || !isStr(v.id) || !isStr(v.startedAt)) return null;
  return { id: v.id, status: "running", startedAt: v.startedAt };
}

export function parseVerifyLast(v: unknown): VerifyLast | null {
  return running(v) ?? parseVerifyReport(v);
}

export function parseChaosAck(v: unknown): ChaosAck | null {
  if (!isObj(v)) return null;
  if (!isStr(v.action) || !isNum(v.target) || !isStr(v.expiresAt) || !isNum(v.autoHealSeconds)) {
    return null;
  }
  return {
    action: v.action,
    target: v.target,
    expiresAt: v.expiresAt,
    autoHealSeconds: v.autoHealSeconds,
  };
}

export function parseHeal(v: unknown): HealResult | null {
  if (!isObj(v) || !isNumArray(v.healed) || !isNumArray(v.failed) || !isStr(v.state)) return null;
  if (!CHAOS_STATES.includes(v.state)) return null;
  return { healed: v.healed, failed: v.failed, state: v.state as ChaosState };
}

/** Parses `{status}` of /healthz and the KV answers, which have no structure worth a type. */
export function parseKeyed(v: unknown): { key: string } | null {
  return isObj(v) && isStr(v.key) ? { key: v.key } : null;
}
export function parseKvValue(v: unknown): { key: string; value: string } | null {
  return isObj(v) && isStr(v.key) && isStr(v.value) ? { key: v.key, value: v.value } : null;
}
export function parseHealth(v: unknown): { status: string } | null {
  return isObj(v) && isStr(v.status) ? { status: v.status } : null;
}
export function parseRunAck(v: unknown): { id: string; seed: string; status: "running" } | null {
  return isObj(v) && isStr(v.id) && isStr(v.seed) && v.status === "running"
    ? { id: v.id, seed: v.seed, status: "running" }
    : null;
}
