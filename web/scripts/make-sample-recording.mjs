// Builds the small SYNTHETIC sample public/recording.jsonl (deterministic, ~60 s, header
// "synthetic": true). It is a stand-in so the replay path works end to end; the real capture of a
// live demo (scripts/record-session.mjs) replaces it in D8.
//   node scripts/make-sample-recording.mjs public/recording.jsonl
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const BASE = Date.parse("2026-11-01T10:00:00Z");
const AZ = ["us-east-1a", "us-east-1b", "us-east-1c"];

function doc(sec, s) {
  const commit = 100 + sec * 3;
  const nodes = [1, 2, 3].map((id) => {
    const up = !(id === 1 && sec >= 10 && sec < 25);
    const role = !up ? "unknown" : id === s.leader ? "leader" : "follower";
    const lag = id === 1 && sec >= 25 && sec < 28 ? (28 - sec) * 30 : 0;
    const c = up ? commit - lag : 0;
    return {
      id,
      az: AZ[id - 1],
      up,
      role,
      term: up ? s.term : 0,
      commitIndex: c,
      lastApplied: c,
      logLen: up ? Math.min(60, c - 80 + 1) : 0,
      snapshotIndex: up ? 80 : 0,
      lastContactMs: up ? 300 : null,
      paused: false,
    };
  });
  const active = sec >= 10 && sec < 25;
  const chaos = active
    ? {
        state: "active",
        action: "kill",
        target: 1,
        expiresAt: new Date(BASE + 55_000).toISOString(),
      }
    : { state: "idle" };
  return { leaderId: s.leader, nodes, chaos };
}

export function buildSyntheticRecording() {
  const out = [];
  const header = {
    v: 1,
    kind: "header",
    recordedAt: new Date(BASE).toISOString(),
    durationMs: 60_000,
    synthetic: true,
  };
  let id = 0;
  const ev = (t, name, data) => out.push({ t, kind: "event", id: ++id, name, data });
  const s = { leader: 1, term: 3 };
  for (let sec = 0; sec < 60; sec++) {
    const t = sec * 1000;
    if (sec === 10) {
      ev(t, "chaos", { kind: "fault", action: "kill", target: 1, state: "active" });
      ev(t + 100, "node_down", { node: 1 });
      s.leader = 0;
    }
    if (sec === 11) {
      s.term = 4;
      ev(t, "state", { node: 2, term: 4, role: "candidate", leaderId: 0, seq: 1, unixNano: 1 });
      ev(t + 300, "state", { node: 2, term: 4, role: "leader", leaderId: 2, seq: 2, unixNano: 2 });
      s.leader = 2;
    }
    if (sec === 25) {
      ev(t, "chaos", { kind: "heal", action: "heal", target: 0, state: "recovering" });
      ev(t + 200, "node_up", { node: 1 });
    }
    if (sec === 30) ev(t, "snapshot", { node: 3, snapshotIndex: 80 });
    if (sec === 40) ev(t, "verify", { phase: "start", id: "vsample" });
    if (sec === 42)
      ev(t, "verify", { phase: "fault", id: "vsample", action: "partition", ok: true });
    if (sec === 50) {
      ev(t, "verify", { phase: "done", id: "vsample", verdict: "linearizable", ops: 812 });
      out.push({
        t: t + 50,
        kind: "verify",
        data: {
          id: "vsample",
          seed: "1",
          status: "done",
          verdict: "linearizable",
          startedAt: new Date(BASE + 40_000).toISOString(),
          endedAt: new Date(BASE + 50_000).toISOString(),
          elapsedMs: 10_000,
          ops: 812,
          reads: 330,
          writes: 482,
          ok: 790,
          failed: 2,
          unknown: 20,
          explored: 9000,
          recovered: true,
          faults: [
            { action: "partition", target: 2, atMs: 2000, ok: true },
            { action: "heal", atMs: 6000, ok: true },
          ],
        },
      });
    }
    out.push({ t, kind: "cluster", data: doc(sec, s) });
  }
  out.sort((a, b) => a.t - b.t);
  return `${[JSON.stringify(header), ...out.map((l) => JSON.stringify(l))].join("\n")}\n`;
}

if (process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url)) {
  const path = process.argv[2];
  if (!path) {
    process.stderr.write("usage: make-sample-recording.mjs OUT\n");
    process.exit(2);
  }
  writeFileSync(path, buildSyntheticRecording());
}
