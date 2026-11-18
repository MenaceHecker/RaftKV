import type { Cluster, EventName } from "../api/types";

export interface TimelineRow {
  key: number;
  at: number; // browser receive time, ms since epoch
  kind: string; // an event name, "note" or "caught_up"
  text: string;
  derived?: boolean;
  node?: number;
}
export interface Timeline {
  rows: TimelineRow[];
  awaiting: number[]; // nodes that came back up and have not caught up yet
  nextKey: number;
}
export const TIMELINE_CAP = 500;
/** "Caught up" means the node's applied index is within this of the leader's. */
const CAUGHT_UP_WITHIN = 2;

export function emptyTimeline(): Timeline {
  return { rows: [], awaiting: [], nextKey: 1 };
}

type Obj = Record<string, unknown>;
const obj = (v: unknown): Obj | null =>
  typeof v === "object" && v !== null && !Array.isArray(v) ? (v as Obj) : null;
const num = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const str = (v: unknown): string | null => (typeof v === "string" ? v : null);

function push(t: Timeline, row: Omit<TimelineRow, "key">, awaiting = t.awaiting): Timeline {
  const rows = [...t.rows, { ...row, key: t.nextKey }];
  return {
    rows: rows.length > TIMELINE_CAP ? rows.slice(rows.length - TIMELINE_CAP) : rows,
    awaiting,
    nextKey: t.nextKey + 1,
  };
}

/** Turn one SSE event (as the gateway documents it, events.go) into a row. Bad shapes are dropped. */
export function addEvent(t: Timeline, name: EventName, data: unknown, at: number): Timeline {
  const d = obj(data);
  switch (name) {
    case "state": {
      const node = num(d?.node);
      const term = num(d?.term);
      const role = str(d?.role);
      if (node === null || term === null || role === null) return t;
      const text =
        role === "leader"
          ? `node ${node} became leader, term ${term}`
          : `node ${node} became ${role} (term ${term})`;
      return push(t, { at, kind: name, text, node });
    }
    case "node_down": {
      const node = num(d?.node);
      if (node === null) return t;
      return push(
        t,
        { at, kind: name, text: `node ${node} went down`, node },
        t.awaiting.filter((n) => n !== node),
      );
    }
    case "node_up": {
      const node = num(d?.node);
      if (node === null) return t;
      const awaiting = t.awaiting.includes(node) ? t.awaiting : [...t.awaiting, node];
      return push(t, { at, kind: name, text: `node ${node} came back up`, node }, awaiting);
    }
    case "snapshot": {
      const node = num(d?.node);
      const idx = num(d?.snapshotIndex);
      if (node === null || idx === null) return t;
      return push(t, {
        at,
        kind: name,
        text: `node ${node} installed a snapshot at index ${idx}`,
        node,
      });
    }
    case "commit": {
      const node = num(d?.node);
      const idx = num(d?.commitIndex);
      if (node === null || idx === null) return t;
      return push(t, { at, kind: name, text: `node ${node} commit index ${idx}`, node });
    }
    case "chaos": {
      const kind = str(d?.kind);
      const action = str(d?.action);
      if (kind === null || action === null) return t;
      const target = num(d?.target);
      const text = kind === "heal" ? "heal" : `fault: ${action}${target ? ` node ${target}` : ""}`;
      return push(t, { at, kind: name, text });
    }
    case "verify": {
      const phase = str(d?.phase);
      if (phase === "start") return push(t, { at, kind: name, text: "verification started" });
      if (phase === "fault") {
        const action = str(d?.action) ?? "fault";
        return push(t, {
          at,
          kind: name,
          text: `verification fault: ${action} ${d?.ok === true ? "ok" : "failed"}`,
        });
      }
      if (phase === "done") {
        const verdict = str(d?.verdict) ?? "unknown";
        const ops = num(d?.ops);
        return push(t, {
          at,
          kind: name,
          text: `verification done: ${verdict}${ops === null ? "" : ` (${ops} ops)`}`,
        });
      }
      return t;
    }
    case "resync":
      return push(t, { at, kind: name, text: "events resynchronised from the cluster document" });
  }
}

export function addNote(t: Timeline, text: string, at: number): Timeline {
  return push(t, { at, kind: "note", text });
}

/**
 * "Caught up" is derived here, not reported by the gateway: a node that came back up has caught
 * up when its lastApplied is within 2 of the leader's. The row says so.
 */
export function observeCluster(t: Timeline, c: Cluster, at: number): Timeline {
  if (t.awaiting.length === 0) return t;
  const leader = c.nodes.find((n) => n.id === c.leaderId && n.up);
  if (!leader) return t;
  let out = t;
  for (const id of t.awaiting) {
    const n = c.nodes.find((x) => x.id === id);
    if (n?.up && leader.lastApplied - n.lastApplied <= CAUGHT_UP_WITHIN) {
      out = push(
        out,
        {
          at,
          kind: "caught_up",
          text: `node ${id} caught up (derived: applied index within ${CAUGHT_UP_WITHIN} of the leader's)`,
          derived: true,
          node: id,
        },
        out.awaiting.filter((x) => x !== id),
      );
    }
  }
  return out;
}
