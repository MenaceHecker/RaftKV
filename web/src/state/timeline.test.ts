import { describe, expect, it } from "vitest";
import { cluster, node } from "../test/fixtures";
import {
  addEvent,
  addNote,
  emptyTimeline,
  observeCluster,
  TIMELINE_CAP,
  type Timeline,
} from "./timeline";

const texts = (t: Timeline) => t.rows.map((r) => r.text);

describe("timeline rows", () => {
  it("describes role changes from state events", () => {
    let t = emptyTimeline();
    t = addEvent(
      t,
      "state",
      { node: 2, term: 4, role: "candidate", leaderId: 0, seq: 1, unixNano: 1 },
      1000,
    );
    t = addEvent(
      t,
      "state",
      { node: 2, term: 4, role: "leader", leaderId: 2, seq: 2, unixNano: 2 },
      1100,
    );
    expect(texts(t)).toEqual(["node 2 became candidate (term 4)", "node 2 became leader, term 4"]);
    expect(t.rows[0]?.at).toBe(1000);
  });

  it("describes node_down, node_up, snapshot, chaos, verify and resync", () => {
    let t = emptyTimeline();
    t = addEvent(t, "node_down", { node: 1 }, 1);
    t = addEvent(t, "node_up", { node: 1 }, 2);
    t = addEvent(t, "snapshot", { node: 3, snapshotIndex: 90 }, 3);
    t = addEvent(t, "chaos", { kind: "fault", action: "kill", target: 1, state: "active" }, 4);
    t = addEvent(t, "chaos", { kind: "heal", action: "heal", target: 0, state: "idle" }, 5);
    t = addEvent(t, "verify", { phase: "start", id: "v1" }, 6);
    t = addEvent(t, "verify", { phase: "done", id: "v1", verdict: "inconclusive", ops: 5 }, 7);
    t = addEvent(t, "resync", cluster(), 8);
    expect(texts(t)).toEqual([
      "node 1 went down",
      "node 1 came back up",
      "node 3 installed a snapshot at index 90",
      "fault: kill node 1",
      "heal",
      "verification started",
      "verification done: inconclusive (5 ops)",
      "events resynchronised from the cluster document",
    ]);
  });

  it("keeps commit rows but marks them kind=commit so the view can hide them", () => {
    const t = addEvent(emptyTimeline(), "commit", { node: 1, commitIndex: 12 }, 1);
    expect(t.rows).toHaveLength(1);
    expect(t.rows[0]?.kind).toBe("commit");
  });

  it("ignores payloads of the wrong shape instead of throwing", () => {
    const t = addEvent(emptyTimeline(), "state", { nope: true }, 1);
    expect(t.rows).toHaveLength(0);
  });

  it("addNote appends a plain row", () => {
    const t = addNote(emptyTimeline(), "recording restarted", 5);
    expect(t.rows[0]).toMatchObject({ kind: "note", text: "recording restarted" });
  });

  it("caps at 500 rows keeping the newest", () => {
    let t = emptyTimeline();
    for (let i = 0; i < TIMELINE_CAP + 25; i++) t = addNote(t, `n${i}`, i);
    expect(t.rows).toHaveLength(TIMELINE_CAP);
    expect(t.rows[0]?.text).toBe("n25");
    expect(t.rows.at(-1)?.text).toBe(`n${TIMELINE_CAP + 24}`);
  });

  it("row keys stay unique", () => {
    let t = emptyTimeline();
    for (let i = 0; i < 600; i++) t = addNote(t, "x", i);
    expect(new Set(t.rows.map((r) => r.key)).size).toBe(t.rows.length);
  });
});

describe("derived 'caught up'", () => {
  it("is added once a returned node's lastApplied is within 2 of the leader's, and labelled derived", () => {
    let t = emptyTimeline();
    t = addEvent(t, "node_down", { node: 2 }, 1);
    t = addEvent(t, "node_up", { node: 2 }, 2);
    expect(t.awaiting).toEqual([2]);

    const behind = cluster({
      nodes: [
        node(1, { role: "leader", lastApplied: 200 }),
        node(2, { lastApplied: 150 }),
        node(3),
      ],
    });
    t = observeCluster(t, behind, 3);
    expect(t.rows.some((r) => r.kind === "caught_up")).toBe(false);

    const near = cluster({
      nodes: [
        node(1, { role: "leader", lastApplied: 200 }),
        node(2, { lastApplied: 198 }),
        node(3),
      ],
    });
    t = observeCluster(t, near, 4);
    const row = t.rows.find((r) => r.kind === "caught_up");
    expect(row?.derived).toBe(true);
    expect(row?.text).toMatch(/node 2 caught up/);
    expect(row?.text).toMatch(/derived/);
    expect(t.awaiting).toEqual([]);

    // and only once
    t = observeCluster(t, near, 5);
    expect(t.rows.filter((r) => r.kind === "caught_up")).toHaveLength(1);
  });

  it("does not fire while the node is still down or there is no leader", () => {
    let t = emptyTimeline();
    t = addEvent(t, "node_up", { node: 2 }, 1);
    const down = cluster({ nodes: [node(1, { role: "leader" }), node(2, { up: false }), node(3)] });
    t = observeCluster(t, down, 2);
    const noLeader = cluster({ leaderId: 0, nodes: [node(1), node(2), node(3)] });
    t = observeCluster(t, noLeader, 3);
    expect(t.rows.some((r) => r.kind === "caught_up")).toBe(false);
    expect(t.awaiting).toEqual([2]);
  });
});
