import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { parseRecording } from "../src/state/replay";
import { cluster, report } from "../src/test/fixtures";
import { fakeRes, MockGateway } from "../src/test/mockGateway";
import { recordSession, writeRecording } from "./record-session.mjs";

let gw: MockGateway;
beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
});
afterEach(() => vi.useRealTimers());

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

async function record(seconds = 5, fetchImpl: typeof fetch = gw.fetch) {
  const p = recordSession({
    gateway: "http://gw",
    seconds,
    fetch: fetchImpl,
    sleep,
    now: () => Date.now(),
  });
  await vi.advanceTimersByTimeAsync(0);
  return p;
}

describe("record-session", () => {
  it("writes a header, cluster polls, verbatim events and the report on a verify done event", async () => {
    gw.setLastReport(report());
    const p = recordSession({
      gateway: "http://gw",
      seconds: 5,
      fetch: gw.fetch,
      sleep,
      now: () => Date.now(),
    });
    await vi.advanceTimersByTimeAsync(1500);
    gw.publish("state", { node: 2, term: 4, role: "leader", leaderId: 2, seq: 1, unixNano: 1 });
    await vi.advanceTimersByTimeAsync(500);
    gw.publish("verify", { phase: "done", id: "v1", verdict: "linearizable", ops: 1 });
    await vi.advanceTimersByTimeAsync(5000);
    const lines = await p;

    const rec = parseRecording(lines.join("\n"));
    expect(rec).not.toBeNull();
    expect(rec?.header).toMatchObject({ v: 1, kind: "header", synthetic: false });
    expect(rec?.badLines).toBe(0);
    const kinds = rec?.lines.map((l) => l.kind);
    expect(kinds?.filter((k) => k === "cluster").length).toBeGreaterThanOrEqual(5);
    const ev = rec?.lines.find((l) => l.kind === "event");
    expect(ev).toMatchObject({ kind: "event", name: "state", data: { node: 2, term: 4 } });
    expect(rec?.lines.some((l) => l.kind === "verify")).toBe(true);
    // t is relative to the start, in ms, and non-decreasing
    const ts = rec?.lines.map((l) => l.t) ?? [];
    expect(ts[0]).toBeGreaterThanOrEqual(0);
    expect(ts[0]).toBeLessThan(100);
    expect([...ts].sort((a, b) => a - b)).toEqual(ts);
  });

  it("never sends an Authorization header (public access only)", async () => {
    const p = record(3);
    await vi.advanceTimersByTimeAsync(4000);
    await p;
    expect(gw.calls.length).toBeGreaterThan(0);
    for (const c of gw.calls) expect(c.headers.authorization).toBeUndefined();
  });

  it("refuses to produce output containing the substring Bearer", async () => {
    const poisoned: typeof fetch = async (input, init) => {
      const r = await gw.fetch(input, init);
      if (String(input).endsWith("/v1/cluster")) {
        return fakeRes(200, JSON.stringify({ ...cluster(), note: "Authorization: Bearer abc" }));
      }
      return r;
    };
    const p = recordSession({
      gateway: "http://gw",
      seconds: 2,
      fetch: poisoned,
      sleep,
      now: () => Date.now(),
    });
    const settled = p.then(
      (lines) => ({ lines }),
      (error: Error) => ({ error }),
    );
    await vi.advanceTimersByTimeAsync(3000);
    const out = await settled;
    expect(out).toHaveProperty("error");
    expect((out as { error: Error }).error.message).toMatch(/Bearer/);
  });

  it("writeRecording writes the lines to the file and refuses Bearer too", async () => {
    vi.useRealTimers();
    const dir = mkdtempSync(join(tmpdir(), "rec-"));
    const file = join(dir, "out.jsonl");
    await writeRecording(file, ['{"v":1}', '{"t":1}']);
    expect(readFileSync(file, "utf8")).toBe('{"v":1}\n{"t":1}\n');
    await expect(writeRecording(join(dir, "bad.jsonl"), ['{"x":"Bearer t"}'])).rejects.toThrow(
      /Bearer/,
    );
  });
});
