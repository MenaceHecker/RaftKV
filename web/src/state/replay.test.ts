import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { buildSyntheticRecording } from "../../scripts/make-sample-recording.mjs";
import { cluster, report } from "../test/fixtures";
import { createPlayer, parseRecording } from "./replay";

const header = {
  v: 1,
  kind: "header",
  recordedAt: "2026-11-01T10:00:00Z",
  durationMs: 2000,
  synthetic: true,
};
const text = (...lines: unknown[]) =>
  lines.map((l) => (typeof l === "string" ? l : JSON.stringify(l))).join("\n");

describe("parseRecording", () => {
  it("parses the header and lines, sorted by t", () => {
    const rec = parseRecording(
      text(
        header,
        { t: 1000, kind: "cluster", data: cluster() },
        { t: 500, kind: "event", id: 1, name: "state", data: { node: 1 } },
        { t: 1500, kind: "verify", data: report() },
      ),
    );
    expect(rec?.header.synthetic).toBe(true);
    expect(rec?.header.durationMs).toBe(2000);
    expect(rec?.lines.map((l) => l.t)).toEqual([500, 1000, 1500]);
    expect(rec?.badLines).toBe(0);
  });

  it("skips and counts a bad line, skips unknown kinds silently", () => {
    const rec = parseRecording(
      text(
        header,
        "{broken",
        { t: 1, kind: "future", data: 1 },
        { t: 2, kind: "cluster", data: { nodes: 1 } },
        { t: 3, kind: "cluster", data: cluster() },
      ),
    );
    expect(rec?.lines).toHaveLength(1);
    expect(rec?.badLines).toBe(2); // broken JSON and a cluster document that fails the guard
  });

  it("rejects a file with no header, an unknown version, or no content", () => {
    expect(parseRecording(text({ t: 1, kind: "cluster", data: cluster() }))).toBeNull();
    expect(parseRecording(text({ ...header, v: 2 }))).toBeNull();
    expect(parseRecording("")).toBeNull();
  });

  it("parses the generator's synthetic recording", () => {
    const rec = parseRecording(buildSyntheticRecording());
    expect(rec).not.toBeNull();
    expect(rec?.badLines).toBe(0);
  });
});

describe("player", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  function setup() {
    const rec = parseRecording(
      text(
        header,
        { t: 0, kind: "cluster", data: cluster({ leaderId: 1 }) },
        { t: 500, kind: "event", id: 7, name: "node_down", data: { node: 2 } },
        { t: 1000, kind: "cluster", data: cluster({ leaderId: 3 }) },
        { t: 1200, kind: "verify", data: report() },
      ),
    );
    expect(rec).not.toBeNull();
    // biome-ignore lint/style/noNonNullAssertion: asserted just above
    const recording = rec!;
    const log: string[] = [];
    const player = createPlayer({
      recording,
      onCluster: (c) => log.push(`cluster:${c.leaderId}`),
      onEvent: (e) => log.push(`event:${e.name}:${e.id}`),
      onVerify: () => log.push("verify"),
      onRestart: () => log.push("restart"),
    });
    return { player, log };
  }

  it("plays lines in order at 1x", async () => {
    const { player, log } = setup();
    player.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(log).toEqual(["cluster:1"]);
    await vi.advanceTimersByTimeAsync(499);
    expect(log).toEqual(["cluster:1"]);
    await vi.advanceTimersByTimeAsync(1);
    expect(log).toEqual(["cluster:1", "event:node_down:7"]);
    await vi.advanceTimersByTimeAsync(500);
    await vi.advanceTimersByTimeAsync(200);
    expect(log).toEqual(["cluster:1", "event:node_down:7", "cluster:3", "verify"]);
    player.stop();
  });

  it("loops at durationMs with a restart notice", async () => {
    const { player, log } = setup();
    player.start();
    await vi.advanceTimersByTimeAsync(1999);
    expect(log).not.toContain("restart");
    await vi.advanceTimersByTimeAsync(1);
    expect(log.slice(-2)).toEqual(["restart", "cluster:1"]);
    await vi.advanceTimersByTimeAsync(500);
    expect(log.at(-1)).toBe("event:node_down:7");
    player.stop();
  });

  it("stop halts playback", async () => {
    const { player, log } = setup();
    player.start();
    await vi.advanceTimersByTimeAsync(0);
    player.stop();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(log).toEqual(["cluster:1"]);
  });
});
