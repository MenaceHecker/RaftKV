import { existsSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { parseRecording } from "./state/replay";

const path = join(process.cwd(), "public", "recording.jsonl");

describe("public/recording.jsonl", () => {
  it("exists, is at most 300 KB and parses without bad lines", () => {
    expect(existsSync(path)).toBe(true);
    expect(statSync(path).size).toBeLessThanOrEqual(300 * 1024);
    const rec = parseRecording(readFileSync(path, "utf8"));
    expect(rec).not.toBeNull();
    expect(rec?.badLines).toBe(0);
  });

  it("is labelled synthetic until D8 commits the real capture, and covers every line kind", () => {
    const rec = parseRecording(readFileSync(path, "utf8"));
    expect(rec?.header.synthetic).toBe(true);
    const kinds = new Set(rec?.lines.map((l) => l.kind));
    expect([...kinds].sort()).toEqual(["cluster", "event", "verify"]);
  });

  it("contains no token-looking text", () => {
    const raw = readFileSync(path, "utf8");
    expect(raw).not.toMatch(/Bearer|authorization/i);
  });
});
