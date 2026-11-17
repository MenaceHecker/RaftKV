import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const script = join(process.cwd(), "scripts", "check-dist.mjs");
function run(files: Record<string, string>) {
  const root = mkdtempSync(join(tmpdir(), "dist-"));
  for (const [name, text] of Object.entries(files)) {
    const p = join(root, name);
    mkdirSync(join(p, ".."), { recursive: true });
    writeFileSync(p, text);
  }
  return spawnSync(process.execPath, [script, root], { encoding: "utf8" });
}

describe("check-dist", () => {
  it("passes a bundle with one Bearer header template", () => {
    expect(run({ "assets/a.js": "h.Authorization = 'Bearer ' + t" }).status).toBe(0);
  });
  it("fails on a second Bearer mention", () => {
    const r = run({ "assets/a.js": "Bearer x", "assets/b.js": "Bearer y" });
    expect(r.status).toBe(1);
    expect(r.stderr).toMatch(/Bearer/);
  });
  it("fails on key-like strings", () => {
    expect(run({ "a.js": "AKIAABCDEFGHIJKLMNOP" }).status).toBe(1);
    expect(run({ "a.js": "-----BEGIN RSA PRIVATE KEY-----" }).status).toBe(1);
    expect(run({ "a.js": "admin.token-file" }).status).toBe(1);
  });
});
