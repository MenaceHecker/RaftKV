import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const script = join(process.cwd(), "scripts", "check-no-go.mjs");
function run(dir: string) {
  return spawnSync(process.execPath, [script, dir], { encoding: "utf8" });
}
function tree(files: string[]): string {
  const root = mkdtempSync(join(tmpdir(), "nm-"));
  for (const f of files) {
    const p = join(root, f);
    mkdirSync(join(p, ".."), { recursive: true });
    writeFileSync(p, "x");
  }
  return root;
}

describe("check-no-go", () => {
  it("passes on a tree with no Go artefacts", () => {
    const r = run(tree(["a/index.js", "b/c/package.json"]));
    expect(r.status).toBe(0);
  });
  it.each([
    ["a Go source file", "flatted/golang/pkg/flatted/flatted.go"],
    ["go.mod", "x/go.mod"],
    ["go.sum", "x/y/go.sum"],
    ["a nested .git entry", "pkg/.git/HEAD"],
  ])("fails on %s", (_n, f) => {
    const r = run(tree([f, "ok.js"]));
    expect(r.status).toBe(1);
    expect(r.stderr).toContain(f.split("/")[0] as string);
  });
  it("passes on the real node_modules of this project", () => {
    expect(run(join(process.cwd(), "node_modules")).status).toBe(0);
  });
});
