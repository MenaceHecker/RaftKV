import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { CAS_DETAIL } from "./api/types";

// vitest runs with the package directory (web/) as cwd.
const root = process.cwd();
const src = join(root, "src");

function walk(dir: string, out: string[] = []): string[] {
  for (const n of readdirSync(dir)) {
    const p = join(dir, n);
    if (statSync(p).isDirectory()) walk(p, out);
    else out.push(p);
  }
  return out;
}
const sources = () =>
  walk(src).filter(
    (f) => /\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f) && !f.includes("/test/"),
  );

describe("static guarantees", () => {
  it("CAS_DETAIL is the exact text in the Go source", () => {
    const go = readFileSync(join(root, "..", "internal", "gateway", "kv.go"), "utf8");
    expect(go).toContain(`"detail": "${CAS_DETAIL}"`);
  });

  it("reads no import.meta.env variable except the three documented ones", () => {
    const allowed = new Set(["VITE_GATEWAY_URL", "VITE_REPLAY_ONLY", "VITE_REPO_URL"]);
    const found = new Set<string>();
    for (const f of sources()) {
      for (const m of readFileSync(f, "utf8").matchAll(/import\.meta\.env\.([A-Za-z0-9_]+)/g))
        found.add(m[1] as string);
    }
    expect([...found].filter((v) => !allowed.has(v))).toEqual([]);
    expect([...found].sort()).toEqual([...allowed].sort());
  });

  it("only the admin module touches storage, and only sessionStorage", () => {
    for (const f of sources()) {
      const text = readFileSync(f, "utf8");
      expect(text, f).not.toMatch(/localStorage|document\.cookie|indexedDB/);
      if (!f.endsWith("state/admin.ts")) expect(text, f).not.toMatch(/sessionStorage/);
    }
  });

  it("never logs to the console and never uses innerHTML", () => {
    for (const f of sources()) {
      const text = readFileSync(f, "utf8");
      expect(text, f).not.toMatch(/console\.(log|info|warn|error|debug)/);
      expect(text, f).not.toMatch(/innerHTML|dangerouslySetInnerHTML/);
    }
  });

  it("attaches Authorization only in the client", () => {
    for (const f of sources()) {
      if (f.endsWith("api/client.ts")) continue;
      expect(readFileSync(f, "utf8"), f).not.toMatch(/Authorization|Bearer/);
    }
  });
});

describe("vercel.json", () => {
  const load = () =>
    JSON.parse(readFileSync(join(root, "vercel.json"), "utf8")) as {
      outputDirectory: string;
      buildCommand: string;
      headers: { source: string; headers: { key: string; value: string }[] }[];
    };
  const h = (k: string) =>
    load()
      .headers.flatMap((x) => x.headers)
      .find((x) => x.key.toLowerCase() === k.toLowerCase())?.value;

  it("builds web/ into dist", () => {
    expect(load().buildCommand).toBe("npm run build");
    expect(load().outputDirectory).toBe("dist");
  });
  it("sets the security headers and a CSP naming the (unconfirmed) production gateway", () => {
    expect(h("X-Content-Type-Options")).toBe("nosniff");
    expect(h("Referrer-Policy")).toBe("no-referrer");
    const csp = h("Content-Security-Policy") ?? "";
    expect(csp).toContain("default-src 'self'");
    expect(csp).toContain("connect-src 'self' https://api.raftkv.tushar25.com");
    expect(csp).not.toMatch(/unsafe-eval|\*/);
  });
});
