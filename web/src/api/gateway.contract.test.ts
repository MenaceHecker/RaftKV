import { describe, expect, it } from "vitest";
import { createClient } from "./client";
import { CAS_DETAIL } from "./types";

// Run with: CONTRACT_GATEWAY=http://127.0.0.1:8080 npm run test:contract
// against scripts/dev-gateway.sh (the real gateway on its in-memory demo cluster). It checks the
// hand-written guards and the CAS text against the real internal/gateway, so the in-process
// mock used by `npm test` cannot drift unnoticed. Not part of `npm test`.
const base = process.env.CONTRACT_GATEWAY;

describe("real gateway contract", () => {
  it("has CONTRACT_GATEWAY set", () => {
    expect(base, "set CONTRACT_GATEWAY to the dev gateway URL").toBeTruthy();
  });

  const c = createClient({
    baseUrl: base ?? "http://127.0.0.1:1",
    fetch: globalThis.fetch,
    getToken: () => null,
    timeoutMs: 5000,
  });

  it("/healthz and /v1/cluster parse with the guards", async () => {
    expect(await c.health()).toEqual({ ok: true, data: { status: "ok" } });
    const r = await c.cluster();
    expect(r.ok, JSON.stringify(r)).toBe(true);
    if (r.ok) {
      expect(r.data.nodes).toHaveLength(3);
      expect(["idle", "active", "recovering"]).toContain(r.data.chaos.state);
    }
  });

  it("KV put, get, delete round-trip and the error shapes", async () => {
    const key = `contract-${Date.now()}`;
    expect(await c.kvPut(key, "v")).toEqual({ ok: true, data: { key } });
    expect(await c.kvGet(key)).toEqual({ ok: true, data: { key, value: "v" } });
    expect(await c.kvDelete(key)).toEqual({ ok: true, data: { key } });
    expect(await c.kvGet(key)).toMatchObject({ ok: false, status: 404 });
    expect(await c.kvPut("__verify/x", "v")).toMatchObject({ ok: false, status: 403 });
    expect(await c.kvPut("k", "x".repeat(1025))).toMatchObject({ ok: false, status: 413 });
  });

  it("CAS answers 501 with the exact detail text", async () => {
    expect(await c.kvCas("k", {})).toMatchObject({ ok: false, status: 501, detail: CAS_DETAIL });
  });

  it("verify/last is 404 or a parseable report or running marker", async () => {
    const r = await c.verifyLast();
    if (r.ok) expect(["running", "done", "aborted"]).toContain(r.data.status);
    else expect(r.status).toBe(404);
  });

  it("a fault and a heal parse, and the cluster document shows the fault", async () => {
    // The gateway counts its public chaos bucket as just used at start (docs/gateway.md), so the
    // first fault may be answered 429 with Retry-After: wait it out. A partition, unlike a
    // kill, is undone completely by heal, which keeps the demo cluster reusable.
    let f = await c.chaos.partition();
    for (let i = 0; i < 4 && !f.ok && f.status === 429; i++) {
      await new Promise((r) => setTimeout(r, (f.ok ? 1 : (f.retryAfterSec ?? 5)) * 1000 + 250));
      f = await c.chaos.partition();
    }
    expect(f.ok, JSON.stringify(f)).toBe(true);
    const during = await c.cluster();
    expect(during.ok && during.data.chaos.state !== "idle").toBe(true);
    const h = await c.chaos.heal();
    expect(h.ok, JSON.stringify(h)).toBe(true);
  }, 60_000);
});
