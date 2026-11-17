import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MockGateway } from "../test/mockGateway";
import { createClient } from "./client";
import { CAS_DETAIL } from "./types";

let gw: MockGateway;
let token: string | null;
let unauthorized: ReturnType<typeof vi.fn>;

function make(fetchImpl: typeof fetch = gw.fetch) {
  return createClient({
    baseUrl: "http://gw",
    fetch: fetchImpl,
    getToken: () => token,
    onUnauthorized: unauthorized as unknown as () => void,
  });
}
function stub(status: number, body: unknown, headers: Record<string, string> = {}): typeof fetch {
  return async () =>
    new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json", ...headers },
    });
}

beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
  token = null;
  unauthorized = vi.fn();
});
afterEach(() => vi.useRealTimers());

describe("requests", () => {
  it("GET /v1/cluster returns the parsed document", async () => {
    const r = await make().cluster();
    expect(r.ok).toBe(true);
    if (r.ok) expect(r.data.leaderId).toBe(1);
    expect(gw.calls[0]).toMatchObject({ method: "GET", path: "/v1/cluster" });
  });

  it("PUT sends JSON content type, the body and an encoded key", async () => {
    const r = await make().kvPut("héllo wörld", "v1");
    expect(r).toEqual({ ok: true, data: { key: "héllo wörld" } });
    const c = gw.calls[0];
    expect(c?.method).toBe("PUT");
    expect(c?.path).toBe("/v1/kv/h%C3%A9llo%20w%C3%B6rld");
    expect(c?.headers["content-type"]).toBe("application/json");
    expect(c?.body).toBe('{"value":"v1"}');
  });

  it("GET and DELETE use the key path", async () => {
    await make().kvPut("a", "1");
    const g = await make().kvGet("a");
    expect(g).toEqual({ ok: true, data: { key: "a", value: "1" } });
    const d = await make().kvDelete("a");
    expect(d.ok).toBe(true);
    expect(gw.calls.map((c) => c.method)).toEqual(["PUT", "GET", "DELETE"]);
  });

  it("chaos endpoints use the documented paths and bodies", async () => {
    const c = make();
    await c.chaos.killLeader();
    expect(gw.calls.at(-1)).toMatchObject({ method: "POST", path: "/v1/chaos/kill-leader" });
    await c.chaos.heal();
    expect(gw.calls.at(-1)).toMatchObject({ method: "POST", path: "/v1/chaos/heal" });
    await c.chaos.partition();
    expect(gw.calls.at(-1)).toMatchObject({ path: "/v1/chaos/partition", body: null });
    await c.chaos.heal();
    await c.chaos.pause(2, 10);
    expect(gw.calls.at(-1)).toMatchObject({ path: "/v1/chaos/pause/2", body: '{"seconds":10}' });
    await c.chaos.heal();
    await c.chaos.delay(200, 10);
    expect(gw.calls.at(-1)).toMatchObject({
      path: "/v1/chaos/delay",
      body: '{"delayMs":200,"dropPercent":10}',
    });
  });

  it("verify run and last", async () => {
    const c = make();
    const run = await c.verifyRun();
    expect(run.ok && run.data.status).toBe("running");
    const last = await c.verifyLast();
    expect(last.ok && last.data.status).toBe("running");
    expect(gw.calls.map((x) => `${x.method} ${x.path}`)).toEqual([
      "POST /v1/verify/run",
      "GET /v1/verify/last",
    ]);
  });

  it("kvCas is typed but the gateway answers 501 with the detail", async () => {
    const r = await make().kvCas("k", { expect: "a", value: "b" });
    expect(r).toMatchObject({
      ok: false,
      status: 501,
      error: "not implemented",
      detail: CAS_DETAIL,
    });
    expect(gw.calls[0]?.path).toBe("/v1/kv/k/cas");
  });
});

describe("authorization header", () => {
  it("is sent on chaos and verify/run only, and only when a token is set", async () => {
    token = "tok";
    const c = make();
    await c.health();
    await c.cluster();
    await c.kvPut("a", "1");
    await c.kvGet("a");
    await c.kvDelete("a");
    await c.verifyLast();
    for (const call of gw.calls) expect(call.headers.authorization).toBeUndefined();

    gw.calls = [];
    await c.chaos.heal();
    await c.verifyRun();
    expect(gw.calls.map((x) => x.headers.authorization)).toEqual(["Bearer tok", "Bearer tok"]);
  });

  it("is absent without a token", async () => {
    const c = make();
    await c.chaos.killLeader();
    await c.verifyRun();
    for (const call of gw.calls) expect(call.headers.authorization).toBeUndefined();
  });

  it("a 401 with a token set calls onUnauthorized; without a token it does not", async () => {
    token = "wrong";
    const r = await make().chaos.killLeader();
    expect(r).toMatchObject({ ok: false, status: 401 });
    expect(unauthorized).toHaveBeenCalledTimes(1);

    token = null;
    unauthorized.mockClear();
    await make(stub(401, { error: "unauthorized" })).chaos.killLeader();
    expect(unauthorized).not.toHaveBeenCalled();
  });
});

describe("error mapping (bodies from internal/gateway)", () => {
  const cases: [string, number, unknown, Record<string, string>, object][] = [
    ["400", 400, { error: "invalid key" }, {}, { status: 400, error: "invalid key" }],
    ["401", 401, { error: "unauthorized" }, {}, { status: 401, error: "unauthorized" }],
    [
      "403",
      403,
      { error: "reserved key prefix" },
      {},
      { status: 403, error: "reserved key prefix" },
    ],
    ["404", 404, { error: "key not found" }, {}, { status: 404, error: "key not found" }],
    ["413", 413, { error: "value is larger than 1024 bytes" }, {}, { status: 413 }],
    ["415", 415, { error: "Content-Type must be application/json" }, {}, { status: 415 }],
    [
      "429",
      429,
      { error: "rate limit exceeded" },
      { "Retry-After": "7" },
      { status: 429, retryAfterSec: 7 },
    ],
    [
      "501",
      501,
      { error: "not implemented", detail: CAS_DETAIL },
      {},
      { status: 501, detail: CAS_DETAIL },
    ],
    [
      "503",
      503,
      { error: "verification in progress" },
      { "Retry-After": "5" },
      { status: 503, retryAfterSec: 5, error: "verification in progress" },
    ],
    [
      "504",
      504,
      { error: "the write may or may not have been applied", outcome: "unknown" },
      {},
      { status: 504, outcome: "unknown" },
    ],
    ["507", 507, { error: "the keyspace is full" }, {}, { status: 507 }],
    [
      "502",
      502,
      { error: "the cluster refused the request", outcome: "not applied" },
      {},
      { status: 502, outcome: "not applied" },
    ],
  ];
  it.each(cases)("%s", async (_n, status, body, headers, want) => {
    const r = await make(stub(status, body, headers)).kvPut("k", "v");
    expect(r.ok).toBe(false);
    expect(r).toMatchObject({ ok: false, ...want });
  });

  it("a non-JSON error body still maps to a failure with the status", async () => {
    const f: typeof fetch = async () => new Response("<html>bad gateway</html>", { status: 502 });
    const r = await make(f).cluster();
    expect(r).toMatchObject({ ok: false, status: 502 });
  });

  it("a 200 with a malformed cluster document is a failure, not data", async () => {
    const r = await make(stub(200, { nodes: "nope" })).cluster();
    expect(r.ok).toBe(false);
  });

  it("a network error is status 0", async () => {
    gw.down = true;
    const r = await make().cluster();
    expect(r).toMatchObject({ ok: false, status: 0 });
  });

  it("times out after 3 s", async () => {
    gw.hang = true;
    const p = make().cluster();
    await vi.advanceTimersByTimeAsync(2999);
    let settled = false;
    p.then(() => {
      settled = true;
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(await p).toMatchObject({
      ok: false,
      status: 0,
      error: expect.stringMatching(/timed out/i),
    });
  });
});
