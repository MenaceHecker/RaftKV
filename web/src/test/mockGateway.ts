import type { ChaosView, Cluster, VerifyReport } from "../api/types";
import { CAS_DETAIL } from "../api/types";
import { cluster as makeCluster, report as makeReport } from "./fixtures";

/**
 * A Response look-alike whose body resolves in microtasks only. A real Response reads its body
 * through streams, which need real event-loop turns that fake timers do not provide.
 */
export function fakeRes(
  status: number,
  text: string,
  headers: Record<string, string> = {},
): Response {
  const h = new Headers(headers);
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: h,
    json: async () => JSON.parse(text),
    text: async () => text,
  } as unknown as Response;
}

export const ADMIN_TOKEN = "test-admin-token-not-a-secret";
const PUBLIC_BUCKET_MS = 20_000; // chaos.go: one fault per 20 s
const COOLDOWN_MS = 15 * 60_000; // verify.go DefaultCooldown
const VERIFY_MS = 60_000;
const AUTO_HEAL_MS = 45_000;

export interface Call {
  method: string;
  path: string;
  headers: Record<string, string>;
  body: string | null;
}

type Sink = (chunk: string) => void;

/**
 * An in-process gateway: a `fetch` that answers with the exact status codes, bodies and
 * Retry-After values read from internal/gateway, driven by the (fake) clock. No sockets.
 */
export class MockGateway {
  cluster: Cluster = makeCluster();
  calls: Call[] = [];
  down = false; // network error on every request
  hang = false; // never answers (until aborted)
  rateLimit: { until: number; retryAfter: number } | null = null; // 429 on everything
  writeOutcome: "ok" | "unknown" | "not applied" | "no leader" = "ok";
  fail5xx = false;
  recording: string | null = null; // served at /recording.jsonl even when `down` (same-origin static file)
  kv = new Map<string, string>();
  private lastFaultAt: number | null = null;
  private lastVerifyEnd: number | null = null;
  private running: { id: string; startedAt: number } | null = null;
  private lastReport: VerifyReport | null = null;
  private eventSinks = new Set<Sink>();
  private nextEventId = 1;

  fetch: typeof fetch = async (input, init) => {
    const url = new URL(typeof input === "string" ? input : (input as Request).url, "http://gw");
    const method = (init?.method ?? "GET").toUpperCase();
    const headers: Record<string, string> = {};
    new Headers(init?.headers).forEach((v, k) => {
      headers[k.toLowerCase()] = v;
    });
    const body = typeof init?.body === "string" ? init.body : null;
    this.calls.push({ method, path: url.pathname + url.search, headers, body });
    if (url.pathname === "/recording.jsonl") {
      return this.recording === null ? fakeRes(404, "not found") : fakeRes(200, this.recording);
    }
    if (this.down) throw new TypeError("Failed to fetch");
    if (this.hang) {
      return new Promise<Response>((_, reject) => {
        init?.signal?.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError")),
        );
      });
    }
    return this.route(url.pathname, method, headers, body, init?.signal ?? null);
  };

  private json(status: number, body: unknown, extra: Record<string, string> = {}): Response {
    return fakeRes(status, JSON.stringify(body), { "Content-Type": "application/json", ...extra });
  }
  private err(
    status: number,
    error: string,
    extra: Record<string, unknown> = {},
    h = {},
  ): Response {
    return this.json(status, { error, ...extra }, h);
  }

  private authed(headers: Record<string, string>): "none" | "admin" | "bad" {
    const a = headers.authorization;
    if (!a) return "none";
    return a === `Bearer ${ADMIN_TOKEN}` ? "admin" : "bad";
  }

  private now(): number {
    return Date.now();
  }

  private route(
    path: string,
    method: string,
    headers: Record<string, string>,
    body: string | null,
    signal: AbortSignal | null,
  ): Response {
    if (path === "/healthz") return this.json(200, { status: "ok" });
    if (this.rateLimit && this.now() < this.rateLimit.until) {
      return this.err(
        429,
        "rate limit exceeded",
        {},
        { "Retry-After": String(this.rateLimit.retryAfter) },
      );
    }
    if (this.fail5xx) return this.err(502, "cluster error");
    this.tick();
    if (path === "/v1/cluster") return this.json(200, this.cluster);
    if (path === "/v1/events") return this.events(signal);
    if (path.startsWith("/v1/kv/")) return this.kvRoute(path, method, headers, body);
    if (path.startsWith("/v1/chaos/")) return this.chaosRoute(path, method, headers, body);
    if (path === "/v1/verify/run") return this.verifyRun(method, headers);
    if (path === "/v1/verify/last") return this.verifyLast();
    return this.err(404, "not found");
  }

  /** Advance auto-heal and the verify run with the clock. */
  private tick(): void {
    const c = this.cluster.chaos;
    if (c.state === "active" && c.expiresAt && this.now() >= Date.parse(c.expiresAt)) {
      this.cluster = { ...this.cluster, chaos: { state: "idle" } };
    }
    if (this.running && this.now() - this.running.startedAt >= VERIFY_MS) {
      this.finishVerify();
    }
  }

  private kvRoute(
    path: string,
    method: string,
    headers: Record<string, string>,
    body: string | null,
  ): Response {
    const rest = path.slice("/v1/kv/".length);
    if (rest.endsWith("/cas")) {
      return this.err(501, "not implemented", { detail: CAS_DETAIL });
    }
    const key = decodeURIComponent(rest);
    if (key === "" || new TextEncoder().encode(key).length > 64 || key.includes("/")) {
      return this.err(400, "invalid key");
    }
    if (key.startsWith("__verify/")) return this.err(403, "reserved key prefix");
    if (method === "GET") {
      const v = this.kv.get(key);
      return v === undefined ? this.err(404, "key not found") : this.json(200, { key, value: v });
    }
    if (method !== "PUT" && method !== "DELETE") return this.err(405, "method not allowed");
    if (this.running) return this.err(503, "verification in progress", {}, { "Retry-After": "5" });
    if (method === "PUT") {
      if (!(headers["content-type"] ?? "").startsWith("application/json")) {
        return this.err(415, "Content-Type must be application/json");
      }
      let value: unknown;
      try {
        value = (JSON.parse(body ?? "") as { value?: unknown }).value;
      } catch {
        return this.err(400, 'body must be {"value": "<string>"}');
      }
      if (typeof value !== "string") return this.err(400, 'body must be {"value": "<string>"}');
      if (new TextEncoder().encode(value).length > 1024)
        return this.err(413, "value is larger than 1024 bytes");
      if (this.kv.size >= 10_000 && !this.kv.has(key)) return this.err(507, "the keyspace is full");
      const bad = this.writeFailure();
      if (bad) return bad;
      this.kv.set(key, value);
      return this.json(200, { key });
    }
    const bad = this.writeFailure();
    if (bad) return bad;
    this.kv.delete(key);
    return this.json(200, { key });
  }

  private writeFailure(): Response | null {
    switch (this.writeOutcome) {
      case "unknown":
        return this.err(504, "the write may or may not have been applied", { outcome: "unknown" });
      case "not applied":
        return this.err(502, "the cluster refused the request", { outcome: "not applied" });
      case "no leader":
        return this.err(503, "no leader available, try again", {}, { "Retry-After": "1" });
      default:
        return null;
    }
  }

  private chaosRoute(
    path: string,
    method: string,
    headers: Record<string, string>,
    body: string | null,
  ): Response {
    if (method !== "POST") return this.err(405, "method not allowed");
    const who = this.authed(headers);
    if (who === "bad") return this.err(401, "unauthorized");
    const action = path.slice("/v1/chaos/".length);
    if (action === "heal") {
      const was = this.cluster.chaos.state;
      this.cluster = { ...this.cluster, chaos: { state: "idle" } };
      void was;
      return this.json(200, { healed: [1, 2, 3], failed: [], state: "idle" });
    }
    let req: { seconds?: number; delayMs?: number; dropPercent?: number; node?: number } = {};
    if (body) {
      try {
        req = JSON.parse(body);
      } catch {
        return this.err(400, "invalid JSON");
      }
    }
    if (this.running) return this.err(409, "a verification run is in progress");
    const state = this.cluster.chaos.state;
    if (state !== "idle") {
      return this.err(
        409,
        state === "active" ? "a fault is already active" : "the cluster is still recovering",
      );
    }
    const leader = this.cluster.leaderId;
    const m = /^(kill|pause)\/(\d+)$/.exec(action);
    if (m?.[1] === "kill" && Number(m[2]) !== leader && who !== "admin") {
      return this.err(401, "unauthorized");
    }
    if (m?.[1] === "pause" && req.seconds !== undefined && (req.seconds < 1 || req.seconds > 45)) {
      return this.err(400, "seconds must be 1 to 45");
    }
    if (action === "delay" && ((req.delayMs ?? 0) > 500 || (req.dropPercent ?? 0) > 50)) {
      return this.err(400, "delay at most 500 ms, drop at most 50 percent");
    }
    if (who !== "admin" && this.lastFaultAt !== null) {
      const wait = this.lastFaultAt + PUBLIC_BUCKET_MS - this.now();
      if (wait > 0) {
        return this.err(
          429,
          "chaos rate limit",
          {},
          { "Retry-After": String(Math.ceil(wait / 1000)) },
        );
      }
    }
    if (this.cluster.nodes.some((n) => !n.up || n.paused) || leader === 0) {
      return this.err(503, "cluster is not healthy enough for a fault", {}, { "Retry-After": "5" });
    }
    const target = m ? Number(m[2]) : (req.node ?? leader);
    const act = m?.[1] ?? action;
    const expires = new Date(this.now() + AUTO_HEAL_MS).toISOString();
    const chaos: ChaosView = { state: "active", action: act, target, expiresAt: expires };
    this.cluster = { ...this.cluster, chaos };
    this.lastFaultAt = this.now();
    return this.json(200, { action: act, target, expiresAt: expires, autoHealSeconds: 45 });
  }

  private verifyRun(method: string, headers: Record<string, string>): Response {
    if (method !== "POST") return this.err(405, "method not allowed");
    const who = this.authed(headers);
    if (who === "bad") return this.err(401, "unauthorized");
    if (this.running) {
      return this.err(409, "a verification run is already going", { id: this.running.id });
    }
    if (who !== "admin" && this.lastVerifyEnd !== null) {
      const wait = this.lastVerifyEnd + COOLDOWN_MS - this.now();
      if (wait > 0) {
        return this.err(
          429,
          "a verification run was done recently",
          {},
          { "Retry-After": String(Math.ceil(wait / 1000)) },
        );
      }
    }
    const id = `v${this.now().toString(16)}`;
    this.running = { id, startedAt: this.now() };
    this.publish("verify", { phase: "start", id });
    return this.json(202, { id, seed: "42", status: "running" });
  }

  private verifyLast(): Response {
    if (this.running) {
      return this.json(200, {
        id: this.running.id,
        status: "running",
        startedAt: new Date(this.running.startedAt).toISOString(),
      });
    }
    if (!this.lastReport) return this.err(404, "no verification run yet");
    return this.json(200, this.lastReport);
  }

  /** Test hook: end the running verification now with this report. */
  finishVerify(over: Partial<VerifyReport> = {}): void {
    const id = this.running?.id ?? "vmanual";
    this.running = null;
    this.lastVerifyEnd = this.now();
    this.lastReport = makeReport({ id, ...over });
    this.publish("verify", {
      phase: "done",
      id,
      verdict: this.lastReport.verdict,
      ops: this.lastReport.ops,
    });
  }
  get verifyIsRunning(): boolean {
    return this.running !== null;
  }
  /** Test hook: pretend a report exists without a run. */
  setLastReport(r: VerifyReport | null): void {
    this.lastReport = r;
  }

  // --- SSE as a fetch stream (used by the recorder) ---
  private events(signal: AbortSignal | null): Response {
    const enc = new TextEncoder();
    let sink: Sink | null = null;
    const stream = new ReadableStream<Uint8Array>({
      start: (ctrl) => {
        ctrl.enqueue(enc.encode(": connected\n\n"));
        sink = (chunk) => {
          try {
            ctrl.enqueue(enc.encode(chunk));
          } catch {
            // closed
          }
        };
        this.eventSinks.add(sink);
        signal?.addEventListener("abort", () => {
          if (sink) this.eventSinks.delete(sink);
          try {
            ctrl.close();
          } catch {
            // already closed
          }
        });
      },
      cancel: () => {
        if (sink) this.eventSinks.delete(sink);
      },
    });
    return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
  }
  publish(name: string, data: unknown): number {
    const id = this.nextEventId++;
    for (const s of this.eventSinks)
      s(`id: ${id}\nevent: ${name}\ndata: ${JSON.stringify(data)}\n\n`);
    return id;
  }
  get eventClients(): number {
    return this.eventSinks.size;
  }

  callsTo(pathPrefix: string): Call[] {
    return this.calls.filter((c) => c.path.startsWith(pathPrefix));
  }
}
