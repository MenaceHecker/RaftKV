import {
  parseChaosAck,
  parseCluster,
  parseHeal,
  parseHealth,
  parseKeyed,
  parseKvValue,
  parseRunAck,
  parseVerifyLast,
} from "./guards";
import type {
  ApiFailure,
  ApiResult,
  ChaosAck,
  Cluster,
  HealResult,
  KvKey,
  KvValue,
  VerifyLast,
  VerifyRunAck,
} from "./types";

export interface ClientOptions {
  baseUrl: string;
  fetch: typeof fetch;
  getToken: () => string | null;
  /** Called when the gateway answers 401 while a token was sent: the caller clears it. */
  onUnauthorized?: () => void;
  timeoutMs?: number;
}

export interface Client {
  health(): Promise<ApiResult<{ status: string }>>;
  cluster(): Promise<ApiResult<Cluster>>;
  kvGet(key: string): Promise<ApiResult<KvValue>>;
  kvPut(key: string, value: string): Promise<ApiResult<KvKey>>;
  kvDelete(key: string): Promise<ApiResult<KvKey>>;
  /** Typed, never called by the UI: the gateway answers 501 (kv.go handleCAS). */
  kvCas(key: string, body: unknown): Promise<ApiResult<never>>;
  chaos: {
    killLeader(): Promise<ApiResult<ChaosAck>>;
    kill(id: number): Promise<ApiResult<ChaosAck>>;
    pause(id: number, seconds?: number): Promise<ApiResult<ChaosAck>>;
    partition(node?: number): Promise<ApiResult<ChaosAck>>;
    delay(delayMs: number, dropPercent: number, node?: number): Promise<ApiResult<ChaosAck>>;
    heal(): Promise<ApiResult<HealResult>>;
  };
  verifyRun(): Promise<ApiResult<VerifyRunAck>>;
  verifyLast(): Promise<ApiResult<VerifyLast>>;
}

const DEFAULT_TIMEOUT_MS = 3000;

interface Req {
  method: string;
  path: string;
  body?: unknown;
  /** Only chaos and verify/run carry the admin token. */
  auth?: boolean;
}

export function createClient(opts: ClientOptions): Client {
  const base = opts.baseUrl.replace(/\/+$/, "");
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  async function call<T>(req: Req, parse: (v: unknown) => T | null): Promise<ApiResult<T>> {
    const headers: Record<string, string> = {};
    let body: string | undefined;
    if (req.body !== undefined) {
      headers["Content-Type"] = "application/json";
      body = JSON.stringify(req.body);
    }
    const token = req.auth ? opts.getToken() : null;
    if (token) headers.Authorization = `Bearer ${token}`;

    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), timeoutMs);
    let res: Response;
    try {
      res = await opts.fetch(`${base}${req.path}`, {
        method: req.method,
        headers,
        body,
        signal: ctl.signal,
        credentials: "omit",
        cache: "no-store",
      });
    } catch (e) {
      const aborted = e instanceof DOMException && e.name === "AbortError";
      return { ok: false, status: 0, error: aborted ? "request timed out" : "network error" };
    } finally {
      clearTimeout(timer);
    }

    let json: unknown;
    try {
      json = await res.json();
    } catch {
      json = undefined;
    }

    if (res.ok) {
      const data = parse(json);
      if (data === null) {
        return { ok: false, status: res.status, error: "unexpected response from the gateway" };
      }
      return { ok: true, data };
    }

    const fail: ApiFailure = { ok: false, status: res.status, error: `HTTP ${res.status}` };
    if (typeof json === "object" && json !== null) {
      const o = json as Record<string, unknown>;
      if (typeof o.error === "string") fail.error = o.error;
      if (typeof o.outcome === "string") fail.outcome = o.outcome;
      if (typeof o.detail === "string") fail.detail = o.detail;
    }
    const ra = Number(res.headers.get("Retry-After"));
    if (Number.isFinite(ra) && ra > 0) fail.retryAfterSec = ra;
    if (res.status === 401 && token) opts.onUnauthorized?.();
    return fail;
  }

  const kvPath = (key: string) => `/v1/kv/${encodeURIComponent(key)}`;
  const post = (path: string, body?: unknown): Req => ({ method: "POST", path, body, auth: true });

  return {
    health: () => call({ method: "GET", path: "/healthz" }, parseHealth),
    cluster: () => call({ method: "GET", path: "/v1/cluster" }, parseCluster),
    kvGet: (key) => call({ method: "GET", path: kvPath(key) }, parseKvValue),
    kvPut: (key, value) => call({ method: "PUT", path: kvPath(key), body: { value } }, parseKeyed),
    kvDelete: (key) => call({ method: "DELETE", path: kvPath(key) }, parseKeyed),
    kvCas: (key, body) =>
      call({ method: "POST", path: `${kvPath(key)}/cas`, body }, () => null) as Promise<
        ApiResult<never>
      >,
    chaos: {
      killLeader: () => call(post("/v1/chaos/kill-leader"), parseChaosAck),
      kill: (id) => call(post(`/v1/chaos/kill/${id}`), parseChaosAck),
      pause: (id, seconds) =>
        call(
          post(`/v1/chaos/pause/${id}`, seconds === undefined ? undefined : { seconds }),
          parseChaosAck,
        ),
      partition: (node) =>
        call(post("/v1/chaos/partition", node === undefined ? undefined : { node }), parseChaosAck),
      delay: (delayMs, dropPercent, node) =>
        call(
          post("/v1/chaos/delay", {
            delayMs,
            dropPercent,
            ...(node === undefined ? {} : { node }),
          }),
          parseChaosAck,
        ),
      heal: () => call(post("/v1/chaos/heal"), parseHeal),
    },
    verifyRun: () => call(post("/v1/verify/run"), parseRunAck),
    verifyLast: () => call({ method: "GET", path: "/v1/verify/last" }, parseVerifyLast),
  };
}
