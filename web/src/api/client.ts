import type {
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
  onUnauthorized?: () => void;
  timeoutMs?: number;
}

export interface Client {
  health(): Promise<ApiResult<{ status: string }>>;
  cluster(): Promise<ApiResult<Cluster>>;
  kvGet(key: string): Promise<ApiResult<KvValue>>;
  kvPut(key: string, value: string): Promise<ApiResult<KvKey>>;
  kvDelete(key: string): Promise<ApiResult<KvKey>>;
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

// STUB (D7 commit 1): every call fails; the real client arrives in the client commit.
export function createClient(_opts: ClientOptions): Client {
  const todo = async (): Promise<ApiResult<never>> => ({ ok: false, status: 0, error: "todo" });
  return {
    health: todo,
    cluster: todo,
    kvGet: todo,
    kvPut: todo,
    kvDelete: todo,
    kvCas: todo,
    chaos: {
      killLeader: todo,
      kill: todo,
      pause: todo,
      partition: todo,
      delay: todo,
      heal: todo,
    },
    verifyRun: todo,
    verifyLast: todo,
  };
}
