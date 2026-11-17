import type { Client } from "../api/client";
import type { Env } from "../env";
import type { EventSourceCtor } from "./eventStream";
import type { Store } from "./store";
import { createStore } from "./store";

export interface RuntimeDeps {
  env: Env;
  fetch: typeof fetch;
  EventSource: EventSourceCtor;
  rand: () => number;
  search: string; // location.search, for ?replay=1
  isHidden: () => boolean;
  onVisibility: (fn: () => void) => () => void;
}
export interface Runtime {
  store: Store;
  client: Client;
  start(): Promise<void>;
  stop(): void;
}

// STUB (D7 commit 1).
export function createRuntime(_deps: RuntimeDeps): Runtime {
  const store = createStore();
  const todo = async () => ({ ok: false as const, status: 0, error: "todo" });
  // biome-ignore lint/suspicious/noExplicitAny: stub
  const client = { chaos: {} } as any as Client;
  void todo;
  return { store, client, async start() {}, stop() {} };
}
