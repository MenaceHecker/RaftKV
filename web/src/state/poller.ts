import type { Client } from "../api/client";
import type { Cluster } from "../api/types";

export interface PollerOptions {
  client: Client;
  onCluster: (c: Cluster) => void;
  onReplay: () => void;
  isHidden: () => boolean;
  failuresToReplay?: number;
}
export interface Poller {
  start(): void;
  stop(): void;
  refetch(): Promise<void>;
}

// STUB (D7 commit 1).
export function createPoller(_opts: PollerOptions): Poller {
  return { start() {}, stop() {}, refetch: async () => {} };
}
