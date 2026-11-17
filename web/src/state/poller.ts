import type { Client } from "../api/client";
import type { Cluster } from "../api/types";

export interface PollerOptions {
  client: Client;
  onCluster: (c: Cluster) => void;
  /** Three consecutive failures: the caller switches to replay. The poller stops itself. */
  onReplay: () => void;
  isHidden: () => boolean;
  failuresToReplay?: number;
}
export interface Poller {
  start(): void;
  stop(): void;
  refetch(): Promise<void>;
}

const VISIBLE_MS = 1000; // the gateway caches /v1/cluster at 1 Hz, so faster is useless
const HIDDEN_MS = 5000;

/** One setTimeout loop (never setInterval) and never two requests in flight. */
export function createPoller(opts: PollerOptions): Poller {
  const limit = opts.failuresToReplay ?? 3;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = true;
  let closed = false; // stop() was called: late answers are dropped
  let inflight = false;
  let failures = 0;

  const normal = () => (opts.isHidden() ? HIDDEN_MS : VISIBLE_MS);

  /** Fetch once; resolves with the delay before the next poll, or null if the loop must end. */
  async function poll(): Promise<number | null> {
    if (inflight) return normal();
    inflight = true;
    try {
      const r = await opts.client.cluster();
      if (r.ok) {
        failures = 0;
        if (!closed) opts.onCluster(r.data);
        return normal();
      }
      if (r.status === 429) {
        // reachable but throttled: honour Retry-After, never a failure
        return Math.max(1, r.retryAfterSec ?? 1) * 1000;
      }
      failures++;
      if (failures >= limit) return null;
      return normal();
    } finally {
      inflight = false;
    }
  }

  function schedule(ms: number) {
    timer = setTimeout(async () => {
      timer = null;
      const next = await poll();
      if (stopped) return;
      if (next === null) {
        stopped = true;
        opts.onReplay();
        return;
      }
      schedule(next);
    }, ms);
  }

  return {
    start() {
      if (!stopped) return;
      stopped = false;
      closed = false;
      failures = 0;
      schedule(0);
    },
    stop() {
      stopped = true;
      closed = true;
      if (timer !== null) clearTimeout(timer);
      timer = null;
    },
    async refetch() {
      await poll();
    },
  };
}
