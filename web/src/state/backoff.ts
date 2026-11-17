const FLOOR_MS = 5000; // the gateway's Retry-After on a viewer cap (events.go)
const CAP_MS = 60_000;

/**
 * Delay before recreating the event stream: 5 s doubling to 60 s, plus up to 25 % jitter
 * (never below the 5 s floor, never above the cap). `attempt` counts from 0.
 */
export function reconnectDelay(attempt: number, rand: () => number): number {
  const base = Math.min(CAP_MS, FLOOR_MS * 2 ** Math.min(attempt, 10));
  return Math.min(CAP_MS, Math.round(base * (1 + 0.25 * rand())));
}
