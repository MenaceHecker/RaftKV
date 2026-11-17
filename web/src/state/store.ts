import type { Cluster, VerifyLast } from "../api/types";
import type { StreamStatus } from "./eventStream";
import { emptyTimeline, type Timeline } from "./timeline";

export type Mode = "connecting" | "live" | "replay";
export interface AppState {
  mode: Mode;
  replayReason: "none" | "unreachable" | "replay-only" | "no-recording";
  cluster: Cluster | null;
  timeline: Timeline;
  sse: StreamStatus;
  verify: VerifyLast | null;
  recordedAt: string | null;
  synthetic: boolean;
  verifyRunning: boolean; // a run (ours or another visitor's) is in progress
  lastFaultAt: number | null; // browser time of the last fault event or our own fault
}
export interface Store {
  get(): AppState;
  set(fn: (s: AppState) => AppState): void;
  subscribe(fn: () => void): () => void;
}

export function initialState(): AppState {
  return {
    mode: "connecting",
    replayReason: "none",
    cluster: null,
    timeline: emptyTimeline(),
    sse: "off",
    verify: null,
    recordedAt: null,
    synthetic: false,
    verifyRunning: false,
    lastFaultAt: null,
  };
}

// STUB (D7 commit 1): inert store.
export function createStore(initial: AppState = initialState()): Store {
  return { get: () => initial, set() {}, subscribe: () => () => {} };
}
