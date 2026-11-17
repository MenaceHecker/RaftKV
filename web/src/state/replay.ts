import type { Cluster, EventName, VerifyReport } from "../api/types";

export interface RecHeader {
  v: 1;
  kind: "header";
  recordedAt: string;
  durationMs: number;
  synthetic: boolean;
}
export type RecLine =
  | { t: number; kind: "cluster"; data: Cluster }
  | { t: number; kind: "event"; id: number; name: EventName; data: unknown }
  | { t: number; kind: "verify"; data: VerifyReport };
export interface Recording {
  header: RecHeader;
  lines: RecLine[];
  badLines: number;
}

export interface PlayerOptions {
  recording: Recording;
  onCluster: (c: Cluster) => void;
  onEvent: (e: { name: EventName; id: number | null; data: unknown }) => void;
  onVerify: (r: VerifyReport) => void;
  onRestart: () => void;
}
export interface Player {
  start(): void;
  stop(): void;
}

// STUB (D7 commit 1).
export function parseRecording(_text: string): Recording | null {
  return null;
}
export function createPlayer(_opts: PlayerOptions): Player {
  return { start() {}, stop() {} };
}
