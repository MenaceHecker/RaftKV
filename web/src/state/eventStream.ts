import type { EventName } from "../api/types";

export interface EventSourceLike {
  readyState: number;
  onopen: ((e: Event) => void) | null;
  onerror: ((e: Event) => void) | null;
  addEventListener(name: string, fn: (e: MessageEvent) => void): void;
  close(): void;
}
export type EventSourceCtor = new (url: string) => EventSourceLike;

export type StreamStatus = "connecting" | "open" | "unavailable" | "off";

export interface StreamOptions {
  url: string;
  EventSource: EventSourceCtor;
  rand: () => number;
  onEvent: (e: { name: EventName; id: number | null; data: unknown }) => void;
  onGap: () => void;
  onStatus: (s: StreamStatus) => void;
}

export interface EventStream {
  start(): void;
  stop(): void;
  setVisible(visible: boolean): void;
  badJsonCount(): number;
}

// STUB (D7 commit 1): inert; the real wrapper arrives in the state commit.
export function createEventStream(_opts: StreamOptions): EventStream {
  return { start() {}, stop() {}, setVisible() {}, badJsonCount: () => 0 };
}
