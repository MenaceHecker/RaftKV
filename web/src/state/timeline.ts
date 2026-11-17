import type { Cluster, EventName } from "../api/types";

export interface TimelineRow {
  key: number;
  at: number; // browser receive time, ms since epoch
  kind: string; // an event name, "note" or "caught_up"
  text: string;
  derived?: boolean;
  node?: number;
}
export interface Timeline {
  rows: TimelineRow[];
  awaiting: number[]; // nodes that came back up and have not caught up yet
  nextKey: number;
}
export const TIMELINE_CAP = 500;

// STUB (D7 commit 1).
export function emptyTimeline(): Timeline {
  return { rows: [], awaiting: [], nextKey: 1 };
}
export function addEvent(t: Timeline, _name: EventName, _data: unknown, _at: number): Timeline {
  return t;
}
export function addNote(t: Timeline, _text: string, _at: number): Timeline {
  return t;
}
export function observeCluster(t: Timeline, _c: Cluster, _at: number): Timeline {
  return t;
}
