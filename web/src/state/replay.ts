import { parseCluster, parseVerifyReport } from "../api/guards";
import { type Cluster, EVENT_NAMES, type EventName, type VerifyReport } from "../api/types";

// public/recording.jsonl, one JSON object per line, v:1:
//   {"v":1,"kind":"header","recordedAt":ISO,"durationMs":N,"synthetic":bool}
//   {"t":ms,"kind":"cluster","data":<cluster doc>}
//   {"t":ms,"kind":"event","id":N,"name":"<sse name>","data":<payload>}
//   {"t":ms,"kind":"verify","data":<report>}
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

type Obj = Record<string, unknown>;
const isObj = (v: unknown): v is Obj => typeof v === "object" && v !== null && !Array.isArray(v);

function parseJson(line: string): unknown {
  try {
    return JSON.parse(line);
  } catch {
    return undefined;
  }
}

/** Null when there is no valid v:1 header (the UI then shows "no recording"). */
export function parseRecording(text: string): Recording | null {
  const rows = text.split("\n").filter((l) => l.trim() !== "");
  const first = rows[0] === undefined ? undefined : parseJson(rows[0]);
  if (
    !isObj(first) ||
    first.v !== 1 ||
    first.kind !== "header" ||
    typeof first.recordedAt !== "string" ||
    typeof first.durationMs !== "number" ||
    typeof first.synthetic !== "boolean"
  ) {
    return null;
  }
  const header: RecHeader = {
    v: 1,
    kind: "header",
    recordedAt: first.recordedAt,
    durationMs: first.durationMs,
    synthetic: first.synthetic,
  };
  const lines: RecLine[] = [];
  let bad = 0;
  for (const row of rows.slice(1)) {
    const o = parseJson(row);
    if (!isObj(o) || typeof o.t !== "number" || typeof o.kind !== "string") {
      bad++;
      continue;
    }
    if (o.kind === "cluster") {
      const data = parseCluster(o.data);
      if (data) lines.push({ t: o.t, kind: "cluster", data });
      else bad++;
    } else if (o.kind === "event") {
      const name = EVENT_NAMES.find((n) => n === o.name);
      if (name && typeof o.id === "number")
        lines.push({ t: o.t, kind: "event", id: o.id, name, data: o.data });
      else if (!name)
        continue; // an event name from a newer gateway: skipped
      else bad++;
    } else if (o.kind === "verify") {
      const data = parseVerifyReport(o.data);
      if (data) lines.push({ t: o.t, kind: "verify", data });
      else bad++;
    }
    // any other kind: skipped silently
  }
  lines.sort((a, b) => a.t - b.t);
  return { header, lines, badLines: bad };
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

/** Plays the recording at 1x and loops; one timer at a time, driven by the (injectable) clock. */
export function createPlayer(opts: PlayerOptions): Player {
  const { lines, header } = opts.recording;
  const lastT = lines.at(-1)?.t ?? 0;
  const total = Math.max(header.durationMs, lastT, 1000);
  let idx = 0;
  let cursor = 0; // recording time already played, ms
  let timer: ReturnType<typeof setTimeout> | null = null;
  let running = false;

  /** Emit every line that is due, then arm one timer for the next one (never a 0 ms timer). */
  function run() {
    while (running) {
      const line = lines[idx];
      const target = line === undefined ? total : line.t; // after the last line, wait out the rest
      const wait = target - cursor;
      if (wait > 0) {
        timer = setTimeout(() => {
          timer = null;
          cursor = target;
          run();
        }, wait);
        return;
      }
      if (line === undefined) {
        opts.onRestart();
        idx = 0;
        cursor = 0;
        continue;
      }
      idx++;
      if (line.kind === "cluster") opts.onCluster(line.data);
      else if (line.kind === "event")
        opts.onEvent({ name: line.name, id: line.id, data: line.data });
      else opts.onVerify(line.data);
    }
  }

  return {
    start() {
      if (running) return;
      running = true;
      idx = 0;
      cursor = 0;
      run();
    },
    stop() {
      running = false;
      if (timer !== null) clearTimeout(timer);
      timer = null;
    },
  };
}
