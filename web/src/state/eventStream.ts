import { EVENT_NAMES, type EventName } from "../api/types";
import { reconnectDelay } from "./backoff";

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
  /** A source was recreated (or reopened): events in between may be missing. */
  onGap: () => void;
  onStatus: (s: StreamStatus) => void;
}

export interface EventStream {
  start(): void;
  stop(): void;
  setVisible(visible: boolean): void;
  badJsonCount(): number;
}

const CLOSED = 2;
const ERRORS_BEFORE_RECREATE = 3;
const HIDDEN_CLOSE_MS = 60_000;

/**
 * One native EventSource per tab. The browser's own reconnect (readyState CONNECTING) already
 * sends Last-Event-ID, so this wrapper leaves it alone. It steps in only when the source is dead
 * (CLOSED: an HTTP 429/503 cap or a CORS failure, whose status a browser cannot read) or keeps
 * failing without ever opening; then it recreates the source with jittered backoff. A recreated
 * source has no Last-Event-ID, so that is reported as a gap.
 */
export function createEventStream(opts: StreamOptions): EventStream {
  let es: EventSourceLike | null = null;
  let started = false;
  let visible = true;
  let closedWhileHidden = false;
  let errors = 0;
  let attempt = 0;
  let lastId = 0;
  let bad = 0;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let hideTimer: ReturnType<typeof setTimeout> | null = null;

  const clearReconnect = () => {
    if (reconnectTimer !== null) clearTimeout(reconnectTimer);
    reconnectTimer = null;
  };
  const clearHide = () => {
    if (hideTimer !== null) clearTimeout(hideTimer);
    hideTimer = null;
  };
  const closeSource = () => {
    if (es) {
      es.onopen = null;
      es.onerror = null;
      es.close();
    }
    es = null;
  };

  function open() {
    closeSource();
    clearReconnect();
    errors = 0;
    lastId = 0; // a new source is a cold connect: the hub's ids are fresh for it
    opts.onStatus("connecting");
    const src = new opts.EventSource(opts.url);
    es = src;
    src.onopen = () => {
      errors = 0;
      attempt = 0;
      opts.onStatus("open");
    };
    src.onerror = () => {
      errors++;
      if (src.readyState === CLOSED || errors >= ERRORS_BEFORE_RECREATE) {
        closeSource();
        opts.onStatus("unavailable");
        scheduleRecreate();
      }
    };
    for (const name of EVENT_NAMES) {
      src.addEventListener(name, (e) => {
        let data: unknown;
        try {
          data = JSON.parse(e.data);
        } catch {
          bad++;
          return;
        }
        let id: number | null = null;
        if (e.lastEventId) {
          id = Number(e.lastEventId);
          if (!Number.isFinite(id)) id = null;
        }
        if (id !== null) {
          if (id <= lastId) return;
          lastId = id;
        }
        opts.onEvent({ name, id, data });
      });
    }
  }

  function scheduleRecreate() {
    clearReconnect();
    const wait = reconnectDelay(attempt++, opts.rand);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (!started) return;
      open();
      opts.onGap();
    }, wait);
  }

  return {
    start() {
      if (started) return;
      started = true;
      open();
    },
    stop() {
      started = false;
      clearReconnect();
      clearHide();
      closeSource();
      opts.onStatus("off");
    },
    setVisible(v) {
      if (v === visible) return;
      visible = v;
      if (!started) return;
      if (!v) {
        clearHide();
        hideTimer = setTimeout(() => {
          hideTimer = null;
          clearReconnect();
          closeSource();
          closedWhileHidden = true;
          opts.onStatus("off");
        }, HIDDEN_CLOSE_MS);
        return;
      }
      clearHide();
      if (closedWhileHidden) {
        closedWhileHidden = false;
        open();
        opts.onGap();
      }
    },
    badJsonCount: () => bad,
  };
}
