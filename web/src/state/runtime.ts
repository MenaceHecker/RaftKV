import { type Client, createClient } from "../api/client";
import { parseCluster } from "../api/guards";
import type { Cluster, EventName } from "../api/types";
import type { Env } from "../env";
import { clearToken, getToken } from "./admin";
import { createEventStream, type EventSourceCtor } from "./eventStream";
import { createPoller } from "./poller";
import { createPlayer, type Player, parseRecording } from "./replay";
import { type AppState, createStore, type Store } from "./store";
import { addEvent, addNote, emptyTimeline, observeCluster } from "./timeline";

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

const REFETCH_DEBOUNCE_MS = 250;
const PROBE_MS = 30_000;
const REFETCH_EVENTS: readonly EventName[] = ["state", "node_up", "node_down", "chaos", "resync"];

/** Wires the client, poller, event stream, replay player and mode switching to one store. */
export function createRuntime(deps: RuntimeDeps): Runtime {
  const { env } = deps;
  const store = createStore();
  const client = createClient({
    baseUrl: env.gatewayUrl,
    fetch: deps.fetch,
    getToken,
    onUnauthorized: clearToken, // the gateway refused our token: drop it
  });

  const wantsLive =
    env.gatewayUrl !== "" &&
    !env.replayOnly &&
    new URLSearchParams(deps.search).get("replay") !== "1";

  let stopped = false;
  let refetchTimer: ReturnType<typeof setTimeout> | null = null;
  let probeTimer: ReturnType<typeof setTimeout> | null = null;
  let player: Player | null = null;
  let unVisibility: (() => void) | null = null;

  const update = (fn: (s: AppState) => AppState) => store.set(fn);

  function applyCluster(c: Cluster) {
    update((s) => ({
      ...s,
      cluster: c,
      timeline: observeCluster(s.timeline, c, Date.now()),
    }));
  }

  function applyEvent(e: { name: EventName; data: unknown }) {
    const at = Date.now();
    update((s) => {
      let next: AppState = { ...s, timeline: addEvent(s.timeline, e.name, e.data, at) };
      if (e.name === "resync") {
        const c = parseCluster(e.data);
        if (c) next = { ...next, cluster: c, timeline: observeCluster(next.timeline, c, at) };
      }
      if (e.name === "chaos" && (e.data as { kind?: unknown } | null)?.kind === "fault") {
        next = { ...next, lastFaultAt: at };
      }
      if (e.name === "verify") {
        const phase = (e.data as { phase?: unknown } | null)?.phase;
        if (phase === "start") next = { ...next, verifyRunning: true };
        if (phase === "done") next = { ...next, verifyRunning: false };
      }
      return next;
    });
  }

  const poller = createPoller({
    client,
    onCluster: applyCluster,
    onReplay: () => void enterReplay("unreachable"),
    isHidden: deps.isHidden,
  });

  function scheduleRefetch() {
    if (refetchTimer !== null) return;
    refetchTimer = setTimeout(() => {
      refetchTimer = null;
      void poller.refetch();
    }, REFETCH_DEBOUNCE_MS);
  }

  const stream = createEventStream({
    url: `${env.gatewayUrl}/v1/events`,
    EventSource: deps.EventSource,
    rand: deps.rand,
    onEvent: (e) => {
      applyEvent(e);
      if (REFETCH_EVENTS.includes(e.name)) scheduleRefetch();
    },
    onGap: () => {
      update((s) => ({ ...s, timeline: addNote(s.timeline, "events may be missing", Date.now()) }));
      scheduleRefetch();
    },
    onStatus: (sse) => update((s) => ({ ...s, sse })),
  });

  function stopLive() {
    poller.stop();
    stream.stop();
    if (refetchTimer !== null) clearTimeout(refetchTimer);
    refetchTimer = null;
    unVisibility?.();
    unVisibility = null;
  }
  function stopReplay() {
    player?.stop();
    player = null;
    if (probeTimer !== null) clearTimeout(probeTimer);
    probeTimer = null;
  }

  function goLive(first: Cluster | null) {
    stopReplay();
    update((s) => ({
      ...s,
      mode: "live",
      replayReason: "none",
      cluster: first,
      timeline: s.mode === "replay" ? emptyTimeline() : s.timeline,
      verify: null,
      verifyRunning: false,
      lastFaultAt: null,
    }));
    if (first) applyCluster(first);
    poller.start(first ? 1000 : 0);
    stream.start();
    unVisibility = deps.onVisibility(() => {
      stream.setVisible(!deps.isHidden());
      if (!deps.isHidden()) void poller.refetch();
    });
  }

  async function loadRecording() {
    try {
      const res = await deps.fetch("/recording.jsonl", { cache: "no-store" });
      if (!res.ok) return null;
      return parseRecording(await res.text());
    } catch {
      return null;
    }
  }

  async function enterReplay(reason: "unreachable" | "replay-only") {
    if (stopped) return;
    stopLive();
    stopReplay();
    update((s) => ({
      ...s,
      mode: "replay",
      replayReason: reason,
      cluster: null,
      timeline: emptyTimeline(),
      sse: "off",
      verify: null,
      verifyRunning: false,
    }));
    const rec = await loadRecording();
    if (stopped) return;
    if (!rec) {
      update((s) => ({ ...s, replayReason: "no-recording" }));
    } else {
      update((s) => ({ ...s, recordedAt: rec.header.recordedAt, synthetic: rec.header.synthetic }));
      player = createPlayer({
        recording: rec,
        onCluster: applyCluster,
        onEvent: applyEvent,
        onVerify: (r) => update((s) => ({ ...s, verify: r })),
        onRestart: () =>
          update((s) => ({
            ...s,
            timeline: addNote(s.timeline, "recording restarted", Date.now()),
          })),
      });
      player.start();
    }
    if (wantsLive) scheduleProbe();
  }

  /** In replay with a gateway configured: probe /healthz then /v1/cluster, jittered. */
  function scheduleProbe() {
    const wait = Math.round(PROBE_MS * (1 + 0.2 * deps.rand()));
    probeTimer = setTimeout(async () => {
      probeTimer = null;
      if (stopped) return;
      const h = await client.health();
      if (h.ok || h.status === 429) {
        const c = await client.cluster();
        // a 429 means reachable, not down
        if (c.ok) return goLive(c.data);
        if (c.status === 429) return goLive(null);
      }
      scheduleProbe();
    }, wait);
  }

  return {
    store,
    client,
    async start() {
      stopped = false;
      if (!wantsLive) {
        await enterReplay("replay-only");
        return;
      }
      const r = await client.cluster(); // must answer within 3 s (client timeout) or we replay
      if (stopped) return;
      if (r.ok) goLive(r.data);
      else if (r.status === 429) goLive(null);
      else await enterReplay("unreachable");
    },
    stop() {
      stopped = true;
      stopLive();
      stopReplay();
    },
  };
}
