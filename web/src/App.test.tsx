import { act, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { buildSyntheticRecording } from "../scripts/make-sample-recording.mjs";
import { App } from "./App";
import type { Env } from "./env";
import { setToken } from "./state/admin";
import { createRuntime, type Runtime } from "./state/runtime";
import { FakeEventSource, FakeEventSourceCtor } from "./test/fakeEventSource";
import { cluster, node } from "./test/fixtures";
import { MockGateway } from "./test/mockGateway";

let gw: MockGateway;
let runtime: Runtime;
let hidden: boolean;
let visListeners: (() => void)[];

function boot(env: Partial<Env> = {}, search = "") {
  runtime = createRuntime({
    env: {
      gatewayUrl: "http://gw",
      replayOnly: false,
      repoUrl: "https://example.test/repo",
      ...env,
    },
    fetch: gw.fetch,
    EventSource: FakeEventSourceCtor,
    rand: () => 0,
    search,
    isHidden: () => hidden,
    onVisibility: (fn) => {
      visListeners.push(fn);
      return () => {};
    },
  });
  render(<App runtime={runtime} />);
  return runtime.start();
}
const settle = (ms = 0) => act(() => vi.advanceTimersByTimeAsync(ms));

beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
  gw.recording = buildSyntheticRecording();
  FakeEventSource.reset();
  hidden = false;
  visListeners = [];
});
afterEach(() => {
  runtime?.stop();
  vi.useRealTimers();
});

describe("live mode", () => {
  it("shows the live cluster and a LIVE badge", async () => {
    gw.cluster = cluster({
      leaderId: 2,
      nodes: [node(1), node(2, { role: "leader", term: 9 }), node(3)],
    });
    void boot();
    await settle(10);
    expect(screen.getByText("LIVE")).toBeInTheDocument();
    expect(
      within(screen.getByRole("article", { name: /node 2/i })).getByText(/term 9/),
    ).toBeInTheDocument();
    expect(screen.queryByText("REPLAY")).toBeNull();
  });

  it("opens one event stream and adds timeline rows from events, refetching the cluster once after 250 ms", async () => {
    void boot();
    await settle(10);
    expect(FakeEventSource.live).toHaveLength(1);
    FakeEventSource.live[0]?.open();
    const polls = () => gw.callsTo("/v1/cluster").length;
    const before = polls();
    act(() => {
      FakeEventSource.live[0]?.emit(
        "state",
        { node: 2, term: 5, role: "candidate", leaderId: 0, seq: 1, unixNano: 1 },
        1,
      );
      FakeEventSource.live[0]?.emit(
        "state",
        { node: 2, term: 5, role: "leader", leaderId: 2, seq: 2, unixNano: 2 },
        2,
      );
    });
    expect(screen.getByText(/node 2 became candidate/)).toBeInTheDocument();
    await settle(249);
    expect(polls()).toBe(before);
    await settle(1);
    expect(polls()).toBe(before + 1); // debounced: two events, one refetch
  });

  it("SSE trouble alone keeps the dashboard live and says events are unavailable", async () => {
    void boot();
    await settle(10);
    act(() => FakeEventSource.live[0]?.errorClosed());
    expect(screen.getByText(/live events unavailable/i)).toBeInTheDocument();
    expect(screen.getByText("LIVE")).toBeInTheDocument();
    await settle(5000);
    expect(screen.getByText("LIVE")).toBeInTheDocument();
  });

  it("falls back to replay if the first cluster call does not answer within 3 s", async () => {
    gw.hang = true;
    void boot();
    await settle(3100);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
  });

  it("switches to replay after three failed polls and returns to live when the gateway answers", async () => {
    void boot();
    await settle(10);
    expect(screen.getByText("LIVE")).toBeInTheDocument();
    gw.down = true;
    await settle(3100);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
    expect(screen.getByText(/live gateway unreachable/i)).toBeInTheDocument();
    gw.down = false;
    await settle(31_000);
    expect(screen.getByText("LIVE")).toBeInTheDocument();
    expect(gw.callsTo("/healthz").length).toBeGreaterThan(0);
  });

  it("a 429 during the probe counts as reachable", async () => {
    gw.down = true;
    void boot();
    await settle(3100);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
    gw.down = false;
    gw.rateLimit = { until: Date.now() + 120_000, retryAfter: 9 };
    await settle(31_000);
    expect(screen.getByText("LIVE")).toBeInTheDocument();
  });

  it("a 401 on a chaos call clears the stored admin token", async () => {
    void boot();
    await settle(10);
    setToken("wrong-token");
    const r = await runtime.client.chaos.killLeader();
    expect(r).toMatchObject({ ok: false, status: 401 });
    expect(sessionStorage.length).toBe(0);
  });
});

describe("replay mode", () => {
  it("OFFLINE: with the gateway unreachable and no event stream opening, the page shows REPLAY and a populated cluster", async () => {
    gw.down = true;
    void boot();
    await settle(3100);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
    expect(screen.getByText(/live gateway unreachable/i)).toBeInTheDocument();
    expect(screen.getByText(/synthetic/i)).toBeInTheDocument();
    expect(screen.getAllByRole("article").length).toBe(3);
    const kill = screen.getByRole("button", { name: /kill leader/i });
    expect(kill).toHaveAttribute("aria-disabled", "true");
    expect(screen.getAllByText(/replay is read-only/i).length).toBeGreaterThan(0);
    // not a single request other than the static recording and the failing gateway probes
    expect(gw.calls.filter((c) => c.method !== "GET")).toHaveLength(0);
  });

  it("?replay=1 never touches the gateway", async () => {
    void boot({}, "?replay=1");
    await settle(2000);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
    expect(gw.calls.filter((c) => c.path !== "/recording.jsonl")).toHaveLength(0);
    expect(FakeEventSource.instances).toHaveLength(0);
  });

  it("replay-only builds say 'replay only' and never probe", async () => {
    void boot({ replayOnly: true });
    await settle(120_000);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
    expect(screen.getByText(/replay only/i)).toBeInTheDocument();
    expect(gw.calls.filter((c) => c.path !== "/recording.jsonl")).toHaveLength(0);
  });

  it("no gateway configured means replay only", async () => {
    void boot({ gatewayUrl: "" });
    await settle(1000);
    expect(screen.getByText("REPLAY")).toBeInTheDocument();
    expect(gw.calls.filter((c) => c.path !== "/recording.jsonl")).toHaveLength(0);
  });

  it("says 'no recording' when the file is missing", async () => {
    gw.recording = null;
    void boot({ replayOnly: true });
    await settle(1000);
    expect(screen.getByText(/no recording/i)).toBeInTheDocument();
  });

  it("the timeline keeps playing recorded events and notes the restart when it loops", async () => {
    void boot({ replayOnly: true });
    await settle(1000);
    expect(screen.getByRole("log").textContent).not.toBe("");
    await settle(70_000);
    expect(screen.getByText(/recording restarted/i)).toBeInTheDocument();
  });
});
