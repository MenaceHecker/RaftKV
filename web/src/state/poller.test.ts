import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../api/client";
import { cluster } from "../test/fixtures";
import { MockGateway } from "../test/mockGateway";
import { createPoller } from "./poller";

let gw: MockGateway;
let hidden: boolean;
let got: number;
let replay: number;

function make() {
  const client = createClient({ baseUrl: "http://gw", fetch: gw.fetch, getToken: () => null });
  return createPoller({
    client,
    onCluster: () => {
      got++;
    },
    onReplay: () => {
      replay++;
    },
    isHidden: () => hidden,
  });
}
const polls = () => gw.callsTo("/v1/cluster").length;

beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
  hidden = false;
  got = 0;
  replay = 0;
});
afterEach(() => vi.useRealTimers());

describe("poller", () => {
  it("polls at once and then every second while visible", async () => {
    const p = make();
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(polls()).toBe(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(polls()).toBe(2);
    await vi.advanceTimersByTimeAsync(3000);
    expect(polls()).toBe(5);
    expect(got).toBe(5);
    p.stop();
  });

  it("polls every 5 s while the page is hidden", async () => {
    hidden = true;
    const p = make();
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(4999);
    expect(polls()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(polls()).toBe(2);
    p.stop();
  });

  it("stop ends the loop", async () => {
    const p = make();
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    p.stop();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(polls()).toBe(1);
  });

  it("honours Retry-After on a 429 (minimum 1 s) and does not count it as a failure", async () => {
    const p = make();
    gw.rateLimit = { until: Date.now() + 30_000, retryAfter: 7 };
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(polls()).toBe(1);
    await vi.advanceTimersByTimeAsync(6999);
    expect(polls()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(polls()).toBe(2);
    // never switches to replay however long the gateway throttles
    await vi.advanceTimersByTimeAsync(20_000);
    expect(replay).toBe(0);
    p.stop();
  });

  it("switches to replay after 3 consecutive failures, not before", async () => {
    gw.down = true;
    const p = make();
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);
    expect(replay).toBe(0);
    await vi.advanceTimersByTimeAsync(1000);
    expect(replay).toBe(1);
    const n = polls();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(polls()).toBe(n); // the poller stopped itself
  });

  it("a success resets the failure count", async () => {
    const p = make();
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    gw.fail5xx = true;
    await vi.advanceTimersByTimeAsync(2000); // two 5xx
    gw.fail5xx = false;
    await vi.advanceTimersByTimeAsync(1000); // success
    gw.fail5xx = true;
    await vi.advanceTimersByTimeAsync(2000); // two more
    expect(replay).toBe(0);
    p.stop();
  });

  it("never has two requests in flight", async () => {
    gw.hang = true;
    const p = make();
    p.start();
    await vi.advanceTimersByTimeAsync(0);
    void p.refetch();
    await vi.advanceTimersByTimeAsync(2500);
    expect(polls()).toBe(1); // still waiting on the first (3 s timeout)
    p.stop();
  });

  it("refetch fetches now and updates the cluster", async () => {
    const p = make();
    gw.cluster = cluster({ leaderId: 2 });
    await p.refetch();
    expect(got).toBe(1);
    expect(polls()).toBe(1);
  });
});
