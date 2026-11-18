import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EVENT_NAMES } from "../api/types";
import { FakeEventSource, FakeEventSourceCtor } from "../test/fakeEventSource";
import { createEventStream } from "./eventStream";

let events: { name: string; id: number | null; data: unknown }[];
let gaps: number;
let statuses: string[];

function make(rand = () => 0) {
  return createEventStream({
    url: "http://gw/v1/events",
    EventSource: FakeEventSourceCtor,
    rand,
    onEvent: (e) => events.push(e),
    onGap: () => {
      gaps++;
    },
    onStatus: (s) => statuses.push(s),
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  FakeEventSource.reset();
  events = [];
  gaps = 0;
  statuses = [];
});
afterEach(() => vi.useRealTimers());

describe("event stream", () => {
  it("opens exactly one source on the events URL", () => {
    const s = make();
    s.start();
    s.start();
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(FakeEventSource.instances[0]?.url).toBe("http://gw/v1/events");
  });

  it("routes all 8 named events with their ids and parsed data", () => {
    make().start();
    const es = FakeEventSource.instances[0];
    es?.open();
    for (const [i, n] of EVENT_NAMES.entries())
      es?.emit(n, { n }, n === "resync" ? undefined : i + 1);
    expect(events.map((e) => e.name)).toEqual([...EVENT_NAMES]);
    expect(events[0]).toEqual({ name: "state", id: 1, data: { n: "state" } });
    expect(events.at(-1)?.id).toBeNull(); // resync carries no id
  });

  it("ignores unknown event names", () => {
    make().start();
    const es = FakeEventSource.instances[0];
    es?.emit("mystery", { a: 1 }, 1);
    expect(events).toEqual([]);
  });

  it("counts malformed JSON and never throws", () => {
    const s = make();
    s.start();
    const es = FakeEventSource.instances[0];
    expect(() => es?.emit("state", "{not json", 1)).not.toThrow();
    expect(events).toEqual([]);
    expect(s.badJsonCount()).toBe(1);
  });

  it("de-duplicates by id: an id <= the last seen is dropped", () => {
    make().start();
    const es = FakeEventSource.instances[0];
    es?.emit("state", { a: 1 }, 5);
    es?.emit("state", { a: 2 }, 5);
    es?.emit("state", { a: 3 }, 4);
    es?.emit("state", { a: 4 }, 6);
    expect(events.map((e) => e.id)).toEqual([5, 6]);
  });

  it("a native reconnect (readyState CONNECTING) is left alone", async () => {
    make().start();
    const es = FakeEventSource.instances[0];
    es?.open();
    es?.errorReconnecting();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(es?.readyState).not.toBe(2);
    expect(gaps).toBe(0);
  });

  it("a CLOSED source is recreated with backoff 5 s, 10 s, 20 s ... capped at 60 s, marking a gap", async () => {
    make(() => 0).start();
    const delays = [5000, 10000, 20000, 40000, 60000, 60000];
    for (const [i, d] of delays.entries()) {
      FakeEventSource.live.at(-1)?.errorClosed();
      expect(FakeEventSource.instances).toHaveLength(i + 1);
      await vi.advanceTimersByTimeAsync(d - 1);
      expect(FakeEventSource.instances).toHaveLength(i + 1);
      await vi.advanceTimersByTimeAsync(1);
      expect(FakeEventSource.instances).toHaveLength(i + 2);
    }
    expect(gaps).toBe(delays.length);
    expect(statuses).toContain("unavailable");
  });

  it("three errors in a row without an open also recreate the source", async () => {
    make(() => 0).start();
    const es = FakeEventSource.instances[0];
    es?.errorReconnecting();
    es?.errorReconnecting();
    expect(FakeEventSource.instances).toHaveLength(1);
    es?.errorReconnecting();
    expect(es?.readyState).toBe(2); // the wrapper closed it
    await vi.advanceTimersByTimeAsync(5000);
    expect(FakeEventSource.instances).toHaveLength(2);
    expect(gaps).toBe(1);
  });

  it("an open resets the failure count and the backoff", async () => {
    make(() => 0).start();
    FakeEventSource.live.at(-1)?.errorClosed();
    await vi.advanceTimersByTimeAsync(5000);
    FakeEventSource.live.at(-1)?.open();
    expect(statuses.at(-1)).toBe("open");
    FakeEventSource.live.at(-1)?.errorClosed();
    await vi.advanceTimersByTimeAsync(4999);
    expect(FakeEventSource.instances).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(FakeEventSource.instances).toHaveLength(3);
  });

  it("never holds two live sources", async () => {
    const s = make(() => 0);
    s.start();
    for (let i = 0; i < 4; i++) {
      FakeEventSource.live.at(-1)?.errorClosed();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(FakeEventSource.live).toHaveLength(1);
    }
    s.setVisible(false);
    s.setVisible(true);
    expect(FakeEventSource.live.length).toBeLessThanOrEqual(1);
  });

  it("closes after 60 s hidden and reopens, with a gap, when visible again", async () => {
    const s = make();
    s.start();
    FakeEventSource.instances[0]?.open();
    s.setVisible(false);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(FakeEventSource.live).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(FakeEventSource.live).toHaveLength(0);
    expect(statuses.at(-1)).toBe("off");
    s.setVisible(true);
    expect(FakeEventSource.live).toHaveLength(1);
    expect(gaps).toBe(1);
  });

  it("becoming visible before 60 s cancels the close", async () => {
    const s = make();
    s.start();
    s.setVisible(false);
    await vi.advanceTimersByTimeAsync(30_000);
    s.setVisible(true);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(FakeEventSource.live).toHaveLength(1);
    expect(gaps).toBe(0);
  });

  it("stop closes the source and cancels a pending reconnect", async () => {
    const s = make();
    s.start();
    FakeEventSource.instances[0]?.errorClosed();
    s.stop();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(FakeEventSource.live).toHaveLength(0);
  });
});
