import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../api/client";
import { CAS_DETAIL } from "../api/types";
import { MockGateway } from "../test/mockGateway";
import { KvPlayground, type KvProps } from "./KvPlayground";

let gw: MockGateway;
const user = () => userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
function ui(over: Partial<KvProps> = {}) {
  const client = createClient({ baseUrl: "http://gw", fetch: gw.fetch, getToken: () => null });
  return <KvPlayground client={client} mode="live" verifying={false} {...over} />;
}
const key = () => screen.getByLabelText(/^key/i);
const value = () => screen.getByLabelText(/^value/i);
const btn = (n: RegExp) => screen.getByRole("button", { name: n });

beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
});
afterEach(() => vi.useRealTimers());

describe("KvPlayground", () => {
  it("counts UTF-8 bytes against 64 for the key and 1024 for the value", async () => {
    render(ui());
    const u = user();
    await u.type(key(), "héllo");
    await u.type(value(), "abc");
    expect(screen.getByText("6 / 64 bytes")).toBeInTheDocument();
    expect(screen.getByText("3 / 1024 bytes")).toBeInTheDocument();
  });

  it("Put then Get round-trips and shows latency labelled as measured in the browser", async () => {
    render(ui());
    const u = user();
    await u.type(key(), "k1");
    await u.type(value(), "v1");
    await u.click(btn(/^put/i));
    expect(await screen.findByText(/put ok/i)).toBeInTheDocument();
    expect(screen.getByText(/\d+ ms from your browser/i)).toBeInTheDocument();
    await u.click(btn(/^get/i));
    expect(await screen.findByText(/"v1"/)).toBeInTheDocument();
    expect(gw.kv.get("k1")).toBe("v1");
  });

  it("Get of a missing key says not found", async () => {
    render(ui());
    const u = user();
    await u.type(key(), "nope");
    await u.click(btn(/^get/i));
    expect(await screen.findByText(/not found/i)).toBeInTheDocument();
  });

  it("Delete removes the key", async () => {
    gw.kv.set("k", "v");
    render(ui());
    const u = user();
    await u.type(key(), "k");
    await u.click(btn(/^delete/i));
    expect(await screen.findByText(/delete ok/i)).toBeInTheDocument();
    expect(gw.kv.has("k")).toBe(false);
  });

  it("a 504 with outcome unknown is explained, not shown as a plain failure", async () => {
    gw.writeOutcome = "unknown";
    render(ui());
    const u = user();
    await u.type(key(), "k");
    await u.type(value(), "v");
    await u.click(btn(/^put/i));
    expect(
      await screen.findByText(/may or may not have been applied; Get to check/i),
    ).toBeInTheDocument();
  });

  it("a value over 1024 bytes disables Put with the reason and sends nothing", async () => {
    render(ui());
    const u = user();
    await u.type(key(), "k");
    await u.click(value());
    await u.paste("x".repeat(1025));
    expect(btn(/^put/i)).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText(/value is longer than 1024 bytes/i)).toBeInTheDocument();
    await u.click(btn(/^put/i));
    expect(gw.callsTo("/v1/kv/")).toHaveLength(0);
  });

  it("shows compare-and-swap disabled with the gateway's own explanation and never calls it", async () => {
    render(ui());
    const cas = btn(/compare-and-swap/i);
    expect(cas).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText(CAS_DETAIL)).toBeInTheDocument();
    await user().click(cas);
    expect(gw.calls.some((c) => c.path.endsWith("/cas"))).toBe(false);
  });

  it("disables writes with the reason while verification runs, but not Get", async () => {
    render(ui({ verifying: true }));
    expect(btn(/^put/i)).toHaveAttribute("aria-disabled", "true");
    expect(btn(/^delete/i)).toHaveAttribute("aria-disabled", "true");
    expect(btn(/^get/i)).not.toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText(/verification in progress/i)).toBeInTheDocument();
  });

  it("is read-only in replay mode", () => {
    render(ui({ mode: "replay" }));
    expect(btn(/^get/i)).toHaveAttribute("aria-disabled", "true");
    expect(screen.getAllByText(/replay is read-only/i).length).toBeGreaterThan(0);
  });

  it("never persists keys or values", async () => {
    render(ui());
    const u = user();
    await u.type(key(), "secretkey");
    await u.type(value(), "secretvalue");
    await u.click(btn(/^put/i));
    await vi.advanceTimersByTimeAsync(10);
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
    expect(document.cookie).toBe("");
  });
});
