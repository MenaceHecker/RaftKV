import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../api/client";
import { cluster, node, withChaos } from "../test/fixtures";
import { ADMIN_TOKEN, MockGateway } from "../test/mockGateway";
import { tick } from "../test/tick";
import { Controls, type ControlsProps } from "./Controls";

let gw: MockGateway;
let token: string | null;
const user = () => userEvent.setup({ delay: null });

function client() {
  return createClient({ baseUrl: "http://gw", fetch: gw.fetch, getToken: () => token });
}
function ui(over: Partial<ControlsProps> = {}) {
  return (
    <Controls
      client={client()}
      cluster={gw.cluster}
      mode="live"
      verifying={false}
      lastFaultAt={null}
      hasToken={token !== null}
      {...over}
    />
  );
}
const btn = (name: RegExp) => screen.getByRole("button", { name });
const disabled = (name: RegExp) => expect(btn(name)).toHaveAttribute("aria-disabled", "true");
const enabled = (name: RegExp) => expect(btn(name)).not.toHaveAttribute("aria-disabled", "true");
const faultCalls = () =>
  gw.calls.filter((c) => c.path.startsWith("/v1/chaos/") && !c.path.endsWith("/heal"));

beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
  token = null;
});
afterEach(() => vi.useRealTimers());

describe("Controls", () => {
  it("Kill leader posts once and reports the fault with its auto-heal time", async () => {
    render(ui());
    await user().click(btn(/kill leader/i));
    expect(gw.calls.filter((c) => c.path === "/v1/chaos/kill-leader")).toHaveLength(1);
    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(
        /kill.*node 1.*heals? (automatically )?in 45 s/i,
      ),
    );
  });

  it("Isolate leader posts partition without a body", async () => {
    render(ui());
    await user().click(btn(/isolate leader/i));
    expect(gw.calls.at(-1)).toMatchObject({
      method: "POST",
      path: "/v1/chaos/partition",
      body: null,
    });
  });

  it("a double click sends one request", async () => {
    render(ui());
    await user().dblClick(btn(/kill leader/i));
    expect(faultCalls()).toHaveLength(1);
  });

  it("Heal is always enabled in live mode, even with a fault active or a run going", () => {
    const active = withChaos(cluster(), {
      state: "active",
      action: "kill",
      target: 1,
      expiresAt: new Date(Date.now() + 30_000).toISOString(),
    });
    render(ui({ cluster: active, verifying: true }));
    enabled(/^heal/i);
  });

  it("fault buttons are disabled with a countdown while a fault is active", async () => {
    const active = withChaos(cluster(), {
      state: "active",
      action: "kill",
      target: 1,
      expiresAt: new Date(Date.now() + 30_000).toISOString(),
    });
    render(ui({ cluster: active }));
    disabled(/kill leader/i);
    disabled(/isolate leader/i);
    expect(screen.getByText(/fault active, heals in 30 s/i)).toBeInTheDocument();
    await tick(1000);
    expect(screen.getByText(/fault active, heals in 29 s/i)).toBeInTheDocument();
  });

  it("explains the recovering state", () => {
    render(
      ui({ cluster: withChaos(cluster(), { state: "recovering", action: "kill", target: 1 }) }),
    );
    disabled(/kill leader/i);
    expect(screen.getByText(/recovering: waiting for every node/i)).toBeInTheDocument();
  });

  it("is disabled with a reason while verification runs", () => {
    render(ui({ verifying: true }));
    disabled(/kill leader/i);
    expect(screen.getByText(/verification is running/i)).toBeInTheDocument();
  });

  it("is read-only in replay mode, including Heal", () => {
    render(ui({ mode: "replay" }));
    disabled(/kill leader/i);
    disabled(/^heal/i);
    expect(screen.getAllByText(/replay is read-only/i).length).toBeGreaterThan(0);
  });

  it("disabled buttons stay focusable (aria-disabled, not disabled) and do nothing when pressed", async () => {
    render(ui({ verifying: true }));
    const b = btn(/kill leader/i);
    expect(b).not.toBeDisabled();
    await user().click(b);
    expect(faultCalls()).toHaveLength(0);
  });

  it("Kill a follower needs the admin token and says so", () => {
    render(ui());
    disabled(/kill a follower/i);
    expect(screen.getByText(/needs the admin token/i)).toBeInTheDocument();
  });

  it("with a token, Kill a follower kills an up follower and sends the token", async () => {
    token = ADMIN_TOKEN;
    render(ui());
    enabled(/kill a follower/i);
    await user().click(btn(/kill a follower/i));
    const call = gw.calls.find((c) => /^\/v1\/chaos\/kill\/[23]$/.test(c.path));
    expect(call?.headers.authorization).toBe(`Bearer ${ADMIN_TOKEN}`);
  });

  it("estimates a 20 s cooldown after our own fault, labelled 'about', and re-enables after it", async () => {
    render(ui());
    await user().click(btn(/isolate leader/i));
    await tick(0);
    disabled(/isolate leader/i);
    expect(screen.getByText(/cooldown about 20 s/i)).toBeInTheDocument();
    await tick(20_000);
    enabled(/isolate leader/i);
  });

  it("starts the cooldown estimate from a fault event seen at lastFaultAt", () => {
    render(ui({ lastFaultAt: Date.now() - 5000 }));
    disabled(/kill leader/i);
    expect(screen.getByText(/cooldown about 15 s/i)).toBeInTheDocument();
  });

  it("a 429 shows the gateway's Retry-After as the cooldown", async () => {
    await client().chaos.killLeader();
    await client().chaos.heal(); // slot free, bucket still used
    await tick(3000);
    gw.cluster = cluster();
    render(ui());
    await user().click(btn(/isolate leader/i));
    await tick(0);
    expect(screen.getByRole("status")).toHaveTextContent(/rate limit|chaos rate limit/i);
    expect(screen.getByText(/cooldown about 17 s/i)).toBeInTheDocument();
  });

  it("a token removes the cooldown estimate", async () => {
    token = ADMIN_TOKEN;
    render(ui());
    await user().click(btn(/isolate leader/i));
    await tick(0);
    expect(screen.queryByText(/cooldown/i)).toBeNull();
    enabled(/isolate leader/i);
  });

  it("shows the gateway's own refusal text for 503 and 409", async () => {
    gw.cluster = cluster({
      nodes: [node(1, { role: "leader" }), node(2, { up: false, role: "unknown" }), node(3)],
    });
    render(ui());
    await user().click(btn(/kill leader/i));
    const status = screen.getByRole("status");
    expect(status).toHaveAttribute("aria-live", "polite");
    await waitFor(() =>
      expect(status).toHaveTextContent(/cluster is not healthy enough for a fault/i),
    );
  });

  it("More faults: Pause and Delay send their bodies", async () => {
    render(ui());
    const u = user();
    await u.click(screen.getByText(/more faults/i));
    const secs = screen.getByLabelText(/pause seconds/i);
    await u.clear(secs);
    await u.type(secs, "10");
    await u.click(btn(/^pause leader/i));
    expect(gw.calls.at(-1)).toMatchObject({ path: "/v1/chaos/pause/1", body: '{"seconds":10}' });
  });

  it("More faults: Delay validates 500 ms / 50 % before sending", async () => {
    render(ui());
    const u = user();
    await u.click(screen.getByText(/more faults/i));
    const ms = screen.getByLabelText(/delay ms/i);
    await u.clear(ms);
    await u.type(ms, "600");
    disabled(/^add delay/i);
    expect(screen.getByText(/at most 500 ms/i)).toBeInTheDocument();
    await u.clear(ms);
    await u.type(ms, "200");
    await u.clear(screen.getByLabelText(/drop %/i));
    await u.type(screen.getByLabelText(/drop %/i), "10");
    await u.click(btn(/^add delay/i));
    expect(gw.calls.at(-1)).toMatchObject({
      path: "/v1/chaos/delay",
      body: '{"delayMs":200,"dropPercent":10}',
    });
  });

  it("has a named region", () => {
    render(ui());
    expect(
      within(screen.getByRole("region", { name: /controls/i })).getAllByRole("button").length,
    ).toBeGreaterThan(3);
  });
});
