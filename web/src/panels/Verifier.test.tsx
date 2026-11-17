import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createClient } from "../api/client";
import { report } from "../test/fixtures";
import { MockGateway } from "../test/mockGateway";
import { Verifier, type VerifierProps } from "./Verifier";

let gw: MockGateway;
let running: boolean[];
const user = () => userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
function ui(over: Partial<VerifierProps> = {}) {
  const client = createClient({ baseUrl: "http://gw", fetch: gw.fetch, getToken: () => null });
  return (
    <Verifier
      client={client}
      mode="live"
      recorded={null}
      events={[]}
      repoUrl="https://example.test/repo"
      onRunningChange={(r) => running.push(r)}
      {...over}
    />
  );
}
const run = () => screen.getByRole("button", { name: /run verification/i });

beforeEach(() => {
  vi.useFakeTimers();
  gw = new MockGateway();
  running = [];
});
afterEach(() => vi.useRealTimers());

describe("Verifier", () => {
  it("says there is no run yet on a 404", async () => {
    render(ui());
    await vi.advanceTimersByTimeAsync(0);
    expect(await screen.findByText(/no run yet/i)).toBeInTheDocument();
  });

  it("runs: 202, progress by time, then the result from verify/last", async () => {
    render(ui());
    await user().click(run());
    expect(gw.calls.some((c) => c.method === "POST" && c.path === "/v1/verify/run")).toBe(true);
    expect(running).toContain(true);
    expect(screen.getByRole("progressbar", { name: /verification/i })).toBeInTheDocument();
    expect(
      screen.getByText(/operation counts are not reported while running/i),
    ).toBeInTheDocument();
    await vi.advanceTimersByTimeAsync(30_000);
    const bar = screen.getByRole("progressbar", { name: /verification/i }) as HTMLProgressElement;
    expect(bar.value).toBeGreaterThan(0.4);
    expect(bar.value).toBeLessThan(0.6);
    await vi.advanceTimersByTimeAsync(31_000);
    expect(await screen.findByText(/PASS/)).toBeInTheDocument();
    expect(running.at(-1)).toBe(false);
  });

  it("polls verify/last every 2 s while running", async () => {
    render(ui());
    await user().click(run());
    const before = gw.callsTo("/v1/verify/last").length;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(gw.callsTo("/v1/verify/last").length - before).toBe(5);
  });

  it("a 409 attaches to the run that is already going", async () => {
    await createClient({ baseUrl: "http://gw", fetch: gw.fetch, getToken: () => null }).verifyRun();
    render(ui());
    await user().click(run());
    expect(await screen.findByText(/already running/i)).toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: /verification/i })).toBeInTheDocument();
  });

  it("a 429 shows a countdown from Retry-After and mentions the admin token", async () => {
    const c = createClient({ baseUrl: "http://gw", fetch: gw.fetch, getToken: () => null });
    await c.verifyRun();
    gw.finishVerify();
    await vi.advanceTimersByTimeAsync(60_000);
    render(ui());
    await user().click(run());
    expect(await screen.findByText(/try again in 14 min/i)).toBeInTheDocument();
    expect(screen.getByText(/the admin token skips this/i)).toBeInTheDocument();
    expect(run()).toHaveAttribute("aria-disabled", "true");
  });

  it.each([
    ["linearizable", /PASS/, null],
    ["not_linearizable", /FAIL/, null],
    ["inconclusive", /INCONCLUSIVE/, /inconclusive is not a pass/i],
  ] as const)("shows verdict %s", (verdict, badge, note) => {
    render(ui({ mode: "replay", recorded: report({ verdict }) }));
    expect(screen.getByText(badge)).toBeInTheDocument();
    if (note) expect(screen.getByText(note)).toBeInTheDocument();
    else expect(screen.queryByText(/inconclusive is not a pass/i)).toBeNull();
  });

  it("shows the stats and the fault table", () => {
    render(ui({ mode: "replay", recorded: report() }));
    expect(screen.getByText(/812/)).toBeInTheDocument();
    const table = screen.getByRole("table", { name: /faults/i });
    expect(within(table).getByText("partition")).toBeInTheDocument();
    expect(within(table).getAllByRole("row")).toHaveLength(4);
  });

  it("renders a hostile counterexample as inert text", () => {
    const evil = '<img src=x onerror="window.__pwn=1"><script>window.__pwn=2</script>';
    const { container } = render(
      ui({
        mode: "replay",
        recorded: report({ verdict: "not_linearizable", counterexample: evil }),
      }),
    );
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("script")).toBeNull();
    expect(screen.getByText(evil, { exact: false }).tagName).toBe("PRE");
    expect((window as unknown as { __pwn?: number }).__pwn).toBeUndefined();
  });

  it("truncates a huge counterexample at 64 KB until 'Show all' is pressed", async () => {
    const big = "x".repeat(70_000);
    render(
      ui({
        mode: "replay",
        recorded: report({ verdict: "not_linearizable", counterexample: big }),
      }),
    );
    const pre = screen.getByTestId("counterexample");
    expect(pre.textContent?.length).toBeLessThanOrEqual(65_536);
    await user().click(screen.getByRole("button", { name: /show all/i }));
    expect(screen.getByTestId("counterexample").textContent).toHaveLength(70_000);
  });

  it("offers copy, download and a link to the results document", () => {
    render(
      ui({
        mode: "replay",
        recorded: report({ verdict: "not_linearizable", counterexample: "c" }),
      }),
    );
    expect(screen.getByRole("button", { name: /copy/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /download json/i })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /latest report/i })).toHaveAttribute(
      "href",
      "https://example.test/repo/blob/main/docs/CHAOS_RESULTS.md",
    );
  });

  it("is read-only in replay and shows the recorded report", () => {
    render(ui({ mode: "replay", recorded: report() }));
    expect(run()).toHaveAttribute("aria-disabled", "true");
    expect(screen.getByText(/replay is read-only/i)).toBeInTheDocument();
    expect(gw.calls).toHaveLength(0);
  });

  it("lists fault rows from verify events while running", async () => {
    render(
      ui({
        events: [{ key: 1, at: 1, kind: "verify", text: "verification fault: partition ok" }],
      }),
    );
    await user().click(run());
    expect(screen.getByText(/verification fault: partition ok/)).toBeInTheDocument();
  });
});
