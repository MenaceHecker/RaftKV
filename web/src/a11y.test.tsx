import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { axe } from "vitest-axe";
import { createClient } from "./api/client";
import { AdminToken } from "./panels/AdminToken";
import { ClusterView } from "./panels/ClusterView";
import { Controls } from "./panels/Controls";
import { KvPlayground } from "./panels/KvPlayground";
import { LogView } from "./panels/LogView";
import { ModeBanner } from "./panels/ModeBanner";
import { Timeline } from "./panels/Timeline";
import { Verifier } from "./panels/Verifier";
import { initialState } from "./state/store";
import { addEvent, emptyTimeline } from "./state/timeline";
import { cluster, downNode, node, report } from "./test/fixtures";
import { MockGateway } from "./test/mockGateway";

const client = createClient({
  baseUrl: "http://gw",
  fetch: new MockGateway().fetch,
  getToken: () => null,
});
const busy = cluster({
  nodes: [node(1, { role: "leader", paused: true }), downNode(2), node(3, { lastContactMs: 9000 })],
});

async function clean(ui: React.ReactElement) {
  const { container } = render(ui);
  // a stub renders "todo": the axe run must be on a real panel
  expect(container.textContent).not.toMatch(/todo/);
  expect(container.textContent?.length).toBeGreaterThan(15);
  const results = await axe(container);
  expect(results).toHaveNoViolations();
}

describe("axe", () => {
  it("ClusterView", async () => {
    await clean(<ClusterView cluster={busy} />);
  });
  it("LogView", async () => {
    await clean(<LogView cluster={busy} />);
  });
  it("Timeline", async () => {
    let t = emptyTimeline();
    t = addEvent(t, "node_down", { node: 1 }, 1);
    await clean(<Timeline rows={t.rows} />);
  });
  it("Controls (live and replay)", async () => {
    await clean(
      <Controls
        client={client}
        cluster={cluster()}
        mode="live"
        verifying={false}
        lastFaultAt={null}
        hasToken={false}
      />,
    );
    await clean(
      <Controls
        client={client}
        cluster={cluster()}
        mode="replay"
        verifying={false}
        lastFaultAt={null}
        hasToken={false}
      />,
    );
  });
  it("KvPlayground", async () => {
    vi.useRealTimers();
    await clean(<KvPlayground client={client} mode="live" verifying={false} />);
  });
  it("Verifier with a failing report", async () => {
    await clean(
      <Verifier
        client={client}
        mode="replay"
        recorded={report({ verdict: "not_linearizable", counterexample: "op 1 ..." })}
        events={[]}
        repoUrl="https://example.test/repo"
        onRunningChange={() => {}}
      />,
    );
  });
  it("AdminToken", async () => {
    await clean(<AdminToken />);
  });
  it("ModeBanner (live and replay)", async () => {
    await clean(<ModeBanner state={{ ...initialState(), mode: "live" }} />);
    await clean(
      <ModeBanner
        state={{
          ...initialState(),
          mode: "replay",
          replayReason: "unreachable",
          recordedAt: "2026-11-01T10:00:00Z",
        }}
      />,
    );
  });
});
