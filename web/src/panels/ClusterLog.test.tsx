import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { cluster, downNode, node } from "../test/fixtures";
import { ClusterView } from "./ClusterView";
import { LogView } from "./LogView";

describe("ClusterView", () => {
  it("shows one card per node with role, term, commit and applied as text", () => {
    render(<ClusterView cluster={cluster()} />);
    const n1 = screen.getByRole("article", { name: /node 1/i });
    expect(within(n1).getByText(/leader/i)).toBeInTheDocument();
    expect(within(n1).getByText(/term 3/i)).toBeInTheDocument();
    expect(within(n1).getByText(/commit 120/i)).toBeInTheDocument();
    expect(within(n1).getByText(/applied 120/i)).toBeInTheDocument();
    const n2 = screen.getByRole("article", { name: /node 2/i });
    expect(within(n2).getByText(/follower/i)).toBeInTheDocument();
  });

  it("groups nodes into one lane per availability zone", () => {
    render(<ClusterView cluster={cluster()} />);
    for (const az of ["us-east-1a", "us-east-1b", "us-east-1c"]) {
      expect(screen.getByRole("group", { name: az })).toBeInTheDocument();
    }
  });

  it("marks only the node named by leaderId as the leader", () => {
    const c = cluster({
      leaderId: 2,
      nodes: [node(1), node(2, { role: "leader" }), node(3)],
    });
    render(<ClusterView cluster={c} />);
    expect(
      within(screen.getByRole("article", { name: /node 2/i })).getByText(/leader/i),
    ).toBeInTheDocument();
    expect(
      within(screen.getByRole("article", { name: /node 1/i })).queryByText(/leader/i),
    ).toBeNull();
  });

  it("shows down, paused and stale nodes in words, not colour alone", () => {
    const c = cluster({
      nodes: [
        node(1, { role: "leader", paused: true }),
        downNode(2),
        node(3, { lastContactMs: 5001 }),
      ],
    });
    render(<ClusterView cluster={c} />);
    expect(
      within(screen.getByRole("article", { name: /node 1/i })).getByText(/paused/i),
    ).toBeInTheDocument();
    expect(
      within(screen.getByRole("article", { name: /node 2/i })).getByText(/down/i),
    ).toBeInTheDocument();
    expect(
      within(screen.getByRole("article", { name: /node 3/i })).getByText(/stale/i),
    ).toBeInTheDocument();
    expect(
      within(screen.getByRole("article", { name: /node 1/i })).queryByText(/stale/i),
    ).toBeNull();
  });

  it("says when there is no leader and when there is no data yet", () => {
    const { rerender } = render(
      <ClusterView cluster={cluster({ leaderId: 0, nodes: [node(1), node(2), node(3)] })} />,
    );
    expect(screen.getByText(/no leader/i)).toBeInTheDocument();
    rerender(<ClusterView cluster={null} />);
    expect(screen.getByText(/waiting for the cluster/i)).toBeInTheDocument();
  });
});

describe("LogView", () => {
  const c = cluster({
    nodes: [
      node(1, {
        role: "leader",
        snapshotIndex: 80,
        lastApplied: 200,
        commitIndex: 200,
        logLen: 121,
      }),
      node(2, { snapshotIndex: 60, lastApplied: 190, commitIndex: 195, logLen: 136 }),
      node(3, {
        snapshotIndex: 0,
        lastApplied: 0,
        commitIndex: 0,
        logLen: 0,
        up: false,
        role: "unknown",
      }),
    ],
  });

  it("offers a table equivalent with the markers, the length and the lag behind the highest commit", () => {
    render(<LogView cluster={c} />);
    const table = screen.getByRole("table");
    const row2 = within(table).getByRole("row", { name: /node 2/i });
    const cells = within(row2)
      .getAllByRole("cell")
      .map((x) => x.textContent);
    // node, snapshot, applied, commit, log length, commit lag, applied lag
    expect(cells).toEqual(["2", "60", "190", "195", "136", "5", "10"]);
  });

  it("draws one bar per node on the absolute index axis with a text alternative", () => {
    render(<LogView cluster={c} />);
    const bar = screen.getByRole("img", { name: /node 1.*snapshot 80.*applied 200.*commit 200/i });
    expect(bar).toBeInTheDocument();
  });

  it("states what it cannot show instead of inventing per-entry terms", () => {
    render(<LogView cluster={c} />);
    expect(screen.getByText(/coloured by the node's current term only/i)).toBeInTheDocument();
    expect(screen.getByText(/does not report per-entry terms/i)).toBeInTheDocument();
  });

  it("shows a down node as down rather than as an empty log", () => {
    render(<LogView cluster={c} />);
    expect(screen.getByRole("img", { name: /node 3.*down/i })).toBeInTheDocument();
  });
});
