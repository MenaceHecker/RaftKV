import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { addEvent, addNote, emptyTimeline } from "../state/timeline";
import { Timeline } from "./Timeline";

function rows() {
  let t = emptyTimeline();
  t = addEvent(
    t,
    "state",
    { node: 2, term: 4, role: "leader", leaderId: 2, seq: 1, unixNano: 5 },
    1000,
  );
  t = addEvent(t, "commit", { node: 1, commitIndex: 12 }, 1100);
  t = addNote(t, "events may be missing", 1200);
  return t.rows;
}

describe("Timeline panel", () => {
  it("is a polite live log with the receive time shown as browser time", () => {
    render(<Timeline rows={rows()} />);
    const log = screen.getByRole("log");
    expect(log).toHaveAttribute("aria-live", "polite");
    expect(within(log).getByText(/node 2 became leader, term 4/)).toBeInTheDocument();
    expect(screen.getByText(/times are when your browser received the event/i)).toBeInTheDocument();
  });

  it("hides commit rows by default and shows them with the toggle, never announced", async () => {
    render(<Timeline rows={rows()} />);
    expect(screen.queryByText(/commit index 12/i)).toBeNull();
    await userEvent.click(screen.getByRole("checkbox", { name: /show commit rows/i }));
    const row = screen.getByText(/node 1 commit index 12/i).closest("li");
    expect(row).toHaveAttribute("aria-live", "off");
  });

  it("says that votes are not reported by the nodes", () => {
    render(<Timeline rows={[]} />);
    expect(screen.getByText(/votes are not reported by the nodes/i)).toBeInTheDocument();
  });

  it("labels derived rows as derived", () => {
    const t = addNote(emptyTimeline(), "x", 1);
    const derived = [
      { ...t.rows[0], kind: "caught_up", derived: true, text: "node 2 caught up (derived)" },
    ];
    // biome-ignore lint/suspicious/noExplicitAny: row literal
    render(<Timeline rows={derived as any} />);
    expect(screen.getByText(/derived/)).toBeInTheDocument();
  });
});
