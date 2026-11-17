import { useState } from "react";
import type { TimelineRow } from "../state/timeline";

function clock(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour12: false });
}

/** Panel 3: only what the API provides. Newest first. Commit rows are hidden by default. */
export function Timeline({ rows }: { rows: TimelineRow[] }) {
  const [showCommits, setShowCommits] = useState(false);
  const shown = rows.filter((r) => showCommits || r.kind !== "commit");
  return (
    <section className="panel" aria-labelledby="h-timeline">
      <h2 id="h-timeline">Timeline</h2>
      <label className="check">
        <input
          type="checkbox"
          checked={showCommits}
          onChange={(e) => setShowCommits(e.target.checked)}
        />{" "}
        Show commit rows
      </label>
      <p className="caption">
        Times are when your browser received the event, not node time. A row computed by this page,
        not reported by the gateway, says so in its text.
      </p>
      <p className="caption">
        Votes are not reported by the nodes, so an election shows as role changes only.
      </p>
      <ol className="timeline" role="log" aria-live="polite" aria-relevant="additions">
        {shown.length === 0 ? <li className="muted">No events yet.</li> : null}
        {[...shown].reverse().map((r) => (
          <li
            key={r.key}
            aria-live={r.kind === "commit" ? "off" : undefined}
            className={`ev-${r.kind}`}
          >
            <time>{clock(r.at)}</time> <span>{r.text}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}
