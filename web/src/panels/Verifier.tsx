import { useCallback, useEffect, useRef, useState } from "react";
import type { Client } from "../api/client";
import type { ApiResult, VerifyLast, VerifyReport } from "../api/types";
import type { Mode } from "../state/store";
import type { TimelineRow } from "../state/timeline";
import { useNow } from "../state/useNow";

export interface VerifierProps {
  client: Client;
  mode: Mode;
  recorded: VerifyLast | null;
  events: TimelineRow[]; // the `verify` rows, for fault progress
  repoUrl: string;
  onRunningChange: (running: boolean) => void;
}

const WALL_MS = 60_000; // verify.go: a run lasts at most 60 s
const POLL_MS = 2000;
const CE_LIMIT = 64 * 1024;

const mmss = (sec: number) => `${Math.floor(sec / 60)} min ${sec % 60} s`;

function Verdict({ r }: { r: VerifyReport }) {
  if (r.verdict === "linearizable") {
    return <span className="verdict pass">PASS</span>;
  }
  if (r.verdict === "not_linearizable") {
    return <span className="verdict fail">FAIL</span>;
  }
  return <span className="verdict inconclusive">INCONCLUSIVE</span>;
}

function download(r: VerifyReport) {
  if (typeof URL.createObjectURL !== "function") return;
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(r, null, 2)], { type: "application/json" }),
  );
  const a = document.createElement("a");
  a.href = url;
  a.download = `verify-${r.id}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

function Report({ r }: { r: VerifyReport }) {
  const [all, setAll] = useState(false);
  const ce = r.counterexample ?? "";
  const cut = !all && ce.length > CE_LIMIT;
  return (
    <div>
      <p>
        <Verdict r={r} /> <span>{r.verdict.replace("_", " ")}</span>
        {r.status === "aborted" ? <span> (the run was aborted)</span> : null}
      </p>
      {r.verdict === "inconclusive" ? (
        <p className="caption">inconclusive is not a pass: the checker could not decide.</p>
      ) : null}
      <dl className="stats">
        {(
          [
            ["Operations", r.ops],
            ["Reads", r.reads],
            ["Writes", r.writes],
            ["Succeeded", r.ok],
            ["Failed", r.failed],
            ["Outcome unknown", r.unknown],
            ["States explored", r.explored],
            ["Duration", `${(r.elapsedMs / 1000).toFixed(1)} s`],
            ["Cluster recovered", r.recovered ? "yes" : "no"],
          ] as const
        ).map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
      <div className="table-wrap">
        <table>
          <caption>Faults injected by the run</caption>
          <thead>
            <tr>
              <th scope="col">At</th>
              <th scope="col">Action</th>
              <th scope="col">Target</th>
              <th scope="col">Result</th>
            </tr>
          </thead>
          <tbody>
            {r.faults.map((f) => (
              <tr key={`${f.atMs}-${f.action}`}>
                <td>{(f.atMs / 1000).toFixed(1)} s</td>
                <td>{f.action}</td>
                <td>{f.target === undefined ? "none" : `node ${f.target}`}</td>
                <td>{f.ok ? "ok" : `failed${f.error ? `: ${f.error}` : ""}`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {ce === "" ? null : (
        <div>
          <h3>Counterexample</h3>
          <pre className="counterexample" data-testid="counterexample">
            {cut ? ce.slice(0, CE_LIMIT) : ce}
          </pre>
          <div className="btn-row">
            {cut ? (
              <button type="button" className="secondary" onClick={() => setAll(true)}>
                Show all ({ce.length} characters)
              </button>
            ) : null}
            <button
              type="button"
              className="secondary"
              onClick={() => void navigator.clipboard?.writeText(ce).catch(() => {})}
            >
              Copy counterexample
            </button>
          </div>
        </div>
      )}
      <div className="btn-row">
        <button type="button" className="secondary" onClick={() => download(r)}>
          Download JSON
        </button>
      </div>
    </div>
  );
}

/** Panel 6. */
export function Verifier(p: VerifierProps) {
  const live = p.mode === "live";
  const now = useNow(live);
  const [report, setReport] = useState<VerifyReport | null>(null);
  const [none, setNone] = useState(false);
  const [running, setRunning] = useState(false);
  const [startedAt, setStartedAt] = useState(0);
  const [msg, setMsg] = useState<{ text: string; error: boolean } | null>(null);
  const [coolUntil, setCoolUntil] = useState(0);
  const [polls, setPolls] = useState(0);
  const immediate = useRef(false);
  const busy = useRef(false);
  const onRun = useRef(p.onRunningChange);
  useEffect(() => {
    onRun.current = p.onRunningChange;
  }, [p.onRunningChange]);

  const apply = useCallback((r: ApiResult<VerifyLast>) => {
    if (!r.ok) {
      if (r.status === 404) {
        setNone(true);
        setRunning(false);
        onRun.current(false);
      }
      return;
    }
    setNone(false);
    if (r.data.status === "running") {
      setStartedAt(Date.parse(r.data.startedAt) || Date.now());
      setRunning((was) => {
        if (!was) onRun.current(true);
        return true;
      });
    } else {
      setReport(r.data);
      setRunning((was) => {
        if (was) onRun.current(false);
        return false;
      });
    }
  }, []);

  const lastVerifyEvent = p.events.at(-1)?.key;
  // Initial state, and again whenever a verify event arrives (a run by another visitor).
  // biome-ignore lint/correctness/useExhaustiveDependencies: lastVerifyEvent is the trigger
  useEffect(() => {
    if (!live) return;
    let cancelled = false;
    void p.client.verifyLast().then((r) => {
      if (!cancelled) apply(r);
    });
    return () => {
      cancelled = true;
    };
  }, [live, p.client, apply, lastVerifyEvent]);

  // While a run is going: poll verify/last every 2 s.
  // biome-ignore lint/correctness/useExhaustiveDependencies: polls re-arms the timer after each poll
  useEffect(() => {
    if (!live || !running) return;
    let cancelled = false;
    const wait = immediate.current ? 0 : POLL_MS;
    immediate.current = false;
    const t = setTimeout(async () => {
      const r = await p.client.verifyLast();
      if (cancelled) return;
      apply(r);
      setPolls((n) => n + 1);
    }, wait);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [live, running, polls, p.client, apply]);

  async function run() {
    if (!live || running || busy.current || coolUntil > Date.now()) return;
    busy.current = true;
    try {
      const r = await p.client.verifyRun();
      if (r.ok) {
        setMsg({ text: `Run ${r.data.id} started.`, error: false });
        setStartedAt(Date.now());
        setRunning(true);
        p.onRunningChange(true);
      } else if (r.status === 409) {
        setMsg({ text: "A verification run is already running; attached to it.", error: false });
        immediate.current = true;
        setRunning(true);
        p.onRunningChange(true);
      } else if (r.status === 429) {
        setCoolUntil(Date.now() + (r.retryAfterSec ?? 900) * 1000);
        setMsg(null);
      } else {
        setMsg({ text: r.error, error: true });
      }
    } finally {
      busy.current = false;
    }
  }

  const shown: VerifyReport | null =
    p.mode === "replay"
      ? p.recorded && p.recorded.status !== "running"
        ? p.recorded
        : null
      : report;
  const coolLeft = Math.max(0, Math.ceil((coolUntil - now) / 1000));
  const runOff = !live || running || coolLeft > 0;
  const elapsed = Math.min(1, Math.max(0, (now - startedAt) / WALL_MS));

  return (
    <section className="panel wide" aria-labelledby="h-verify">
      <h2 id="h-verify">Verifier</h2>
      <div className="btn-row">
        <button type="button" aria-disabled={runOff} onClick={() => void run()}>
          Run verification
        </button>
      </div>
      {!live ? <p className="reason muted">replay is read-only</p> : null}
      {coolLeft > 0 ? (
        <>
          <p className="reason muted">A run was done recently; try again in {mmss(coolLeft)}.</p>
          <p className="caption">The admin token skips this limit.</p>
        </>
      ) : null}
      <p className="status" role="status" aria-live="polite" data-error={msg?.error ?? false}>
        {msg?.text ?? ""}
      </p>
      {running ? (
        <div>
          <progress aria-label="Verification progress" max={1} value={elapsed} />
          <p className="caption">
            Progress is elapsed time out of 60 s; operation counts are not reported while running.
          </p>
          <ul>
            {p.events.map((e) => (
              <li key={e.key}>{e.text}</li>
            ))}
          </ul>
        </div>
      ) : null}
      {shown ? <Report r={shown} /> : null}
      {!shown && !running && (none || !live) ? <p>No run yet.</p> : null}
      {p.repoUrl ? (
        <p>
          <a href={`${p.repoUrl.replace(/\/+$/, "")}/blob/main/docs/CHAOS_RESULTS.md`}>
            Latest report (docs/CHAOS_RESULTS.md)
          </a>
        </p>
      ) : null}
    </section>
  );
}
