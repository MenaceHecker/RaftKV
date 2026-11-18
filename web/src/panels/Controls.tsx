import { useRef, useState } from "react";
import type { Client } from "../api/client";
import type { ApiResult, ChaosAck, Cluster } from "../api/types";
import type { Mode } from "../state/store";
import { useNow } from "../state/useNow";

export interface ControlsProps {
  client: Client;
  cluster: Cluster | null;
  mode: Mode;
  verifying: boolean;
  lastFaultAt: number | null;
  hasToken: boolean;
}

// The gateway exposes no bucket state, so this is an estimate (chaos.go: one fault per 20 s).
const BUCKET_MS = 20_000;
const MAX_PAUSE = 45;
const MIN_PAUSE = 5;
const MAX_DELAY_MS = 500;
const MAX_DROP = 50;

const whole = (s: string): number | null => (/^\d+$/.test(s.trim()) ? Number(s) : null);

interface Status {
  text: string;
  error: boolean;
}

/** Panel 4. Disabled buttons use aria-disabled (not disabled) so keyboards still find them. */
export function Controls(p: ControlsProps) {
  const now = useNow();
  const [status, setStatus] = useState<Status>({ text: "", error: false });
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [coolUntil, setCoolUntil] = useState(0);
  const [pauseSecs, setPauseSecs] = useState("20");
  const [delayMs, setDelayMs] = useState("100");
  const [drop, setDrop] = useState("0");

  const chaos = p.cluster?.chaos;
  const leader = p.cluster?.leaderId ?? 0;
  const followers = (p.cluster?.nodes ?? []).filter(
    (n) => n.up && n.id !== leader && n.role === "follower",
  );

  const cooldownEnd = Math.max(coolUntil, p.lastFaultAt === null ? 0 : p.lastFaultAt + BUCKET_MS);
  const cooldownLeft = p.hasToken ? 0 : Math.max(0, Math.ceil((cooldownEnd - now) / 1000));

  let reason: string | null = null;
  if (p.mode === "replay") reason = "replay is read-only";
  else if (p.verifying) reason = "verification is running";
  else if (p.cluster === null) reason = "waiting for the cluster";
  else if (chaos?.state === "active") {
    const left = chaos.expiresAt
      ? Math.max(0, Math.ceil((Date.parse(chaos.expiresAt) - now) / 1000))
      : 0;
    reason = `fault active, heals in ${left} s`;
  } else if (chaos?.state === "recovering") reason = "recovering: waiting for every node";
  else if (cooldownLeft > 0) reason = `cooldown about ${cooldownLeft} s`;

  const faultOff = reason !== null || busy;

  async function act<T>(label: string, fn: () => Promise<ApiResult<T>>, ok: (d: T) => string) {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    try {
      const r = await fn();
      if (r.ok) {
        setStatus({ text: ok(r.data), error: false });
      } else {
        setStatus({ text: `${label}: ${r.error}`, error: true });
        if (r.status === 429 && r.retryAfterSec) setCoolUntil(Date.now() + r.retryAfterSec * 1000);
      }
      return r;
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }

  const faultText = (a: ChaosAck) =>
    `${a.action} on node ${a.target} injected; heals automatically in ${a.autoHealSeconds} s`;

  async function fault(label: string, fn: () => Promise<ApiResult<ChaosAck>>) {
    if (faultOff) return;
    const r = await act(label, fn, faultText);
    if (r?.ok && !p.hasToken) setCoolUntil(Date.now() + BUCKET_MS);
  }

  const pauseN = whole(pauseSecs);
  const pauseBad = pauseN === null || pauseN < MIN_PAUSE || pauseN > MAX_PAUSE;
  const delayN = whole(delayMs);
  const dropN = whole(drop);
  const delayBad = delayN === null || delayN > MAX_DELAY_MS;
  const dropBad = dropN === null || dropN > MAX_DROP;

  const killFollower = () => {
    const f = followers[Math.floor(Math.random() * followers.length)];
    if (!f) return;
    void fault("Kill a follower", () => p.client.chaos.kill(f.id));
  };

  const followerOff = faultOff || !p.hasToken || followers.length === 0;
  const healOff = p.mode === "replay";

  return (
    <section className="panel" aria-labelledby="h-controls">
      <h2 id="h-controls">Controls</h2>
      <div className="btn-row">
        <button
          type="button"
          aria-disabled={faultOff}
          onClick={() => void fault("Kill leader", () => p.client.chaos.killLeader())}
        >
          Kill leader
        </button>
        <button
          type="button"
          aria-disabled={faultOff}
          onClick={() => void fault("Isolate leader", () => p.client.chaos.partition())}
        >
          Isolate leader
        </button>
        <button
          type="button"
          aria-disabled={followerOff}
          onClick={() => {
            if (!followerOff) killFollower();
          }}
        >
          Kill a follower
        </button>
        <button
          type="button"
          className="secondary"
          aria-disabled={healOff}
          onClick={() => {
            if (!healOff) {
              void act(
                "Heal",
                () => p.client.chaos.heal(),
                (h) =>
                  h.failed.length === 0
                    ? "healed every node"
                    : `healed nodes ${h.healed.join(", ")}; failed: ${h.failed.join(", ")}`,
              );
            }
          }}
        >
          Heal
        </button>
      </div>
      {reason ? <p className="reason muted">{reason}</p> : null}
      {p.hasToken ? null : (
        <p className="caption">Killing a follower needs the admin token (Admin section below).</p>
      )}
      <p className="status" role="status" aria-live="polite" data-error={status.error}>
        {status.text}
      </p>
      <details>
        <summary>More faults</summary>
        <div className="field">
          <label htmlFor="pause-secs">
            Pause seconds ({MIN_PAUSE} to {MAX_PAUSE})
          </label>
          <input
            id="pause-secs"
            inputMode="numeric"
            value={pauseSecs}
            onChange={(e) => setPauseSecs(e.target.value)}
          />
          {pauseBad ? (
            <span className="flag">
              Pause is {MIN_PAUSE} to {MAX_PAUSE} seconds.
            </span>
          ) : null}
          <button
            type="button"
            aria-disabled={faultOff || pauseBad || leader === 0}
            onClick={() => {
              if (!(faultOff || pauseBad || leader === 0) && pauseN !== null) {
                void fault("Pause", () => p.client.chaos.pause(leader, pauseN));
              }
            }}
          >
            Pause leader ({pauseN ?? "?"} s)
          </button>
        </div>
        <div className="field">
          <label htmlFor="delay-ms">Delay ms (0 to {MAX_DELAY_MS})</label>
          <input
            id="delay-ms"
            inputMode="numeric"
            value={delayMs}
            onChange={(e) => setDelayMs(e.target.value)}
          />
          {delayBad ? <span className="flag">Delay is at most {MAX_DELAY_MS} ms.</span> : null}
          <label htmlFor="drop-pct">Drop % (0 to {MAX_DROP})</label>
          <input
            id="drop-pct"
            inputMode="numeric"
            value={drop}
            onChange={(e) => setDrop(e.target.value)}
          />
          {dropBad ? <span className="flag">Drop is at most {MAX_DROP} %.</span> : null}
          <button
            type="button"
            aria-disabled={faultOff || delayBad || dropBad}
            onClick={() => {
              if (!(faultOff || delayBad || dropBad) && delayN !== null && dropN !== null) {
                void fault("Delay", () => p.client.chaos.delay(delayN, dropN));
              }
            }}
          >
            Add delay and packet drop
          </button>
        </div>
      </details>
    </section>
  );
}
