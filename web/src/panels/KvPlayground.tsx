import { useRef, useState } from "react";
import type { Client } from "../api/client";
import { type ApiFailure, CAS_DETAIL } from "../api/types";
import type { Mode } from "../state/store";

export interface KvProps {
  client: Client;
  mode: Mode;
  verifying: boolean;
}

const MAX_KEY = 64;
const MAX_VALUE = 1024;
const bytes = (s: string) => new TextEncoder().encode(s).length;

interface Result {
  text: string;
  error: boolean;
}

function failText(op: string, f: ApiFailure): string {
  if (f.outcome === "unknown") return `${op}: may or may not have been applied; Get to check.`;
  if (f.outcome === "not applied") return `${op}: the cluster refused it; not applied.`;
  return `${op}: ${f.error}`;
}

/** Panel 5. Never persists keys or values. */
export function KvPlayground(p: KvProps) {
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [result, setResult] = useState<Result>({ text: "", error: false });
  const [latency, setLatency] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);

  const keyBytes = bytes(key);
  const valueBytes = bytes(value);

  let readReason: string | null = null;
  if (p.mode === "replay") readReason = "replay is read-only";
  else if (keyBytes > MAX_KEY) readReason = `key is longer than ${MAX_KEY} bytes`;
  else if (key.includes("/")) readReason = "key must not contain /";

  let writeReason: string | null = readReason;
  if (p.mode !== "replay") {
    if (p.verifying) writeReason = "verification in progress: public writes are locked";
    else if (!readReason && valueBytes > MAX_VALUE)
      writeReason = `value is longer than ${MAX_VALUE} bytes`;
  }

  async function run(
    op: string,
    off: boolean,
    call: () => Promise<{ ok: true; data: unknown } | ApiFailure>,
    show: (data: unknown) => string,
  ) {
    if (off || busyRef.current) return;
    if (key === "") {
      setResult({ text: "Enter a key first.", error: true });
      return;
    }
    busyRef.current = true;
    setBusy(true);
    const t0 = performance.now();
    try {
      const r = await call();
      setLatency(Math.round(performance.now() - t0));
      setResult(
        r.ok ? { text: show(r.data), error: false } : { text: failText(op, r), error: true },
      );
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  }

  const readOff = readReason !== null || busy;
  const writeOff = writeReason !== null || busy;
  const shown = writeReason ?? readReason;

  return (
    <section className="panel" aria-labelledby="h-kv">
      <h2 id="h-kv">KV playground</h2>
      <div className="field">
        <label htmlFor="kv-key">Key</label>
        <input
          id="kv-key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          autoComplete="off"
        />
        <span className="caption">{`${keyBytes} / ${MAX_KEY} bytes`}</span>
      </div>
      <div className="field">
        <label htmlFor="kv-value">Value</label>
        <textarea id="kv-value" rows={3} value={value} onChange={(e) => setValue(e.target.value)} />
        <span className="caption">{`${valueBytes} / ${MAX_VALUE} bytes`}</span>
      </div>
      <div className="btn-row">
        <button
          type="button"
          aria-disabled={writeOff}
          onClick={() =>
            void run(
              "Put",
              writeOff,
              () => p.client.kvPut(key, value),
              () => `Put ok: key "${key}"`,
            )
          }
        >
          Put
        </button>
        <button
          type="button"
          aria-disabled={readOff}
          onClick={() =>
            void run(
              "Get",
              readOff,
              () => p.client.kvGet(key),
              (d) => `Get ok: value ${JSON.stringify((d as { value: string }).value)}`,
            )
          }
        >
          Get
        </button>
        <button
          type="button"
          aria-disabled={writeOff}
          onClick={() =>
            void run(
              "Delete",
              writeOff,
              () => p.client.kvDelete(key),
              () => `Delete ok: key "${key}"`,
            )
          }
        >
          Delete
        </button>
        <button type="button" aria-disabled="true" aria-describedby="cas-why">
          Compare-and-swap
        </button>
      </div>
      <p className="caption" id="cas-why">
        {CAS_DETAIL}
      </p>
      {shown ? <p className="reason muted">{shown}</p> : null}
      <p className="status" role="status" aria-live="polite" data-error={result.error}>
        {result.text}
      </p>
      {latency === null ? null : <p className="caption">{`${latency} ms from your browser`}</p>}
    </section>
  );
}
