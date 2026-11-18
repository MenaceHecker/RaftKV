// Capture a session for the dashboard's replay mode (public/recording.jsonl).
//   node scripts/record-session.mjs --gateway https://api.example.com --out recording.jsonl --seconds 120
// PUBLIC access only: this script never sends an Authorization header, and it refuses to write
// any line containing the text "Bearer". No dependencies.
import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const FORBIDDEN = "Bearer";

function assertClean(lines) {
  for (const l of lines) {
    if (l.includes(FORBIDDEN)) {
      throw new Error(`refusing to write a recording that contains "${FORBIDDEN}"`);
    }
  }
}

/** Parse a server-sent-event stream; calls onEvent({id,name,data}) for each complete event. */
async function readEvents(res, onEvent) {
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    buf += dec.decode(value, { stream: true });
    let i = buf.indexOf("\n\n");
    while (i !== -1) {
      const block = buf.slice(0, i);
      buf = buf.slice(i + 2);
      let id = null;
      let name = "message";
      const data = [];
      for (const line of block.split("\n")) {
        if (line.startsWith(":")) continue;
        if (line.startsWith("id:")) id = Number(line.slice(3).trim());
        else if (line.startsWith("event:")) name = line.slice(6).trim();
        else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
      }
      if (data.length > 0 && id !== null && Number.isFinite(id)) {
        onEvent({ id, name, data: data.join("\n") });
      }
      i = buf.indexOf("\n\n");
    }
  }
}

/**
 * Records `seconds` of a gateway session and returns the JSONL lines (header first).
 * Rejects if any line would contain "Bearer".
 */
export async function recordSession({ gateway, seconds, fetch, sleep, now }) {
  const base = gateway.replace(/\/+$/, "");
  const start = now();
  const t = () => now() - start;
  const lines = [];
  const body = [];
  const header = {
    v: 1,
    kind: "header",
    recordedAt: new Date(start).toISOString(),
    durationMs: seconds * 1000,
    synthetic: false,
  };

  const getJson = async (path) => {
    const res = await fetch(`${base}${path}`, { headers: { Accept: "application/json" } });
    if (!res.ok) return null;
    return res.json();
  };

  const ctl = new AbortController();
  const events = (async () => {
    try {
      const res = await fetch(`${base}/v1/events`, {
        headers: { Accept: "text/event-stream" },
        signal: ctl.signal,
      });
      if (!res.ok || !res.body) return;
      await readEvents(res, async (e) => {
        let data;
        try {
          data = JSON.parse(e.data);
        } catch {
          return; // not JSON: not a gateway event worth keeping
        }
        body.push({ t: t(), kind: "event", id: e.id, name: e.name, data });
        if (e.name === "verify" && data && data.phase === "done") {
          const report = await getJson("/v1/verify/last");
          if (report && (report.status === "done" || report.status === "aborted")) {
            body.push({ t: t(), kind: "verify", data: report });
          }
        }
      });
    } catch {
      // aborted at the end, or the stream dropped: keep what we have
    }
  })();

  while (t() < seconds * 1000) {
    try {
      const doc = await getJson("/v1/cluster");
      if (doc) body.push({ t: t(), kind: "cluster", data: doc });
    } catch {
      // a missed poll is a gap in the recording, not an error
    }
    await sleep(1000);
  }
  ctl.abort();
  await events;

  body.sort((a, b) => a.t - b.t);
  lines.push(JSON.stringify(header), ...body.map((l) => JSON.stringify(l)));
  assertClean(lines);
  return lines;
}

export async function writeRecording(path, lines) {
  assertClean(lines);
  await writeFile(path, `${lines.join("\n")}\n`, "utf8");
}

function arg(name) {
  const i = process.argv.indexOf(`--${name}`);
  return i === -1 ? undefined : process.argv[i + 1];
}

if (process.argv[1] && process.argv[1] === fileURLToPath(import.meta.url)) {
  const gateway = arg("gateway");
  const out = arg("out");
  const seconds = Number(arg("seconds") ?? "120");
  if (!gateway || !out || !Number.isFinite(seconds) || seconds <= 0) {
    process.stderr.write("usage: record-session.mjs --gateway URL --out FILE [--seconds N]\n");
    process.exit(2);
  }
  const lines = await recordSession({
    gateway,
    seconds,
    fetch: globalThis.fetch,
    sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
    now: () => Date.now(),
  });
  await writeRecording(out, lines);
  process.stdout.write(`wrote ${lines.length} lines to ${out}\n`);
}
