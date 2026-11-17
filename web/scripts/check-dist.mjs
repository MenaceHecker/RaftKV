// Fails (exit 1) if the production bundle contains anything that looks like a secret.
// The only allowed mention of "Bearer" is the single header template in the API client.
// Usage: node scripts/check-dist.mjs [dir]   (default: ../dist next to this script)
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const PATTERNS = [
  [/AKIA[0-9A-Z]{16}/, "an AWS access key id"],
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----/, "a private key"],
  [/ghp_[A-Za-z0-9]{30,}/, "a GitHub token"],
  [/admin[-_.]?token[-_.]?file/i, "an admin token file name"],
  [/sk-[A-Za-z0-9]{32,}/, "an API key"],
];

function files(dir, out = []) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) files(p, out);
    else out.push(p);
  }
  return out;
}

export function scanDist(dir) {
  const problems = [];
  let bearer = 0;
  for (const f of files(dir)) {
    if (f.endsWith(".jsonl")) continue; // the recording is data; its own test rejects Bearer
    const text = readFileSync(f, "utf8");
    bearer += text.split("Bearer").length - 1;
    for (const [re, what] of PATTERNS) if (re.test(text)) problems.push(`${f}: looks like ${what}`);
  }
  if (bearer > 1)
    problems.push(`"Bearer" appears ${bearer} times; only the client's header template may use it`);
  return problems;
}

const here = dirname(fileURLToPath(import.meta.url));
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const dir = resolve(process.argv[2] ?? join(here, "..", "dist"));
  const problems = scanDist(dir);
  if (problems.length > 0) {
    for (const p of problems) process.stderr.write(`${p}\n`);
    process.exit(1);
  }
  process.stdout.write(`no secret-looking strings in ${dir}\n`);
}
