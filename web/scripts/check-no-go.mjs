// Fails (exit 1) when web/node_modules holds anything the repo-wide Go tooling
// would pick up: a *.go file, go.mod, go.sum or a nested .git entry. gofmt -l .,
// go vet ./..., go list ./... and the README count walks all descend into it.
// Usage: node scripts/check-no-go.mjs [dir]   (default: ../node_modules next to this script)
import { readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function findGoArtifacts(root) {
  const hits = [];
  const walk = (dir) => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const p = join(dir, e.name);
      if (
        e.name === ".git" ||
        e.name === "go.mod" ||
        e.name === "go.sum" ||
        e.name.endsWith(".go")
      ) {
        hits.push(p);
      }
      if (e.isDirectory() && !e.isSymbolicLink() && e.name !== ".git") walk(p);
    }
  };
  walk(root);
  return hits;
}

const here = dirname(fileURLToPath(import.meta.url));
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const root = resolve(process.argv[2] ?? join(here, "..", "node_modules"));
  const hits = findGoArtifacts(root);
  if (hits.length > 0) {
    process.stderr.write(`Go files under ${root} (the Go tooling would see them):\n`);
    for (const h of hits) process.stderr.write(`  ${h}\n`);
    process.exit(1);
  }
  process.stdout.write(`no Go files under ${root}\n`);
}
