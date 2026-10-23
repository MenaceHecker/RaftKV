#!/usr/bin/env bash
set -euo pipefail
task="$1"; base="${2:-origin/main}"
out="review-packet-$task.md"
{
  echo "# Review packet: $task"; echo; echo "## Task card"; cat "docs/tasks/$task.md"
  echo; echo "## Invariants"; cat docs/INVARIANTS.md
  echo; echo "## Run report"; cat RUN_REPORT.md
  echo; echo "## guard-tests"; scripts/guard-tests.sh "$base" 2>&1 || true
  echo; echo "## Diff"; echo '```diff'; git diff "$base"...HEAD; echo '```'
  echo; echo "## Full text of changed core files"
  for f in $(git diff --name-only "$base"...HEAD | grep -E '^internal/(raft|storage)/.*\.go$' | grep -v _test.go || true); do
    echo "### $f"; echo '```go'; cat "$f"; echo '```'
  done
} > "$out"
echo "$out"
