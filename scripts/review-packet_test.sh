#!/usr/bin/env bash
# Test for scripts/review-packet.sh. Builds a throwaway git repo with a fake task card.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
fails=0
check() { if eval "$2"; then echo "ok   $1"; else echo "FAIL $1"; fails=$((fails+1)); fi; }

cd "$dir" || exit 1
git init -q -b main . && git config user.email t@t && git config user.name t
mkdir -p docs/tasks scripts internal/raft internal/node
cp "$here/guard-tests.sh" scripts/guard-tests.sh
echo "FAKE-TASK-CARD" > docs/tasks/T9.9.md
echo "FAKE-INVARIANTS" > docs/INVARIANTS.md
echo "FAKE-RUN-REPORT" > RUN_REPORT.md
printf 'package raft\n\nfunc Old() {}\n' > internal/raft/core.go
git add . && git commit -q -m base && git checkout -q -b feature
printf 'package raft\n\nfunc Old() {}\n\nfunc CORE_MARKER() {}\n' > internal/raft/core.go
printf 'package raft\n\nfunc TEST_MARKER() {}\n' > internal/raft/core_test.go
printf 'package node\n\nfunc NODE_MARKER() {}\n' > internal/node/n.go
git add . && git commit -q -m change

out=$("$here/review-packet.sh" T9.9 main 2>&1); rc=$?
check "exits 0"                                   '[ "$rc" -eq 0 ]'
check "prints output file name"                   '[ "$out" = "review-packet-T9.9.md" ]'
f=review-packet-T9.9.md
check "output file exists"                        '[ -f "$f" ]'
for s in "## Task card" "## Invariants" "## Run report" "## guard-tests" "## Diff" "## Full text of changed core files"; do
  check "has section: $s"                         'grep -qF "$s" "$f"'
done
check "contains card, invariants, report text"    'grep -q FAKE-TASK-CARD "$f" && grep -q FAKE-INVARIANTS "$f" && grep -q FAKE-RUN-REPORT "$f"'
check "full text of changed internal/raft file"   'sed -n "/## Full text/,\$p" "$f" | grep -q "func Old"'
check "core file section lists core.go"           'sed -n "/## Full text/,\$p" "$f" | grep -q "### internal/raft/core.go"'
check "no _test.go file in full-text section"     '[ -f "$f" ] && ! sed -n "/## Full text/,\$p" "$f" | grep -q "core_test.go"'
check "no internal/node file in full-text section" '[ -f "$f" ] && ! sed -n "/## Full text/,\$p" "$f" | grep -q "### internal/node"'

[ "$fails" -eq 0 ] || { echo "$fails failure(s)"; exit 1; }
echo "all review-packet cases passed"
