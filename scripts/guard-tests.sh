#!/usr/bin/env bash
# Flags test-weakening patterns in the diff vs main. Exit 1 if found.
set -euo pipefail
base="${1:-origin/main}"
diff=$(git diff "$base"...HEAD -U0 -- '*_test.go' || true)
bad=0
if echo "$diff" | grep -E '^\+.*\bt\.Skip(Now)?\(' ; then echo "ERROR: added t.Skip"; bad=1; fi
if echo "$diff" | grep -E '^-\s*(require|assert)\.' | head -1 >/dev/null; then
  removed=$(echo "$diff" | grep -cE '^-\s*(require|assert)\.|^-.*t\.(Fatal|Error)f?\(' || true)
  added=$(echo "$diff" | grep -cE '^\+\s*(require|assert)\.|^\+.*t\.(Fatal|Error)f?\(' || true)
  if [ "$removed" -gt "$added" ]; then echo "WARN: net assertions removed ($removed removed, $added added)"; bad=1; fi
fi
# Both orders: `time.Millisecond * 5000` and `5000 * time.Millisecond`.
if echo "$diff" | grep -E '^\+.*(time\.(Second|Millisecond)\s*\*\s*[0-9]{3,}|[0-9]{3,}\s*\*\s*time\.(Second|Millisecond))' ; then echo "WARN: large timeout added"; bad=1; fi
exit $bad
