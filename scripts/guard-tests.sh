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
mult='(time\.(Second|Millisecond)[[:space:]]*\*[[:space:]]*[0-9]{3,}|[0-9]{3,}[[:space:]]*\*[[:space:]]*time\.(Second|Millisecond))'
# Fake-clock inputs cost no real time. A line is exempt only when it ends in the marker comment
# `// guard:fakeclock` and contains no real wait (time.Sleep/After/Tick/NewTimer/NewTicker,
# context.WithTimeout/WithDeadline, Eventually, WaitFor).
marker='//[[:space:]]*guard:fakeclock[[:space:]]*$'
real='time\.(Sleep|After|AfterFunc|Tick|NewTimer|NewTicker)\(|context\.With(Timeout|Deadline)\(|Eventually|WaitFor'
big=$(echo "$diff" | grep -E "^\+.*$mult" || true)
if [ -n "$big" ]; then
  flagged=""
  while IFS= read -r line; do
    if ! echo "$line" | grep -qE "$real" && echo "$line" | grep -qE "$marker"; then continue; fi
    flagged+="$line"$'\n'
  done <<< "$big"
  if [ -n "$flagged" ]; then printf '%s' "$flagged"; echo "WARN: large timeout added"; bad=1; fi
fi
exit $bad
