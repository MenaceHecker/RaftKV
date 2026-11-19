#!/usr/bin/env bash
# Self-test for check.sh: every testdata/bad-*.json must make it exit 1, and
# testdata/good.json must make it exit 0. Run from anywhere.
set -uo pipefail
cd "$(dirname "$0")"

fail=0
shopt -s nullglob
bad=(testdata/bad-*.json)
[ "${#bad[@]}" -gt 0 ] || { echo "FAIL no testdata/bad-*.json fixtures found"; exit 1; }
[ -f testdata/good.json ] || { echo "FAIL testdata/good.json missing"; exit 1; }

for f in "${bad[@]}"; do
  if ./check.sh "$f" >/dev/null 2>&1; then
    echo "FAIL $f: check.sh exited 0, expected 1"; fail=1
  else
    rc=$?
    if [ "$rc" -ne 1 ]; then echo "FAIL $f: check.sh exited $rc, expected exactly 1"; fail=1; else echo "ok   $f"; fi
  fi
done

if out=$(./check.sh testdata/good.json 2>&1); then
  echo "ok   testdata/good.json"
else
  echo "FAIL testdata/good.json: check.sh rejected the good plan:"; echo "$out"; fail=1
fi

# A missing file or invalid JSON must not read as "clean".
if ./check.sh testdata/does-not-exist.json >/dev/null 2>&1; then echo "FAIL missing file exited 0"; fail=1; else echo "ok   missing file refused"; fi
echo '{not json' > "${TMPDIR:-/tmp}/policy-selftest-$$.json"
if ./check.sh "${TMPDIR:-/tmp}/policy-selftest-$$.json" >/dev/null 2>&1; then echo "FAIL invalid json exited 0"; fail=1; else echo "ok   invalid json refused"; fi

exit "$fail"
