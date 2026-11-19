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
tmpjson=$(mktemp "${TMPDIR:-/tmp}/policy-selftest.XXXXXX")
echo '{not json' > "$tmpjson"
if ./check.sh "$tmpjson" >/dev/null 2>&1; then echo "FAIL invalid json exited 0"; fail=1; else echo "ok   invalid json refused"; fi
rm -f "$tmpjson"

# --hcl mode: forbidden resource types and literal wildcards in a copy of testdata/hcl-bad.tf.txt
# must fail; the real infra tree must pass.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/policy-selftest-hcl.XXXXXX")
cp testdata/hcl-bad.tf.txt "$tmp/bad.tf"
if ./check.sh --hcl "$tmp" >/dev/null 2>&1; then echo "FAIL --hcl accepted testdata/hcl-bad.tf.txt"; fail=1; else echo "ok   --hcl rejects testdata/hcl-bad.tf.txt"; fi
rm -r "$tmp"
if out=$(./check.sh --hcl .. 2>&1); then echo "ok   --hcl accepts the infra tree"; else echo "FAIL --hcl rejected the infra tree:"; echo "$out"; fail=1; fi

exit "$fail"
