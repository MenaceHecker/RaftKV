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
  rc=0
  out=$(./check.sh "$f" 2>&1) || rc=$?
  if [ "$rc" -eq 0 ]; then
    echo "FAIL $f: check.sh exited 0, expected 1"; fail=1
  elif [ "$rc" -ne 1 ]; then
    echo "FAIL $f: check.sh exited $rc, expected exactly 1"; fail=1
  else
    # A fixture may name the violation it must trigger ("_expect"): it has to fail for that
    # reason, not for another one.
    want=$(jq -r '._expect // empty' "$f")
    if [ -n "$want" ] && ! grep -qF -- "$want" <<<"$out"; then
      echo "FAIL $f: exited 1 but not for the expected reason '$want'; got:"; echo "$out"; fail=1
    else
      echo "ok   $f"
    fi
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

# --hcl mode: every testdata/hcl-bad*.tf.txt (copied to a .tf in an empty directory) must fail, for
# the reason named in its first line ("# expect: <text>", or none for the oldest one);
# hcl-good*.tf.txt and the real infra tree must pass.
for f in testdata/hcl-bad*.tf.txt; do
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/policy-selftest-hcl.XXXXXX")
  cp "$f" "$tmp/bad.tf"
  rc=0
  out=$(./check.sh --hcl "$tmp" 2>&1) || rc=$?
  want=$(sed -n '1s/^# expect: //p' "$f")
  if [ "$rc" -ne 1 ]; then
    echo "FAIL --hcl on $f exited $rc, expected 1"; fail=1
  elif [ -n "$want" ] && ! grep -qF -- "$want" <<<"$out"; then
    echo "FAIL --hcl on $f exited 1 but not for the expected reason '$want'; got:"; echo "$out"; fail=1
  else
    echo "ok   --hcl rejects $f"
  fi
  rm -r "$tmp"
done
for f in testdata/hcl-good*.tf.txt; do
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/policy-selftest-hcl.XXXXXX")
  cp "$f" "$tmp/good.tf"
  if out=$(./check.sh --hcl "$tmp" 2>&1); then echo "ok   --hcl accepts $f"; else echo "FAIL --hcl rejected $f:"; echo "$out"; fail=1; fi
  rm -r "$tmp"
done
if out=$(./check.sh --hcl .. 2>&1); then echo "ok   --hcl accepts the infra tree"; else echo "FAIL --hcl rejected the infra tree:"; echo "$out"; fail=1; fi

# A *.tfvars or *.tfstate that git tracks is a violation (they hold values and secrets).
for name in terraform.tfvars prod.auto.tfvars terraform.tfstate; do
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/policy-selftest-git.XXXXXX")
  ( cd "$tmp" && git init -q . && : > main.tf && : > "$name" && git add -f main.tf "$name" ) >/dev/null 2>&1
  rc=0
  out=$(./check.sh --hcl "$tmp" 2>&1) || rc=$?
  if [ "$rc" -eq 1 ] && grep -qF 'tracked by git' <<<"$out"; then echo "ok   --hcl rejects tracked $name"; else echo "FAIL --hcl tracked $name: exit $rc: $out"; fail=1; fi
  rm -rf "$tmp"
done

exit "$fail"
