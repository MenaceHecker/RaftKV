#!/usr/bin/env bash
# Tests for scripts/release-upload.sh (D3). A STUB aws is first on PATH and logs its argv;
# the real aws is never reachable. Every refusal must exit non-zero with its own reason
# AND leave the stub log empty (no aws call at all).
set -uo pipefail
. "$(dirname "$0")/release_testlib.sh"

clone="$work/one/raftkv"; mkdir -p "$work/one"
snapshot "$clone" || { echo "FAIL snapshot"; exit 1; }
rel "$clone" "$work/good" >"$work/good.out" 2>&1
good=$(verdir "$work/good")
if [ -z "$good" ]; then fail "build a good dist dir with release.sh" "$(tail -3 "$work/good.out" | tr '\n' '|')"; good="$work/good/none"; fi
ver=$(basename "$good")

stub="$work/stubbin"; mkdir -p "$stub"; log="$work/aws.log"
# The stub logs argv. head-object answers like S3 for a missing key (exit 254, 404) unless
# AWS_STUB_EXISTS=1 (exit 0). Everything else succeeds.
cat > "$stub/aws" <<'STUB'
#!/bin/sh
echo "$@" >> "$AWS_STUB_LOG"
case "$*" in
  *head-object*)
    if [ "${AWS_STUB_EXISTS:-}" = 1 ]; then echo '{}'; exit 0; fi
    if [ "${AWS_STUB_EXISTS:-}" = denied ]; then echo "An error occurred (403) when calling the HeadObject operation: Forbidden" >&2; exit 254; fi
    echo "An error occurred (404) when calling the HeadObject operation: Not Found" >&2; exit 254 ;;
esac
exit 0
STUB
chmod +x "$stub/aws"
[ "$(PATH="$stub:$PATH" command -v aws)" = "$stub/aws" ] || { echo "FAIL the stub is not the first aws on PATH"; exit 1; }

# up <dir> [VAR=val...]: run the upload script from a clean environment plus the given vars.
up() {
  local d="$1"; shift
  : > "$log"
  (cd "$clone" && env -u CI -u GITHUB_ACTIONS -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY -u AWS_SESSION_TOKEN \
     PATH="$stub:$PATH" AWS_STUB_LOG="$log" "$@" scripts/release-upload.sh "$d") 2>&1
}
ok_env=(RELEASE_AWS_PROFILE=prof RELEASE_BUCKET=bkt)

# refuse <name> <dir> <reason regex> [VAR=val...]
refuse() {
  local name="$1" d="$2" re="$3" out rc; shift 3
  out=$(up "$d" "$@"); rc=$?
  if [ "$rc" -ne 0 ] && echo "$out" | grep -Eqi -- "$re" && [ ! -s "$log" ]; then pass "$name"
  else fail "$name" "rc=$rc want /$re/ and an empty aws log; out=$(echo "$out" | tr '\n' '|' | cut -c1-250) log=$(tr '\n' '|' <"$log")"; fi
}

copy_good() { mkdir -p "$work/$1"; cp -R "$good" "$work/$1/$ver"; echo "$work/$1/$ver"; }

refuse "refuses with CI=true"                "$good" 'CI'                 "${ok_env[@]}" CI=true
refuse "refuses with GITHUB_ACTIONS set"     "$good" 'CI|GITHUB_ACTIONS'  "${ok_env[@]}" GITHUB_ACTIONS=true
refuse "refuses without RELEASE_AWS_PROFILE" "$good" 'RELEASE_AWS_PROFILE' RELEASE_BUCKET=bkt
refuse "refuses without RELEASE_BUCKET"      "$good" 'RELEASE_BUCKET'     RELEASE_AWS_PROFILE=prof
refuse "refuses with AWS_ACCESS_KEY_ID set"  "$good" 'AWS_ACCESS_KEY_ID|long-lived|keys' "${ok_env[@]}" AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXXXXX
refuse "refuses with AWS_SECRET_ACCESS_KEY set" "$good" 'AWS_SECRET_ACCESS_KEY|long-lived|keys' "${ok_env[@]}" AWS_SECRET_ACCESS_KEY=x
refuse "refuses with AWS_SESSION_TOKEN set"  "$good" 'AWS_SESSION_TOKEN|long-lived|keys' "${ok_env[@]}" AWS_SESSION_TOKEN=x

d=$(copy_good dirty); sed -i.bak 's/^dirty=.*/dirty=true/' "$d/MANIFEST"; rm -f "$d/MANIFEST.bak"
refuse "refuses a dirty manifest"            "$d" 'dirty'                 "${ok_env[@]}"

d=$(copy_good tamper); printf 'x' >> "$d/gateway-linux-arm64"
refuse "refuses a tampered binary (sha256)"  "$d" 'sha256|verify'         "${ok_env[@]}"

d=$(copy_good badsha); echo "$(printf '0%.0s' $(seq 64))  raftkv-server-linux-arm64" > "$d/raftkv-server-linux-arm64.sha256"
refuse "refuses a wrong .sha256 file"        "$d" 'sha256|verify'         "${ok_env[@]}"

# The one refusal that must talk to aws: it asks head-object, finds the key, and stops. Only
# head-object calls may be in the log, never a cp.
out=$(up "$good" "${ok_env[@]}" AWS_STUB_EXISTS=1); rc=$?
if [ "$rc" -ne 0 ] && echo "$out" | grep -Eqi 'exist' && [ -s "$log" ] && ! grep -qv 'head-object' "$log"; then pass "refuses when the object already exists (head-object only, no cp)"
else fail "refuses when the object already exists" "rc=$rc out=$(echo "$out" | tr '\n' '|' | cut -c1-250) log=$(tr '\n' '|' <"$log")"; fi

out=$(up "$good" "${ok_env[@]}" AWS_STUB_EXISTS=denied); rc=$?
if [ "$rc" -ne 0 ] && echo "$out" | grep -Eqi 'could not tell' && ! grep -q ' cp ' "$log"; then pass "refuses when head-object fails for any reason but Not Found"
else fail "head-object denied" "rc=$rc out=$(echo "$out" | tr '\n' '|' | cut -c1-250) log=$(tr '\n' '|' <"$log")"; fi

refuse "refuses without a directory argument" "" 'usage|RELEASE_DIR|directory' "${ok_env[@]}"
refuse "refuses a directory that does not exist" "$work/nope" 'missing|not a directory|no such' "${ok_env[@]}"

# ---- success path ----
out=$(up "$good" "${ok_env[@]}"); rc=$?
if [ "$rc" -eq 0 ]; then pass "uploads a good dir"; else fail "uploads a good dir" "rc=$rc $(echo "$out" | tr '\n' '|' | cut -c1-300)"; fi
check "every aws call uses --profile prof and --region us-east-1" bash -c '
  [ -s "$1" ] || { echo "empty log"; exit 1; }
  while IFS= read -r l; do case "$l" in *"--profile prof"*"--region us-east-1"*) ;; *) echo "bad line: $l"; exit 1;; esac; done < "$1"' _ "$log"
cps=$(grep ' s3 cp ' "$log" | sed 's/.* s3 cp //')
want=$(printf '%s\n' \
  "$good/raftkv-server-linux-arm64 s3://bkt/artifacts/raftkvd/$ver/raftkv-server-linux-arm64" \
  "$good/raftkv-server-linux-arm64.sha256 s3://bkt/artifacts/raftkvd/$ver/raftkv-server-linux-arm64.sha256" \
  "$good/MANIFEST s3://bkt/artifacts/raftkvd/$ver/MANIFEST" \
  "$good/gateway-linux-arm64 s3://bkt/artifacts/gateway/$ver/gateway-linux-arm64" \
  "$good/gateway-linux-arm64.sha256 s3://bkt/artifacts/gateway/$ver/gateway-linux-arm64.sha256" \
  "$good/MANIFEST s3://bkt/artifacts/gateway/$ver/MANIFEST")
if [ "$(echo "$cps" | sort)" = "$(echo "$want" | sort)" ] && [ "$(echo "$cps" | wc -l | tr -d ' ')" = 6 ]; then pass "copies exactly the 6 expected keys"; else fail "copies exactly the 6 expected keys" "got: $(echo "$cps" | tr '\n' '|')"; fi
check "MANIFEST is copied last under each prefix" bash -c '
  cps=$(grep " s3 cp " "$1")
  for p in raftkvd gateway; do
    last=$(echo "$cps" | grep "artifacts/$p/" | tail -1)
    case "$last" in *"/MANIFEST s3://"*"/MANIFEST") ;; *) echo "$p: last is $last"; exit 1;; esac
  done' _ "$log"
if ! grep -Eq -- 'delete|--acl|sync|rm |mv ' "$log"; then pass "no delete, --acl, sync, rm or mv"; else fail "no delete/acl/sync" "$(grep -E -- 'delete|--acl|sync|rm |mv ' "$log")"; fi

# ---- make release-upload ----
(cd "$clone" && env -u RELEASE_DIR PATH="$stub:$PATH" AWS_STUB_LOG="$work/mk.log" make release-upload) >"$work/mk.out" 2>&1; rc=$?
if [ "$rc" -ne 0 ] && grep -q RELEASE_DIR "$work/mk.out" && [ ! -s "$work/mk.log" ]; then pass "make release-upload with no RELEASE_DIR fails (no aws call)"; else fail "make release-upload with no vars" "rc=$rc $(tr '\n' '|' <"$work/mk.out")"; fi
(cd "$clone" && env -u CI PATH="$stub:$PATH" AWS_STUB_LOG="$work/mk2.log" make release-upload RELEASE_DIR="$good") >"$work/mk2.out" 2>&1; rc=$?
if [ "$rc" -ne 0 ] && grep -q RELEASE_AWS_PROFILE "$work/mk2.out" && [ ! -s "$work/mk2.log" ]; then pass "make release-upload without profile and bucket fails (no aws call)"; else fail "make release-upload no profile" "rc=$rc $(tr '\n' '|' <"$work/mk2.out")"; fi

echo
if [ "$fails" -ne 0 ]; then echo "$fails release-upload test(s) failed"; exit 1; fi
echo "all release-upload tests passed"
