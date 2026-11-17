#!/usr/bin/env bash
# Tests for scripts/release.sh (D3). Runs real go builds, never calls aws.
# Every case works in a throwaway git clone holding a snapshot of the CURRENT
# working tree, so the tests see uncommitted changes and never touch the real repo.
set -uo pipefail
. "$(dirname "$0")/release_testlib.sh"

clone1="$work/one/raftkv"; mkdir -p "$work/one"
snapshot "$clone1" || { echo "FAIL snapshot"; exit 1; }
# ---- builds both binaries, sha256 and MANIFEST ----
d1="$work/d1"
rel "$clone1" "$d1" >"$work/rel1.out" 2>&1; rc=$?
v1=$(verdir "$d1")
if [ "$rc" -eq 0 ] && [ -n "$v1" ] && [ -f "$v1/raftkv-server-linux-arm64" ] && [ -f "$v1/gateway-linux-arm64" ] \
   && [ -f "$v1/raftkv-server-linux-arm64.sha256" ] && [ -f "$v1/gateway-linux-arm64.sha256" ] && [ -f "$v1/MANIFEST" ]; then
  pass "builds both binaries and sha256 and MANIFEST"
else
  fail "builds both binaries and sha256 and MANIFEST" "rc=$rc dir=$v1 $(tail -3 "$work/rel1.out" | tr '\n' '|')"
fi

check "sha256 files are sha256sum -c format" bash -c '
  cd "$1" && for f in raftkv-server-linux-arm64 gateway-linux-arm64; do
    want=$(cat "$f.sha256"); got="$(shasum -a 256 "$f" | cut -d" " -f1)  $f"; [ "$want" = "$got" ] || { echo "$want != $got"; exit 1; }; done' _ "$v1"

check "binaries are linux arm64 static" bash -c '
  for f in raftkv-server-linux-arm64 gateway-linux-arm64; do
    o=$(go version -m "$1/$f") || exit 1
    for want in "GOOS=linux" "GOARCH=arm64" "CGO_ENABLED=0" "-trimpath=true"; do
      echo "$o" | grep -q -- "$want" || { echo "$f lacks $want"; exit 1; }; done; done' _ "$v1"

check "-X values equal manifest" bash -c '
  m="$1/MANIFEST"
  ver=$(sed -n "s/^version=//p" "$m"); com=$(sed -n "s/^commit=//p" "$m"); dat=$(sed -n "s/^date=//p" "$m")
  [ -n "$ver" ] && [ -n "$com" ] && [ -n "$dat" ] || { echo "empty manifest fields"; exit 1; }
  for f in raftkv-server-linux-arm64 gateway-linux-arm64; do
    for v in "$ver" "$com" "$dat"; do
      grep -aqF -- "$v" "$1/$f" || { echo "$f lacks $v"; exit 1; }; done; done' _ "$v1"

check "MANIFEST records goos/goarch, go version, dirty=false and both sha256" bash -c '
  m="$1/MANIFEST"
  grep -q "^dirty=false$" "$m" && grep -q "^goos=linux$" "$m" && grep -q "^goarch=arm64$" "$m" && grep -q "^goversion=go" "$m" \
    && grep -q "^sha256.raftkv-server-linux-arm64=[0-9a-f]\{64\}$" "$m" && grep -q "^sha256.gateway-linux-arm64=[0-9a-f]\{64\}$" "$m"' _ "$v1"

check "version is git describe, date is the commit date" bash -c '
  c="$1"; m="$2/MANIFEST"
  want=$(cd "$c" && git describe --tags --always --abbrev=12); wd=$(cd "$c" && TZ=UTC git show -s --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ)
  [ "$(sed -n "s/^version=//p" "$m")" = "$want" ] || { echo "version"; exit 1; }
  [ "$(sed -n "s/^date=//p" "$m")" = "$wd" ] || { echo "date"; exit 1; }
  [ "$(sed -n "s/^commit=//p" "$m")" = "$(cd "$c" && git rev-parse HEAD)" ]' _ "$clone1" "$v1"

# ---- reproducibility ----
d2="$work/d2"
rel "$clone1" "$d2" >/dev/null 2>&1
check "two builds of the same commit are byte-identical" bash -c '
  for f in raftkv-server-linux-arm64 gateway-linux-arm64 MANIFEST; do cmp "$1/$f" "$2/$f" || exit 1; done' _ "$v1" "$(verdir "$d2")"

clone2="$work/two/some/other/path/raftkv"; mkdir -p "$(dirname "$clone2")"
git clone -q "$clone1" "$clone2"
d3="$work/d3"
rel "$clone2" "$d3" >"$work/rel3.out" 2>&1
check "two builds from two different clone paths are byte-identical" bash -c '
  for f in raftkv-server-linux-arm64 gateway-linux-arm64 MANIFEST; do cmp "$1/$f" "$2/$f" || exit 1; done' _ "$v1" "$(verdir "$d3")"

# ---- dirty handling ----
cdirty="$work/dirty"; git clone -q "$clone1" "$cdirty"
echo "// dirty" >> "$cdirty/internal/buildinfo/buildinfo.go"
dd1="$work/dd1"
rel "$cdirty" "$dd1" >"$work/dd1.out" 2>&1; rc=$?
if [ "$rc" -ne 0 ] && grep -qi dirty "$work/dd1.out" && [ -z "$(ls "$dd1" 2>/dev/null)" ]; then pass "dirty tree refuses (and builds nothing)"; else fail "dirty tree refuses" "rc=$rc out=$(ls "$dd1" 2>/dev/null)"; fi

dd2="$work/dd2"
rel "$cdirty" "$dd2" ALLOW_DIRTY=1 >"$work/dd2.out" 2>&1; rc=$?
vd=$(verdir "$dd2")
check "ALLOW_DIRTY=1 gives -dirty and dirty=true" bash -c '
  [ "$1" -eq 0 ] && case "$(basename "$2")" in *-dirty) ;; *) exit 1;; esac &&
  grep -q "^dirty=true$" "$2/MANIFEST" && grep -q "^version=.*-dirty$" "$2/MANIFEST" && grep -q "^commit=.*-dirty$" "$2/MANIFEST"' _ "$rc" "$vd"

cuntr="$work/untr"; git clone -q "$clone1" "$cuntr"
echo "not code" > "$cuntr/review-x.md"
rel "$cuntr" "$work/du1" >"$work/du1.out" 2>&1; rc=$?
if [ "$rc" -eq 0 ]; then pass "untracked review-x.md does not make it dirty"; else fail "untracked review-x.md does not make it dirty" "rc=$rc $(tail -2 "$work/du1.out" | tr '\n' '|')"; fi
printf 'package buildinfo\n' > "$cuntr/internal/buildinfo/x.go"
rel "$cuntr" "$work/du2" >"$work/du2.out" 2>&1; rc=$?
if [ "$rc" -ne 0 ] && grep -qi dirty "$work/du2.out"; then pass "untracked x.go does"; else fail "untracked x.go does" "rc=$rc, want a refusal mentioning dirty: $(tail -2 "$work/du2.out" | tr '\n' '|')"; fi

# ---- fakecluster positive control: a tagged gateway must fail verification ----
tag="$work/tagged"; cp -R "$v1" "$tag"
(cd "$clone1" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags fakecluster -trimpath -buildvcs=false \
   -ldflags "-s -w -buildid= -X github.com/MenaceHecker/raftkv/internal/buildinfo.Version=$(sed -n 's/^version=//p' "$v1/MANIFEST") -X github.com/MenaceHecker/raftkv/internal/buildinfo.Commit=$(sed -n 's/^commit=//p' "$v1/MANIFEST") -X github.com/MenaceHecker/raftkv/internal/buildinfo.Date=$(sed -n 's/^date=//p' "$v1/MANIFEST")" \
   -o "$tag/gateway-linux-arm64" ./cmd/gateway) >/dev/null 2>&1
h=$(shasum -a 256 "$tag/gateway-linux-arm64" | cut -d' ' -f1)
echo "$h  gateway-linux-arm64" > "$tag/gateway-linux-arm64.sha256"
sed -i.bak "s/^sha256.gateway-linux-arm64=.*/sha256.gateway-linux-arm64=$h/" "$tag/MANIFEST"; rm -f "$tag/MANIFEST.bak"
mkdir "$work/tagged-parent" && mv "$tag" "$work/tagged-parent/$(basename "$v1")" && tag="$work/tagged-parent/$(basename "$v1")"
(cd "$clone1" && scripts/verify-release.sh "$v1") >/dev/null 2>&1; goodrc=$?
(cd "$clone1" && scripts/verify-release.sh "$tag") >"$work/tag.out" 2>&1; badrc=$?
if [ "$goodrc" -eq 0 ] && [ "$badrc" -ne 0 ] && grep -qi fakecluster "$work/tag.out"; then pass "tagged gateway (-tags fakecluster) fails verify"; else fail "tagged gateway fails verify" "good=$goodrc bad=$badrc $(tr '\n' '|' <"$work/tag.out")"; fi

# ---- never calls aws ----
stub="$work/stubbin"; mkdir -p "$stub"; log="$work/aws.log"; : > "$log"
printf '#!/bin/sh\necho "$@" >> "%s"\nexit 1\n' "$log" > "$stub/aws"; chmod +x "$stub/aws"
rel "$clone1" "$work/d4" PATH="$stub:$PATH" AWS_STUB_LOG="$log" >"$work/d4.out" 2>&1; rc=$?
if [ "$rc" -eq 0 ] && [ ! -s "$log" ] && [ "$(PATH="$stub:$PATH" command -v aws)" = "$stub/aws" ]; then pass "with a stub aws on PATH the stub log stays empty"; else fail "stub aws log stays empty" "rc=$rc log=$(cat "$log")"; fi
if ! grep -q 'aws' "$here/release.sh" "$here/verify-release.sh"; then pass "release.sh and verify-release.sh never mention aws"; else fail "never mention aws" "$(grep -n aws "$here"/release.sh "$here"/verify-release.sh | head -3)"; fi

nostub="$work/noaws"; mkdir -p "$nostub"
for t in go git make tar shasum sha256sum awk sed grep cat cut tr head sort ls cp mv mkdir rm cmp dirname basename date env bash sh uname wc mktemp strings find xargs readlink pwd tee; do p=$(command -v "$t" 2>/dev/null) && [ -x "$p" ] && ln -sf "$p" "$nostub/$t"; done
gobin=$(dirname "$(command -v go)")
(cd "$clone1" && env -i HOME="$HOME" PATH="$nostub:$gobin" DIST_DIR="$work/d5" make release) >"$work/d5.out" 2>&1; rc=$?
if [ "$rc" -eq 0 ] && ! PATH="$nostub" command -v aws >/dev/null 2>&1; then pass "make release works with no aws on PATH"; else fail "make release works with no aws on PATH" "rc=$rc $(tail -3 "$work/d5.out" | tr '\n' '|')"; fi

echo
if [ "$fails" -ne 0 ]; then echo "$fails release test(s) failed"; exit 1; fi
echo "all release tests passed"
