#!/usr/bin/env bash
# Tests for scripts/verify-release.sh (D3). Builds one good dist dir with release.sh
# (real go build, no aws), then breaks copies of it one way at a time. Each negative
# case must fail with its own reason, so a stub that always fails cannot pass.
set -uo pipefail
. "$(dirname "$0")/release_testlib.sh"

clone="$work/one/raftkv"; mkdir -p "$work/one"
snapshot "$clone" || { echo "FAIL snapshot"; exit 1; }
rel "$clone" "$work/good" >"$work/good.out" 2>&1
good=$(verdir "$work/good")
if [ -z "$good" ]; then fail "build a good dist dir with release.sh" "$(tail -3 "$work/good.out" | tr '\n' '|')"; good="$work/good/none"; fi
ver=$(basename "$good")
ldx="-s -w -buildid= -X github.com/MenaceHecker/raftkv/internal/buildinfo.Version=$ver -X github.com/MenaceHecker/raftkv/internal/buildinfo.Commit=$(sed -n 's/^commit=//p' "$good/MANIFEST") -X github.com/MenaceHecker/raftkv/internal/buildinfo.Date=$(sed -n 's/^date=//p' "$good/MANIFEST")"

verify() { (cd "$clone" && scripts/verify-release.sh "$1"); }

# copy_good <name>: a fresh copy of the good dir under a dir named like the version.
copy_good() { mkdir -p "$work/$1"; cp -R "$good" "$work/$1/$ver"; echo "$work/$1/$ver"; }
rehash() { # rehash <dir> <file>: make .sha256 and MANIFEST agree with the file on disk
  local h; h=$(shasum -a 256 "$1/$2" | cut -d' ' -f1)
  echo "$h  $2" > "$1/$2.sha256"
  sed -i.bak "s/^sha256.$2=.*/sha256.$2=$h/" "$1/MANIFEST"; rm -f "$1/MANIFEST.bak"
}

# expect_fail <name> <dir> <reason regex>
expect_fail() {
  local out rc
  out=$(verify "$2" 2>&1); rc=$?
  if [ "$rc" -ne 0 ] && echo "$out" | grep -Eqi -- "$3"; then pass "$1"; else fail "$1" "rc=$rc want /$3/ got: $(echo "$out" | tr '\n' '|' | cut -c1-300)"; fi
}

out=$(verify "$good" 2>&1); rc=$?
if [ "$rc" -eq 0 ]; then pass "passes on a good dir"; else fail "passes on a good dir" "rc=$rc $(echo "$out" | tr '\n' '|')"; fi

d=$(copy_good flip)
printf '\xff' | dd of="$d/gateway-linux-arm64" bs=1 seek=100 conv=notrunc 2>/dev/null
expect_fail "fails on a flipped byte" "$d" 'sha256'

d=$(copy_good wrongsha)
echo "0000000000000000000000000000000000000000000000000000000000000000  raftkv-server-linux-arm64" > "$d/raftkv-server-linux-arm64.sha256"
expect_fail "fails on a wrong .sha256 file" "$d" 'sha256'

d=$(copy_good nomanifest); rm "$d/MANIFEST"
expect_fail "fails on a missing MANIFEST" "$d" 'missing.*MANIFEST|MANIFEST.*missing'

d=$(copy_good nobin); rm "$d/raftkv-server-linux-arm64"
expect_fail "fails on a missing binary" "$d" 'missing.*raftkv-server-linux-arm64'

d=$(copy_good manifestsha)
sed -i.bak 's/^sha256.gateway-linux-arm64=.*/sha256.gateway-linux-arm64=1111111111111111111111111111111111111111111111111111111111111111/' "$d/MANIFEST"; rm -f "$d/MANIFEST.bak"
expect_fail "fails when the manifest sha256 disagrees" "$d" 'manifest.*sha256|sha256.*manifest'

d=$(copy_good vermismatch)
sed -i.bak 's/^version=.*/version=v9.9.9/' "$d/MANIFEST"; rm -f "$d/MANIFEST.bak"
expect_fail "fails on a manifest/version mismatch" "$d" 'version'

d=$(copy_good amd64)
(cd "$clone" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -ldflags "$ldx" -o "$d/raftkv-server-linux-arm64" ./cmd/raftkv-server) >/dev/null 2>&1
rehash "$d" raftkv-server-linux-arm64
expect_fail "fails on a non-arm64 build" "$d" 'GOARCH|arm64'

d=$(copy_good notrim)
(cd "$clone" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -buildvcs=false -ldflags "$ldx" -o "$d/gateway-linux-arm64" ./cmd/gateway) >/dev/null 2>&1
rehash "$d" gateway-linux-arm64
expect_fail "fails on a build without -trimpath" "$d" 'trimpath'

d=$(copy_good badx)
(cd "$clone" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X github.com/MenaceHecker/raftkv/internal/buildinfo.Version=other" -o "$d/gateway-linux-arm64" ./cmd/gateway) >/dev/null 2>&1
rehash "$d" gateway-linux-arm64
expect_fail "fails when -X values differ from the manifest" "$d" 'version|commit|date|-X'

d=$(copy_good fake)
(cd "$clone" && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags fakecluster -trimpath -buildvcs=false -ldflags "$ldx" -o "$d/gateway-linux-arm64" ./cmd/gateway) >/dev/null 2>&1
rehash "$d" gateway-linux-arm64
expect_fail "fails on a gateway built with fakecluster" "$d" 'fakecluster'

d=$(copy_good nodir); expect_fail "fails on a directory that does not exist" "$work/nope/$ver" 'missing|not a directory|no such'

echo
if [ "$fails" -ne 0 ]; then echo "$fails verify-release test(s) failed"; exit 1; fi
echo "all verify-release tests passed"
