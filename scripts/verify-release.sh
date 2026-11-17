#!/usr/bin/env bash
# Verify a dist/<version>/ directory made by scripts/release.sh (D3). Exits non-zero with a
# one-line "verify-release: FAIL ..." reason at the first failed check. It never runs an
# arm64 binary and needs no AWS. Usage: scripts/verify-release.sh dist/<version>
set -uo pipefail
cd "$(dirname "$0")/.."
root=$PWD
tc=$(awk '$1=="toolchain" {print $2}' go.mod)
[ -n "$tc" ] && export GOTOOLCHAIN="$tc"

pkg=github.com/MenaceHecker/raftkv/internal/buildinfo
files="raftkv-server-linux-arm64 gateway-linux-arm64"

fail() { echo "verify-release: FAIL $*" >&2; exit 1; }
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

dir=${1:-}
[ -n "$dir" ] || fail "usage: verify-release.sh <dir>"
[ -d "$dir" ] || fail "$dir is not a directory (missing or no such directory)"
dir=$(cd "$dir" && pwd)

manifest="$dir/MANIFEST"
[ -f "$manifest" ] || fail "missing MANIFEST in $dir"
for f in $files; do
  [ -f "$dir/$f" ] || fail "missing $f in $dir"
  [ -f "$dir/$f.sha256" ] || fail "missing $f.sha256 in $dir"
done

mf() { sed -n "s/^$1=//p" "$manifest" | head -1; }

# sha256: the file, its .sha256 and the manifest must all agree.
for f in $files; do
  got=$(sha "$dir/$f")
  want=$(cut -d' ' -f1 "$dir/$f.sha256")
  name=$(awk '{print $2}' "$dir/$f.sha256")
  [ "$name" = "$f" ] || fail "$f.sha256 names '$name', want $f"
  [ "$got" = "$want" ] || fail "sha256 mismatch for $f: file $got, $f.sha256 $want"
  [ "$(mf "sha256.$f")" = "$got" ] || fail "manifest sha256 mismatch for $f: file $got, MANIFEST $(mf "sha256.$f")"
done

version=$(mf version); commit=$(mf commit); date=$(mf date)
[ -n "$version" ] && [ -n "$commit" ] && [ -n "$date" ] || fail "MANIFEST lacks version, commit or date"
dirname=$(basename "$dir")
[ "$version" = "$dirname" ] || fail "version mismatch: MANIFEST says $version, directory is $dirname"

# Build settings recorded in the binary itself (readable on stripped binaries).
for f in $files; do
  info=$(go version -m "$dir/$f" 2>&1) || fail "go version -m $f failed: $info"
  for want in "GOOS=linux" "GOARCH=arm64" "CGO_ENABLED=0"; do
    echo "$info" | grep -q "[[:space:]]$want\$" || fail "$f was not built with $want (GOARCH/GOOS/CGO check)"
  done
  echo "$info" | grep -q -- "-trimpath=true" || fail "$f was not built with -trimpath"
  # go version -m does not record -ldflags (nor the -X values) under -trimpath, so the
  # stamped strings are looked for in the binary itself.
  for kv in "$version" "$commit" "$date"; do
    grep -aqF -- "$kv" "$dir/$f" || fail "$f does not contain '$kv': -X version/commit/date differ from MANIFEST"
  done
done

# The production gateway must not be able to serve fake data. Two independent checks: the
# package graph (what a plain build links) and the finished binary (symbols are stripped,
# but function names stay in the pclntab, so the package path is still present).
if go list -deps ./cmd/gateway 2>/dev/null | grep -q fakecluster; then
  fail "go list -deps ./cmd/gateway (no tags) contains fakecluster"
fi
if grep -aqF 'internal/gateway/fakecluster' "$dir/gateway-linux-arm64"; then
  fail "gateway-linux-arm64 contains internal/gateway/fakecluster (built with -tags fakecluster?)"
fi

# -trimpath: no build directory or home directory in either binary.
for f in $files; do
  for p in "$root" "${HOME:-}"; do
    case "$p" in ""|/) continue ;; esac
    if grep -aqF -- "$p" "$dir/$f"; then fail "$f contains the build path $p (built without -trimpath?)"; fi
  done
done
echo "verify-release: ok $dirname"
