#!/usr/bin/env bash
# Reproducible linux/arm64 release build (RUNBOOK 10.3 D3). BUILD ONLY: this script
# needs no AWS and never uploads; scripts/release-upload.sh does that, by hand.
#
# Output: $DIST_DIR/<version>/{raftkv-server-linux-arm64,gateway-linux-arm64,*.sha256,MANIFEST}
# (DIST_DIR defaults to dist, relative to the repo root).
#
# Reproducible means: the same clean commit gives byte-identical files from any clone path.
# That needs -trimpath, -buildvcs=false, an empty build id, the COMMIT date (never the
# build time), -mod=readonly, no go.work, and the toolchain pinned by the `toolchain` line
# of go.mod. -s -w strips symbols and DWARF (smaller, but crash traces lose them).
#
# A dirty tree is refused unless ALLOW_DIRTY=1, which marks version and commit "-dirty";
# release-upload.sh never uploads those.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD

pkg=github.com/MenaceHecker/raftkv/internal/buildinfo
server=raftkv-server-linux-arm64
gateway=gateway-linux-arm64

die() { echo "release: $*" >&2; exit 1; }

# Pin the exact compiler recorded in go.mod, so the bytes do not depend on the machine's go.
tc=$(awk '$1=="toolchain" {print $2}' go.mod)
[ -n "$tc" ] && export GOTOOLCHAIN="$tc"

sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

git rev-parse --git-dir >/dev/null 2>&1 || die "not a git work tree"
version=$(git describe --tags --always --abbrev=12)
commit=$(git rev-parse HEAD)
# The COMMIT date in UTC: a rebuild a day later must give the same bytes.
date=$(TZ=UTC git show -s --format=%cd --date=format-local:%Y-%m-%dT%H:%M:%SZ HEAD)

# Dirty: any tracked change, or an untracked Go source / module file. Other untracked files
# (notes, review packets) cannot reach the binaries and do not count.
dirty=false
srcpat='^\?\? (.*\.go|(.*/)?go\.mod|(.*/)?go\.sum)$'
status=$(git status --porcelain --untracked-files=all)
if echo "$status" | grep -v '^??' | grep -q . || echo "$status" | grep -Eq "$srcpat"; then
  dirty=true
fi
if [ "$dirty" = true ]; then
  [ "${ALLOW_DIRTY:-}" = 1 ] || die "the working tree is dirty (tracked changes or untracked .go/go.mod/go.sum files); commit them, or ALLOW_DIRTY=1 for a local -dirty build that can never be uploaded"
  version="$version-dirty"
  commit="$commit-dirty"
fi
case "$version" in
  */*|*[[:space:]]*|"") die "refusing version '$version': it must be non-empty with no '/' or whitespace" ;;
esac

dist="${DIST_DIR:-dist}"
case "$dist" in /*) ;; *) dist="$root/$dist" ;; esac
out="$dist/$version"
rm -rf "$out"
mkdir -p "$out"

ldflags="-s -w -buildid= -X $pkg.Version=$version -X $pkg.Commit=$commit -X $pkg.Date=$date"
build() { # build <package> <output file>; no -tags, so the gateway has no fakecluster
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOARM64=v8.0 GOFLAGS=-mod=readonly GOWORK=off \
    go build -trimpath -buildvcs=false -ldflags "$ldflags" -o "$out/$2" "$1"
}
build ./cmd/raftkv-server "$server"
build ./cmd/gateway "$gateway"

for f in "$server" "$gateway"; do
  echo "$(sha "$out/$f")  $f" > "$out/$f.sha256"
done

{
  echo "version=$version"
  echo "commit=$commit"
  echo "date=$date"
  echo "dirty=$dirty"
  echo "goversion=$(go env GOVERSION)"
  echo "goos=linux"
  echo "goarch=arm64"
  for f in "$server" "$gateway"; do echo "sha256.$f=$(sha "$out/$f")"; done
} > "$out/MANIFEST"

scripts/verify-release.sh "$out"
echo "release: $version -> $out"
