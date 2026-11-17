#!/usr/bin/env bash
# Shared helpers for the release shell tests (sourced, not run). D3.
# Every test works in a throwaway git clone holding a snapshot of the CURRENT working
# tree, so it sees uncommitted changes and never touches the real repo.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fails=0

pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails+1)); }
check() { # check <name> <cmd...>  (cmd must succeed)
  local name="$1"; shift
  if "$@" >"$work/check.out" 2>&1; then pass "$name"; else fail "$name" "$(tr '\n' '|' <"$work/check.out" | cut -c1-400)"; fi
}

# snapshot <dir>: clone HEAD and commit the current working tree on top.
snapshot() {
  local dir="$1"
  git clone -q "$repo" "$dir" || return 1
  (
    cd "$repo" && git ls-files -co --exclude-standard -z | while IFS= read -r -d '' f; do
      [ -e "$f" ] && printf '%s\0' "$f"
    done | tar --null -T - -cf - | tar -xf - -C "$dir"
    cd "$dir" && git config user.email t@t && git config user.name t &&
      git add -A && git commit -q --allow-empty -m snapshot
  )
}


rel() { # rel <clone> <dist> [env...] : run release.sh in a clone, no aws
  local c="$1" d="$2"; shift 2
  (cd "$c" && env DIST_DIR="$d" "$@" scripts/release.sh)
}
verdir() { ls -d "$1"/*/ 2>/dev/null | head -1 | sed 's:/$::'; }

