#!/usr/bin/env bash
# flake-guard (T6.4): re-run the tests a PR adds or changes many times, in the three CI modes,
# so a race that only shows on a 2-core runner fails on the PR that introduces it.
# RUNBOOK 6.4 treats one failure in -count=20 as a P0 bug; this enforces it where it is cheap.
#
#   make flake-guard BASE=origin/main
#
# Env: BASE (origin/main)  COUNT (20)  PKG_COUNT (3)  MODES ("plain procs1 race")  LOAD (0|1)
#   plain = go test; procs1 = GOMAXPROCS=1 go test; race = go test -race.
#   LOAD=1 runs only plain and race, next to cmd/cpuhog (2 busy goroutines), and only for
#   internal/gateway and internal/transport selections.
# Selection and the report come from cmd/flakeselect (go/parser, not a regex over diff text).
# It NEVER retries: a failure is reported once and the exit status is 1.
set -euo pipefail

BASE="${BASE:-origin/main}"
COUNT="${COUNT:-20}"
PKG_COUNT="${PKG_COUNT:-3}"
MODES="${MODES:-plain procs1 race}"
LOAD="${LOAD:-0}"
MAX_SELECTED=40   # above this many selected tests, COUNT drops to 10 (said in the summary)

tools=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
repo=$(git rev-parse --show-toplevel)
cd "$repo"

work=$(mktemp -d)
hog_pid=""
cleanup() { [ -n "$hog_pid" ] && kill "$hog_pid" 2>/dev/null || true; rm -rf "$work"; }
trap cleanup EXIT

if ! git rev-parse --verify -q "$BASE^{commit}" >/dev/null; then
  echo "flake-guard: BASE '$BASE' is not a commit here (fetch it, or pass BASE=...)" >&2
  exit 2
fi

(cd "$tools" && go build -o "$work/flakeselect" ./cmd/flakeselect)
if [ "$LOAD" = 1 ]; then (cd "$tools" && go build -o "$work/cpuhog" ./cmd/cpuhog); fi

"$work/flakeselect" -base "$BASE" -format regex >"$work/sel"

notes=()
total=0
while IFS=$'\t' read -r kind pkg regex n; do
  [ "$kind" = run ] && total=$((total + n))
done <"$work/sel"
if [ "$total" -gt "$MAX_SELECTED" ] && [ "$COUNT" -gt 10 ]; then
  notes+=("$total tests selected (more than $MAX_SELECTED): COUNT reduced from $COUNT to 10")
  COUNT=10
fi

is_load_pkg() { case "$1" in ./internal/gateway|./internal/transport) return 0 ;; *) return 1 ;; esac; }

rows="$work/rows"; : >"$rows"
tails="$work/tails"; : >"$tails"
ran=0

# one_run MODE PKG COUNT REGEX WHOLE: runs go test once, appends report rows. Never retried.
one_run() {
  local mode=$1 pkg=$2 count=$3 regex=$4 whole=$5
  local base=${mode%+hog} args=(test -count="$count" -timeout 10m -json) rc=0 start=$SECONDS
  local out="$work/run.$ran.json"
  [ -n "$regex" ] && args+=(-run "$regex")
  [ "$base" = race ] && args+=(-race)
  args+=("$pkg")
  if [ "$mode" != "$base" ]; then "$work/cpuhog" -n 2 & hog_pid=$!; fi
  if [ "$base" = procs1 ]; then
    GOMAXPROCS=1 go "${args[@]}" >"$out" 2>"$out.err" || rc=$?
  else
    go "${args[@]}" >"$out" 2>"$out.err" || rc=$?
  fi
  if [ -n "$hog_pid" ]; then kill "$hog_pid" 2>/dev/null || true; wait "$hog_pid" 2>/dev/null || true; hog_pid=""; fi
  # go test's own stderr (build failures) rides along as non-JSON lines.
  cat "$out.err" >>"$out"
  local rflags=(-mode "$mode" -pkg "$pkg" -rc "$rc" -tail "$work/tail.$ran")
  [ "$whole" = 1 ] && rflags+=(-whole)
  "$work/flakeselect" report "${rflags[@]}" "$out" >>"$rows"
  [ -f "$work/tail.$ran" ] && cat "$work/tail.$ran" >>"$tails"
  echo "flake-guard: $mode $pkg count=$count took $((SECONDS - start))s (go test exit $rc)"
  ran=$((ran + 1))
}

selected_pkgs=()
while IFS=$'\t' read -r kind pkg regex n; do
  [ "$kind" = run ] || continue
  echo "flake-guard: selected $n test(s) in $pkg: -run '$regex'"
  if [ "$LOAD" = 1 ]; then
    is_load_pkg "$pkg" || continue
    for m in plain race; do one_run "$m+hog" "$pkg" "$COUNT" "$regex" 0; done
  else
    for m in $MODES; do one_run "$m" "$pkg" "$COUNT" "$regex" 0; done
  fi
done <"$work/sel"

if [ "$LOAD" != 1 ]; then
  while IFS=$'\t' read -r kind pkg _rest; do
    [ "$kind" = whole ] || continue
    for m in $MODES; do
      [ "$m" = race ] && continue # race is already covered by ci.yml's `go test -race ./...`
      one_run "$m" "$pkg" "$PKG_COUNT" "" 1
    done
  done <"$work/sel"
fi

summary="$work/summary.md"
{
  echo "### flake-guard (BASE=$BASE, COUNT=$COUNT, PKG_COUNT=$PKG_COUNT, LOAD=$LOAD)"
  for note in "${notes[@]+"${notes[@]}"}"; do echo; echo "> $note"; done
  echo
  if [ "$ran" -eq 0 ]; then
    echo "flake-guard: no test changes, nothing to run"
  else
    echo "| mode | package | test | failures/runs |"
    echo "|---|---|---|---|"
    awk -F'\t' '$1=="row"{printf "| %s | %s | %s | %d/%d |\n",$2,$3,$4,$5,$6}' "$rows"
  fi
} >"$summary"
cat "$summary"
[ -n "${GITHUB_STEP_SUMMARY:-}" ] && cat "$summary" >>"$GITHUB_STEP_SUMMARY"

failed=$(awk -F'\t' '$1=="row" && $5>0 {n++} END{print n+0}' "$rows")
if [ "$failed" -gt 0 ]; then
  echo
  awk -F'\t' '$1=="row" && $5>0 {print $2"\t"$3"\t"$4"\t"$5"\t"$6}' "$rows" |
    while IFS=$'\t' read -r mode pkg test f r; do
      file=""
      if [ "$test" != "(package)" ] && [ "${test#(}" = "$test" ]; then
        file=$(grep -ls "^func $test(" "${pkg#./}"/*_test.go 2>/dev/null | head -1 || true)
      fi
      echo "::error ${file:+file=$file,}title=flake-guard::$test failed $f/$r runs in mode $mode ($pkg)"
      base=${mode%+hog}; flags=""; envp=""
      [ "$base" = race ] && flags="-race "
      [ "$base" = procs1 ] && envp="GOMAXPROCS=1 "
      if [ "${test#(}" = "$test" ]; then
        echo "reproduce: ${envp}go test ${flags}-count=20 -run '^$test\$' $pkg"
      else
        echo "reproduce: ${envp}go test ${flags}-count=$PKG_COUNT $pkg"
      fi
    done
  echo "reproduce the whole guard: make flake-guard BASE=$BASE"
  if [ -s "$tails" ]; then echo "::group::first failure output"; cat "$tails"; echo "::endgroup::"; fi
  echo "flake-guard: FAILED ($failed failing rows). A failure here is a P0 under RUNBOOK 6.4; the guard does not retry."
  exit 1
fi
echo "flake-guard: ok"
