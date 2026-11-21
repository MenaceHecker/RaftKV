#!/usr/bin/env bash
# Self-test for scripts/flake-guard.sh (T6.4). Each case builds a throwaway git repo with a base
# commit on `main` and a PR commit on `pr`, copies fixtures from scripts/testdata/flake/ under their
# real names (they are *.go.txt in this repo so the README count walk, gofmt and go vet never see
# them), and runs the guard there. Real go builds and test runs; no network.
# Run by hand: scripts/flake-guard_test.sh   (about a minute; not part of `go test ./...`)
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
guard="$here/flake-guard.sh"
fx="$here/testdata/flake"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export GOTOOLCHAIN=local GOFLAGS=-mod=mod GOPROXY=off

nfail=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; nfail=$((nfail + 1)); }

# newrepo NAME: empty repo on branch main with a go.mod; sets $repo.
newrepo() {
  repo="$work/$1"
  mkdir -p "$repo"
  git -C "$repo" init -q -b main
  git -C "$repo" config user.email t@example.invalid
  git -C "$repo" config user.name t
  printf 'module flakefix\n\ngo 1.21\n' > "$repo/go.mod"
  printf 'readme\n' > "$repo/README.md"
}
put() { mkdir -p "$(dirname "$repo/$2")"; cp "$fx/$1" "$repo/$2"; }
commit() { git -C "$repo" add -A && git -C "$repo" commit -q -m "$1"; }
topr() { git -C "$repo" checkout -q -b pr; }

# guard: runs the guard in $repo with small counts; sets $rc and $out (file).
guard() {
  out="$work/out.$RANDOM"
  (cd "$repo" && BASE=main COUNT=5 PKG_COUNT=2 "$guard") >"$out" 2>&1
  rc=$?
}
has() { grep -Eq -- "$1" "$out"; }
show() { sed 's/^/    | /' "$out" | tail -25; }

# 1. racy fixture: red, names the test, the race mode, and a nonzero failure count.
newrepo racy; echo x > "$repo/.keep"; commit base; topr; put racy.go.txt racy/racy_test.go; commit pr
guard
if [ "$rc" -ne 0 ] && has '^\| race \| \./racy \| TestRacy \| [1-9][0-9]*/[0-9]+ \|' \
   && has "go test -race -count=20 -run '\^TestRacy\\$' \./racy" && has 'make flake-guard'; then
  pass "racy fixture fails, names test, race mode, failure count and the local command"
else fail "racy fixture fails" "rc=$rc"; show; fi

# 2. clean fixture: green.
newrepo clean; echo x > "$repo/.keep"; commit base; topr; put clean.go.txt clean/clean_test.go; commit pr
guard
if [ "$rc" -eq 0 ] && has '\| TestClean \| 0/[0-9]+ \|'; then pass "clean fixture passes"
else fail "clean fixture passes" "rc=$rc"; show; fi

# 3. helper-only change selects every dependent test (direct and transitive), not the others.
newrepo helper; put helper_base.go.txt h/h_test.go; commit base; topr; put helper_pr.go.txt h/h_test.go; commit pr
guard
if [ "$rc" -eq 0 ] && has '\| TestOne \|' && has '\| TestTwo \|' && has '\| TestFour \|' && ! has '\| TestThree \|'; then
  pass "helper change selects dependents (incl. via another helper) and only them"
else fail "helper change selection" "rc=$rc"; show; fi

# 4. renamed test (moved to another file) still runs under its new name.
newrepo rename; put rename_base.go.txt r/old_test.go; commit base; topr
git -C "$repo" mv r/old_test.go r/new_test.go; put rename_pr.go.txt r/new_test.go; commit pr
guard
if [ "$rc" -eq 0 ] && has '\| TestNew \| 0/[0-9]+ \|' && ! has '\| TestOld \|'; then pass "renamed test runs under its new name"
else fail "renamed test runs" "rc=$rc"; show; fi

# 5. no test changes: quick, green, says so.
newrepo none; echo x > "$repo/.keep"; commit base; topr; echo more >> "$repo/README.md"; commit pr
guard
if [ "$rc" -eq 0 ] && has 'flake-guard: no test changes, nothing to run'; then pass "no test changes: nothing to run, exit 0"
else fail "no test changes" "rc=$rc"; show; fi

# 6. exact names: changing TestA runs TestA (with its subtest) but not TestAB.
newrepo exact; put exact_base.go.txt e/e_test.go; commit base; topr; put exact_pr.go.txt e/e_test.go; commit pr
guard
if [ "$rc" -eq 0 ] && has '\| TestA \| 0/[0-9]+ \|' && ! has '\| TestAB \|' && has "-run '\^\(TestA\)\\$'"; then
  pass "selection uses exact anchored names (TestAB not selected by TestA)"
else fail "exact names" "rc=$rc"; show; fi

# 7. non-test change under internal/gateway selects the whole-package pass only.
newrepo whole; mkdir -p "$repo/internal/gateway"
printf 'package gateway\n\nfunc F() int { return 1 }\n' > "$repo/internal/gateway/g.go"
printf 'package gateway\n\nimport "testing"\n\nfunc TestF(t *testing.T) { _ = F() }\n' > "$repo/internal/gateway/g_test.go"
commit base; topr
printf 'package gateway\n\nfunc F() int { return 2 }\n' > "$repo/internal/gateway/g.go"; commit pr
guard
if [ "$rc" -eq 0 ] && has '\| plain \| \./internal/gateway \| \(whole package' && has '\| procs1 \| \./internal/gateway \| \(whole package' && ! has '\| race \| \./internal/gateway'; then
  pass "gateway source change runs the whole-package pass (plain and GOMAXPROCS=1, no race)"
else fail "whole-package pass" "rc=$rc"; show; fi

# 8. the guard never retries: a failing run is reported once and the exit stays nonzero.
newrepo noretry; echo x > "$repo/.keep"; commit base; topr
mkdir -p "$repo/f"; printf 'package f\n\nimport "testing"\n\nfunc TestAlwaysFails(t *testing.T) { t.Fatal("boom") }\n' > "$repo/f/f_test.go"; commit pr
guard
if [ "$rc" -ne 0 ] && has '^\| plain \| \./f \| TestAlwaysFails \| 5/5 \|'; then pass "a failing test is reported 5/5 and the guard exits nonzero (no retry to green)"
else fail "no retry" "rc=$rc"; show; fi

if [ "$nfail" -ne 0 ]; then echo "$nfail case(s) failed"; exit 1; fi
echo "all flake-guard cases passed"
