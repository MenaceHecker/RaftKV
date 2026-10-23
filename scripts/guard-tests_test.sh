#!/usr/bin/env bash
# Tests for scripts/guard-tests.sh. Builds a throwaway git repo per case.
set -uo pipefail
guard="$(cd "$(dirname "$0")" && pwd)/guard-tests.sh"
fails=0

# run_case <name> <expected exit> <setup function run on the feature branch>
run_case() {
  local name="$1" want="$2" setup="$3" dir out got
  dir=$(mktemp -d)
  (
    cd "$dir" || exit 1
    git init -q -b main . && git config user.email t@t && git config user.name t
    printf 'package x\n\nfunc F() int { return 1 }\n' > code.go
    printf 'package x\n\nimport "testing"\n\nfunc TestF(t *testing.T) {\n\trequire.Equal(t, 1, F())\n\trequire.NotNil(t, F)\n\tt.Fatalf("x")\n}\n' > code_test.go
    git add . && git commit -q -m base
    git checkout -q -b feature
    "$setup"
    git add . && git commit -q -m change
  )
  out=$(cd "$dir" && "$guard" main 2>&1)
  got=$?
  if [ "$got" -ne "$want" ]; then
    echo "FAIL $name: want exit $want, got $got"; echo "$out" | sed 's/^/    /'; fails=$((fails+1))
  else
    echo "ok   $name"
  fi
  rm -rf "$dir"
}

add_skip()      { printf 'package x\n\nimport "testing"\n\nfunc TestS(t *testing.T) {\n\tt.Skip("later")\n}\n' > skip_test.go; }
drop_asserts()  { printf 'package x\n\nimport "testing"\n\nfunc TestF(t *testing.T) {\n}\n' > code_test.go; }
big_timeout()   { printf 'package x\n\nimport "time"\n\nvar d = 5000 * time.Millisecond\n' > timeout_test.go; }
add_good_test() { printf 'package x\n\nimport "testing"\n\nfunc TestG(t *testing.T) {\n\trequire.Equal(t, 1, F())\n}\n' > good_test.go; }
touch_code()    { printf 'package x\n\nfunc F() int { return 2 }\n' > code.go; }

run_case "t.Skip added exits 1"            1 add_skip
run_case "assertions removed exits 1"      1 drop_asserts
run_case "large timeout added exits 1"     1 big_timeout
run_case "new test with assertion exits 0" 0 add_good_test
run_case "non-test change exits 0"         0 touch_code

# the WARN text must be printed for the removed-assertions case
dir=$(mktemp -d)
(
  cd "$dir" && git init -q -b main . && git config user.email t@t && git config user.name t
  printf 'package x\n\nfunc TestF(t *testing.T) {\n\trequire.Equal(t, 1, 1)\n\trequire.NotNil(t, 1)\n}\n' > a_test.go
  git add . && git commit -q -m base && git checkout -q -b feature
  printf 'package x\n\nfunc TestF(t *testing.T) {\n}\n' > a_test.go
  git add . && git commit -q -m change
)
warn_out=$(cd "$dir" && "$guard" main 2>&1 || true)
if echo "$warn_out" | grep -q WARN; then echo "ok   WARN printed"; else echo "FAIL WARN not printed"; fails=$((fails+1)); fi
rm -rf "$dir"

[ "$fails" -eq 0 ] || { echo "$fails failure(s)"; exit 1; }
echo "all guard-tests cases passed"
