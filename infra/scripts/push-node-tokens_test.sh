#!/usr/bin/env bash
# Tests for push-node-tokens.sh with STUB aws/ssh/sudo (see _testlib.sh): nothing touches AWS or a host.
set -uo pipefail
. "$(dirname "$0")/_testlib.sh"
script="$here/push-node-tokens.sh"
ids=(i-0aaa1111bbbb2222c i-0aaa1111bbbb2222d i-0aaa1111bbbb2222e)

setup() {
  new_sandbox
  export AWS_PROFILE=stub-profile RAFTKV_TOKEN_DIR="$sb/remote"
  mkdir -p "$RAFTKV_TOKEN_DIR"
}
nofiles() { [ -z "$(ls -A "$sb/remote")" ]; }

# --- success ---------------------------------------------------------------------------------
setup
run_script bash "$script" "${ids[@]}"
expect_ok "success: exit 0" "$([ "$rc" = 0 ] && echo true || echo false)" "rc=$rc: $(cat "$sb/out")"
expect_ok "node-admin token written exactly" "$([ "$(cat "$RAFTKV_TOKEN_DIR/node-admin.token" 2>/dev/null)" = "$TOK_NODE" ] && echo true || echo false)"
expect_ok "cluster token written exactly" "$([ "$(cat "$RAFTKV_TOKEN_DIR/cluster.token" 2>/dev/null)" = "$TOK_CLUSTER" ] && echo true || echo false)"
expect_ok "files are mode 600" "$([ "$(mode_of "$RAFTKV_TOKEN_DIR/node-admin.token")" = 600 ] && [ "$(mode_of "$RAFTKV_TOKEN_DIR/cluster.token")" = 600 ] && echo true || echo false)"
expect_ok "no stray .new file left on the host" "$([ "$(ls -A "$RAFTKV_TOKEN_DIR" | sort | tr '\n' ' ')" = "cluster.token node-admin.token " ] && echo true || echo false)" "$(ls -A "$RAFTKV_TOKEN_DIR")"
expect_ok "files are chowned to raftkv" "$([ "$(grep -c '^chown \[raftkv:raftkv\]' "$STUB_LOG")" = 6 ] && echo true || echo false)" "$(grep '^chown' "$STUB_LOG")"
expect_ok "every instance is reached over EICE, in order, as ec2-user" \
  "$(for i in 0 1 2; do grep -q "^aws \[ec2-instance-connect\] \[ssh\] .*\[--instance-id\] \[${ids[$i]}\] .*\[--connection-type\] \[eice\]" "$STUB_LOG" || { echo false; exit; }; done; grep -q '\[--os-user\] \[ec2-user\]' "$STUB_LOG" && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "the unit is started on each node after its tokens" "$([ "$(grep -c '^systemctl \[start\] \[raftkvd\]' "$STUB_LOG")" = 3 ] && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "no token in any logged command line, output or temp file" "$(no_secret_anywhere && echo true || echo false)"
expect_ok "no remote command line carries a token" "$([ -s "$STUB_LOG.remote" ] && ! grep -qF -e "$TOK_NODE" -e "$TOK_CLUSTER" "$STUB_LOG.remote" && echo true || echo false)"
expect_ok "never --value, never the admin token, only the two node-side parameters are read" \
  "$(! grep -q -e '--value' -e '/raftkv/admin-token' "$STUB_LOG" && grep -q '\[/raftkv/node-admin-token\]' "$STUB_LOG" && grep -q '\[/raftkv/cluster-token\]' "$STUB_LOG" && echo true || echo false)"
expect_ok "never calls ssh or scp directly" "$(! grep -q FORBIDDEN "$STUB_LOG" && echo true || echo false)"
expect_ok "never traces (no set -x in the script)" "$(! grep -Eq 'set[[:space:]]+-[a-z]*x|xtrace' "$script" && echo true || echo false)"

# --- refusals happen before any aws call ---------------------------------------------------------
refuses() { # refuses <name> [args...] : non-zero exit and zero aws calls
  local name=$1; shift
  run_script bash "$script" "$@"
  expect_ok "$name" "$([ "$rc" != 0 ] && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && nofiles && echo true || echo false)" "rc=$rc calls=$(cat "$STUB_LOG")"
}
setup; refuses "no instance ids" 
setup; refuses "two instance ids" "${ids[0]}" "${ids[1]}"
setup; refuses "four instance ids" "${ids[@]}" i-0aaa1111bbbb2222f
setup; refuses "malformed instance id" "${ids[0]}" "not-an-id" "${ids[2]}"
setup; refuses "instance id with shell metacharacters" "${ids[0]}" 'i-0aaa;rm -rf x' "${ids[2]}"
setup; refuses "duplicate instance id" "${ids[0]}" "${ids[0]}" "${ids[2]}"
setup; unset AWS_PROFILE; refuses "AWS_PROFILE unset" "${ids[@]}"
setup; export AWS_ACCESS_KEY_ID=AKIAEXAMPLE; refuses "static access key in the environment" "${ids[@]}"; unset AWS_ACCESS_KEY_ID
setup; export CI=true; refuses "CI is set" "${ids[@]}"; unset CI
setup; export RAFTKV_TOKEN_DIR="relative"; refuses "relative token dir" "${ids[@]}"

# --- failures ---------------------------------------------------------------------------------
setup; export STUB_FAIL="/raftkv/cluster-token"
run_script bash "$script" "${ids[@]}"
expect_ok "SSM read fails: non-zero exit, nothing pushed to any node" "$([ "$rc" != 0 ] && ! grep -q 'ec2-instance-connect' "$STUB_LOG" && nofiles && echo true || echo false)"
expect_ok "SSM read fails: no token in log or output" "$(no_secret_anywhere && echo true || echo false)"

setup; export STUB_TOK_CLUSTER_OVERRIDE="$TOK_NODE"
run_script bash "$script" "${ids[@]}"
expect_ok "node-admin and cluster token equal: refused before any push" "$([ "$rc" != 0 ] && ! grep -q 'ec2-instance-connect' "$STUB_LOG" && nofiles && echo true || echo false)"
unset STUB_TOK_CLUSTER_OVERRIDE

setup; export STUB_FAIL="${ids[1]}"
run_script bash "$script" "${ids[@]}"
expect_ok "node 2 unreachable: non-zero exit" "$([ "$rc" != 0 ] && echo true || echo false)"
expect_ok "node 2 unreachable: the unit is not started on it and the run stops there" "$([ "$(grep -c '^systemctl \[start\]' "$STUB_LOG")" = 1 ] && ! grep -q "${ids[2]}" "$STUB_LOG" && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "node 2 unreachable: output names the instance, never a token" "$(grep -q "${ids[1]}" "$sb/out" && no_secret_anywhere && echo true || echo false)" "$(cat "$sb/out")"

echo "push-node-tokens_test: $fails failure(s)"
[ "$fails" -eq 0 ]
