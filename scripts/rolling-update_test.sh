#!/usr/bin/env bash
# Tests for scripts/rolling-update.sh with STUB aws/sleep/curl/ssh/terraform (see ops_testlib.sh): nothing
# touches AWS or a host. The stub aws models the cluster (roles, applied index, per-node failures).
set -uo pipefail
. "$(dirname "$0")/ops_testlib.sh"
script="$repo/scripts/rolling-update.sh"
BUCKET=raftkv-artifacts-stub

setup() { new_ops_sandbox; make_dist "$sb/dist" v2; }
ru() { run_script bash "$script" --bucket "$BUCKET" --dist "$dist" --timeout 10 "$@" "${ids[1]}" "${ids[2]}" "${ids[3]}"; }
b2s() { [ "$1" = 0 ] && echo true || echo false; }
violations_empty() { [ ! -s "$sb/violations" ]; }
all_at_target() { local n; for n in 1 2 3; do [ "$(cat "$sb/cluster/$n/sha")" = "$TARGET_SHA" ] || return 1; done; }
no_remote_write_for() { # no write command went to slot $1
  ! remote_log | grep -E "$write_re" | grep -q "i-0aaa1111bbbb2222$(echo c d e | cut -d' ' -f"$1")"
}

# --- success: order, staging first, never two down ---------------------------------------------
for leader in 1 2 3; do
  setup; export STUB_LEADER=$leader
  ru
  case $leader in 1) want="2 3 1" ;; 2) want="1 3 2" ;; 3) want="1 2 3" ;; esac
  expect_ok "leader $leader: exit 0" "$(b2s "$([ "$rc" = 0 ] && echo 0 || echo 1)")" "rc=$rc: $(cat "$sb/out")"
  expect_ok "leader $leader: restart order is '$want' (followers ascending, leader last)" "$([ "$(restart_order)" = "$want" ] && echo true || echo false)" "got: $(restart_order)"
  expect_ok "leader $leader: all 3 nodes run the target binary" "$(all_at_target && echo true || echo false)"
  expect_ok "leader $leader: all 3 stages precede the first systemctl" \
    "$(first=$(remote_log | grep -n 'systemctl' | head -1 | cut -d: -f1); last=$(remote_log | grep -n 's3 cp' | tail -1 | cut -d: -f1); n=$(remote_log | grep -c 's3 cp'); [ "$n" = 3 ] && [ "$last" -lt "$first" ] && echo true || echo false)" "$(remote_log)"
  expect_ok "leader $leader: never two nodes down (violations empty)" "$(violations_empty && echo true || echo false)" "$(cat "$sb/violations")"
  expect_ok "leader $leader: a .prev copy is taken before every swap" "$([ "$(remote_log | grep -c 'raftkvd.prev')" = 3 ] && echo true || echo false)"
done
expect_ok "every aws call is ec2-instance-connect ssh over --connection-type eice" \
  "$(! grep '^aws' "$STUB_LOG" | grep -qv 'ec2-instance-connect\] \[ssh\] .*\[--connection-type\] \[eice\]' && echo true || echo false)" "$(grep '^aws' "$STUB_LOG" | head -3)"
expect_ok "no direct ssh, scp, terraform or raftkvctl" "$(! grep -q FORBIDDEN "$STUB_LOG" && echo true || echo false)"
expect_ok "no token path, ssm, or token value in any logged line or output" \
  "$(! grep -Eiq '\.token|ssm|TESTTOKEN' "$STUB_LOG" "$sb/out" && no_secret_anywhere && echo true || echo false)"
expect_ok "the hash is verified on the node (sha256sum -c) and the bucket MANIFEST cross-checked" \
  "$(remote_log | grep -q 'sha256sum -c' && remote_log | grep -q 'MANIFEST' && echo true || echo false)"
expect_ok "no set -x in the script" "$(! grep -Eq 'set[[:space:]]+-[a-z]*x|xtrace' "$script" && echo true || echo false)"

# --- staging failures: exit 2, cluster unchanged ----------------------------------------------------
setup; export STUB_FAIL_SHA=2
ru
expect_ok "hash mismatch on node 2: exit 2" "$(b2s "$([ "$rc" = 2 ] && echo 0 || echo 1)")" "rc=$rc"
expect_ok "hash mismatch: no systemctl and no mv on any node" "$([ "$(remote_log | grep -Ec 'systemctl| mv ')" = 0 ] && echo true || echo false)"
expect_ok "hash mismatch: every node still on the old binary" "$(for n in 1 2 3; do [ "$(cat "$sb/cluster/$n/sha")" = "$OLD_SHA" ] || { echo false; exit; }; done; echo true)"
expect_ok "hash mismatch: message names node 2 and the sha256" "$(grep -qi 'node 2' "$sb/out" && grep -qi 'sha256' "$sb/out" && echo true || echo false)" "$(cat "$sb/out")"

setup; export STUB_FAIL_S3=3
ru
expect_ok "S3 failure on node 3: exit 2, message says missing or not uploaded" "$([ "$rc" = 2 ] && grep -q 'missing or not uploaded' "$sb/out" && echo true || echo false)" "rc=$rc $(cat "$sb/out")"
expect_ok "S3 failure: no systemctl" "$([ "$(remote_log | grep -c systemctl)" = 0 ] && echo true || echo false)"

setup; export STUB_FAIL_MANIFEST=1
ru
expect_ok "bucket MANIFEST disagrees: exit 2, nothing restarted" "$([ "$rc" = 2 ] && [ "$(remote_log | grep -c systemctl)" = 0 ] && echo true || echo false)" "rc=$rc"

# --- a node never becomes healthy: rollback of that node only ------------------------------------------
setup; export STUB_LEADER=1 STUB_UNHEALTHY=2
ru
expect_ok "node 2 never healthy: exit 3" "$(b2s "$([ "$rc" = 3 ] && echo 0 || echo 1)")" "rc=$rc: $(cat "$sb/out")"
expect_ok "node 2 never healthy: .prev restored (cp -p raftkvd.prev) and restarted again" \
  "$(remote_log | grep -q 'cp -p /usr/local/bin/raftkvd.prev' && [ "$(restart_order)" = "2 2" ] && echo true || echo false)" "$(restart_order)"
expect_ok "node 2 never healthy: node 2 is back on the old binary" "$([ "$(cat "$sb/cluster/2/sha")" = "$OLD_SHA" ] && echo true || echo false)"
expect_ok "node 2 never healthy: later nodes 1 and 3 were never written to" "$(no_remote_write_for 1 && no_remote_write_for 3 && echo true || echo false)"
expect_ok "node 2 never healthy: never two down" "$(violations_empty && echo true || echo false)" "$(cat "$sb/violations")"

setup; export STUB_LEADER=1 STUB_UNHEALTHY=2 STUB_FAIL_ROLLBACK=2
ru
expect_ok "rollback fails: exit 4 and MANUAL in the message" "$([ "$rc" = 4 ] && grep -q 'MANUAL' "$sb/out" && echo true || echo false)" "rc=$rc: $(cat "$sb/out")"
expect_ok "rollback fails: nothing is touched after node 2" "$(no_remote_write_for 1 && no_remote_write_for 3 && echo true || echo false)"
expect_ok "rollback fails: never two down" "$(violations_empty && echo true || echo false)"

setup; export STUB_FAIL_SWAP=2 STUB_LEADER=1
ru
expect_ok "swap fails before any change: exit 3, node 2 never restarted" "$([ "$rc" = 3 ] && [ "$(remote_log | grep -c systemctl)" = 0 ] && echo true || echo false)" "rc=$rc"

# --- degraded preflight: REFUSED, zero writes --------------------------------------------------------------
refused_clean() { # name
  expect_ok "$1" "$([ "$rc" = 1 ] && grep -q 'REFUSED:' "$sb/out" && [ "$(writes_count)" = 0 ] && echo true || echo false)" "rc=$rc writes=$(writes_count): $(cat "$sb/out")"
}
setup; export STUB_PRE_DOWN=2; ru; refused_clean "a node is down: refused, zero write commands"
setup; export STUB_NOLEADER=1; ru; refused_clean "no leader: refused, zero write commands"
setup; export STUB_DEGRADED=3; ru; refused_clean "node 3 lags by more than --max-lag 5: refused, zero write commands"
setup; export STUB_DEGRADED=3; ru --max-lag 200
expect_ok "--max-lag 200 accepts the same lag" "$(b2s "$([ "$rc" = 0 ] && echo 0 || echo 1)")" "rc=$rc: $(cat "$sb/out")"
setup; export STUB_WRONG_ID=2; ru; refused_clean "node 2 reports another id than its slot: refused, zero writes"
setup; export STUB_NO_CURL=1; ru
expect_ok "curl missing on the nodes: refused with a clear curl message, zero writes" "$([ "$rc" = 1 ] && grep -qi 'curl' "$sb/out" && [ "$(writes_count)" = 0 ] && echo true || echo false)" "rc=$rc: $(cat "$sb/out")"

# --- leadership moves mid-run -------------------------------------------------------------------------------
setup; export STUB_LEADER=1 STUB_LEADER_MOVES=1 STUB_MOVE_TO=3
ru
expect_ok "leadership moves to node 3 after the first restart: order recomputed, new leader last ('2 1 3')" "$([ "$rc" = 0 ] && [ "$(restart_order)" = "2 1 3" ] && echo true || echo false)" "rc=$rc order=$(restart_order)"
expect_ok "leadership moves: never two down" "$(violations_empty && echo true || echo false)" "$(cat "$sb/violations")"

# --- idempotence -----------------------------------------------------------------------------------------------
setup; export STUB_LEADER=1
ru; : > "$STUB_LOG"; ru
expect_ok "rerun after success: exit 0, no write command" "$([ "$rc" = 0 ] && [ "$(writes_count)" = 0 ] && echo true || echo false)" "rc=$rc writes=$(writes_count)"

setup; export STUB_LEADER=1 STUB_UNHEALTHY=3
ru
expect_ok "abort at node 3 (order 2, 3, 1): exit 3" "$(b2s "$([ "$rc" = 3 ] && echo 0 || echo 1)")" "rc=$rc order=$(restart_order)"
unset STUB_UNHEALTHY; : > "$STUB_LOG"
ru
expect_ok "rerun after the abort touches only node 3 and node 1 (not node 2, already updated)" "$([ "$rc" = 0 ] && no_remote_write_for 2 && [ "$(restart_order)" = "3 1" ] && echo true || echo false)" "rc=$rc order=$(restart_order)"
expect_ok "rerun after the abort: all at target, never two down" "$(all_at_target && violations_empty && echo true || echo false)"

# --- dry-run and restart-only ----------------------------------------------------------------------------------------
setup; export STUB_LEADER=2
ru --dry-run
expect_ok "--dry-run: exit 0, no write command at all" "$([ "$rc" = 0 ] && [ "$(writes_count)" = 0 ] && echo true || echo false)" "rc=$rc writes=$(writes_count)"
expect_ok "--dry-run: plan printed with the order and the exact remote commands" "$(grep -q 'systemctl restart raftkvd' "$sb/out" && grep -q 's3 cp' "$sb/out" && grep -q 'node 1' "$sb/out" && echo true || echo false)" "$(cat "$sb/out")"
expect_ok "--dry-run: cluster unchanged" "$([ "$(cat "$sb/cluster/1/sha")" = "$OLD_SHA" ] && echo true || echo false)"

setup; export STUB_LEADER=3
run_script bash "$script" --restart-only --timeout 10 "${ids[1]}" "${ids[2]}" "${ids[3]}"
expect_ok "--restart-only: exit 0, same order, no s3, no mv, no cp" \
  "$([ "$rc" = 0 ] && [ "$(restart_order)" = "1 2 3" ] && [ "$(remote_log | grep -Ec 's3 cp| mv |cp -p|install ')" = 0 ] && echo true || echo false)" "rc=$rc order=$(restart_order): $(cat "$sb/out")"
expect_ok "--restart-only: never two down" "$(violations_empty && echo true || echo false)"
setup; export STUB_LEADER=1
run_script bash "$script" --restart-only --dry-run "${ids[1]}" "${ids[2]}" "${ids[3]}"
expect_ok "--restart-only --dry-run: no write command" "$([ "$rc" = 0 ] && [ "$(writes_count)" = 0 ] && echo true || echo false)"

# --- gateway check ------------------------------------------------------------------------------------------------------------
setup; export STUB_GW_BODY='{"leaderId":1,"chaos":{"state":"recovering"}}'
ru --gateway-url https://api.example.test
refused_clean "gateway reports chaos recovering: refused, zero writes"
setup
ru --gateway-url https://api.example.test
expect_ok "gateway idle: proceeds; only GET /v1/cluster is fetched" "$([ "$rc" = 0 ] && grep '^curl' "$STUB_LOG" | grep -q '/v1/cluster' && ! grep '^curl' "$STUB_LOG" | grep -Eq 'POST|PUT|DELETE|-X' && echo true || echo false)" "rc=$rc"

# --- refusals: literal REFUSED:, zero aws calls ----------------------------------------------------------------------------------
refuses() { # refuses <name> : exit 1, REFUSED:, no aws call
  expect_ok "refuses: $1" "$([ "$rc" = 1 ] && grep -q 'REFUSED:' "$sb/out" && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)" "rc=$rc calls=$(grep -c '^aws' "$STUB_LOG"): $(cat "$sb/out")"
}
setup; unset AWS_PROFILE; ru; refuses "no profile"
setup; export AWS_ACCESS_KEY_ID=AKIAEXAMPLE; ru; refuses "static access key in the environment"; unset AWS_ACCESS_KEY_ID
setup; export CI=true; ru; refuses "CI is set"; unset CI
setup; run_script bash "$script" --bucket "$BUCKET" --dist "$dist" "${ids[1]}" "${ids[2]}"; refuses "two instance ids"
setup; run_script bash "$script" --bucket "$BUCKET" --dist "$dist" "${ids[@]:1}" i-0aaa1111bbbb2222f; refuses "four instance ids"
setup; run_script bash "$script" --bucket "$BUCKET" --dist "$dist" "${ids[1]}" "not-an-id" "${ids[3]}"; refuses "malformed instance id"
setup; run_script bash "$script" --bucket "$BUCKET" --dist "$dist" "${ids[1]}" 'i-0aaa;rm -rf x' "${ids[3]}"; refuses "instance id with shell metacharacters"
setup; run_script bash "$script" --bucket "$BUCKET" --dist "$dist" "${ids[1]}" "${ids[1]}" "${ids[3]}"; refuses "duplicate instance id"
setup; run_script bash "$script" --bucket "$BUCKET" --dist "$sb/nope" "${ids[@]:1}"; refuses "dist directory missing"
setup; sed -i.bak 's/^dirty=false$/dirty=true/' "$dist/MANIFEST" && ru; refuses "dirty dist (MANIFEST dirty=true)"
setup; echo "$(printf 'x%.0s' $(seq 64))  raftkv-server-linux-arm64" > "$dist/raftkv-server-linux-arm64.sha256"; ru; refuses "inconsistent dist (.sha256 disagrees with MANIFEST)"
setup; sed -i.bak 's/^version=.*/version=v2\/evil/' "$dist/MANIFEST" && ru; refuses "version with a slash"
setup; run_script bash "$script" --bucket 'Bad Bucket;x' --dist "$dist" "${ids[@]:1}"; refuses "bad bucket name"
setup; run_script bash "$script" --dist "$dist" "${ids[@]:1}"; refuses "no bucket"
setup; ru --max-lag abc; refuses "non-numeric --max-lag"
setup; ru --bogus; refuses "unknown flag"

echo "rolling-update_test: $fails failure(s)"
[ "$fails" -eq 0 ]
