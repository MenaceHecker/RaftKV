#!/usr/bin/env bash
# Tests for scripts/demo-pause.sh with STUB aws/sleep/terraform/ssh (see ops_testlib.sh).
set -uo pipefail
. "$(dirname "$0")/ops_testlib.sh"
script="$repo/scripts/demo-pause.sh"
GW=i-0bbb1111cccc3333f
setup() { new_ops_sandbox; }
run() { run_script bash "$script" "$@"; }
line_of() { grep -n -- "$1" "$STUB_LOG" | head -1 | cut -d: -f1; }
b2s() { [ "$1" = 0 ] && echo true || echo false; }

setup; run pause "$GW" "${ids[1]}" "${ids[2]}" "${ids[3]}"
expect_ok "pause: exit 0" "$(b2s "$rc")" "rc=$rc: $(cat "$sb/out")"
gwstop=$(line_of "\[stop-instances\] .*\[$GW\]"); nstop=$(line_of "\[stop-instances\] .*\[${ids[1]}\]")
expect_ok "pause: the gateway is stopped first, then the nodes" "$([ -n "$gwstop" ] && [ -n "$nstop" ] && [ "$gwstop" -lt "$nstop" ] && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "pause: waits for stopped" "$(grep -q 'instance-stopped' "$STUB_LOG" && echo true || echo false)"
expect_ok "pause: all three nodes are stopped" "$(for i in 1 2 3; do grep '\[stop-instances\]' "$STUB_LOG" | grep -q "${ids[$i]}" || { echo false; exit; }; done; echo true)"
expect_ok "pause: nothing is terminated, modified or pushed; no ssh, terraform" "$(! grep -Eq 'terminate|modify|push-node-tokens|FORBIDDEN' "$STUB_LOG" "$sb/out" && echo true || echo false)"
expect_ok "pause: billing note names the Elastic IP and EBS" "$(grep -q 'Elastic IP' "$sb/out" && grep -q 'EBS' "$sb/out" && echo true || echo false)" "$(cat "$sb/out")"
expect_ok "pause: no token in any logged line or output" "$(! grep -Eiq 'TESTTOKEN|\.token' "$STUB_LOG" "$sb/out" && no_secret_anywhere && echo true || echo false)"
expect_ok "pause: no set -x in the script" "$(! grep -Eq 'set[[:space:]]+-[a-z]*x|xtrace' "$script" && echo true || echo false)"

setup; run resume "$GW" "${ids[1]}" "${ids[2]}" "${ids[3]}"
expect_ok "resume: exit 0" "$(b2s "$rc")" "rc=$rc: $(cat "$sb/out")"
nstart=$(line_of "\[start-instances\] .*\[${ids[1]}\]"); nwait=$(line_of 'instance-status-ok'); gwstart=$(line_of "\[start-instances\] .*\[$GW\]")
expect_ok "resume: nodes start, then wait for status ok, then the gateway starts" "$([ -n "$nstart" ] && [ -n "$nwait" ] && [ -n "$gwstart" ] && [ "$nstart" -lt "$nwait" ] && [ "$nwait" -lt "$gwstart" ] && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "resume: no terminate, modify, push-node-tokens (tokens survive a stop)" "$(! grep -Eq 'terminate|modify|push-node-tokens' "$STUB_LOG" "$sb/out" && echo true || echo false)"

setup; run pause --dry-run "$GW" "${ids[1]}" "${ids[2]}" "${ids[3]}"
expect_ok "pause --dry-run: exit 0, no stop or start, no aws call at all" "$([ "$rc" = 0 ] && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "pause --dry-run: prints what it would do" "$(grep -q 'stop-instances' "$sb/out" && echo true || echo false)"
setup; run resume --dry-run "$GW" "${ids[1]}" "${ids[2]}" "${ids[3]}"
expect_ok "resume --dry-run: no start" "$([ "$rc" = 0 ] && ! grep -Eq 'start-instances|stop-instances' "$STUB_LOG" && echo true || echo false)"

refuses() { expect_ok "refuses: $1" "$([ "$rc" = 1 ] && grep -q 'REFUSED:' "$sb/out" && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)" "rc=$rc: $(cat "$sb/out")"; }
setup; unset AWS_PROFILE; run pause "$GW" "${ids[@]:1}"; refuses "no profile"
setup; export AWS_ACCESS_KEY_ID=AKIAEXAMPLE; run pause "$GW" "${ids[@]:1}"; refuses "static access key"; unset AWS_ACCESS_KEY_ID
setup; export CI=true; run pause "$GW" "${ids[@]:1}"; refuses "CI is set"; unset CI
setup; run pause "$GW" "${ids[1]}" "${ids[2]}"; refuses "only two node ids"
setup; run pause; refuses "no ids"
setup; run pause "$GW" "${ids[1]}" "bad" "${ids[3]}"; refuses "malformed node id"
setup; run pause "$GW" "${ids[1]}" "${ids[1]}" "${ids[3]}"; refuses "duplicate node id"
setup; run pause "${ids[1]}" "${ids[1]}" "${ids[2]}" "${ids[3]}"; refuses "gateway id equal to a node id"
setup; run explode "$GW" "${ids[@]:1}"; refuses "unknown action"
setup; run "$GW" "${ids[@]:1}"; refuses "no action"

echo "demo-pause_test: $fails failure(s)"
[ "$fails" -eq 0 ]
