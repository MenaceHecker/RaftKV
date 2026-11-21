#!/usr/bin/env bash
# Tests for scripts/demo-down.sh (PRINT ONLY: it never calls terraform) with STUB aws/terraform (see ops_testlib.sh).
set -uo pipefail
. "$(dirname "$0")/ops_testlib.sh"
script="$repo/scripts/demo-down.sh"
setup() { new_ops_sandbox; }
run() { run_script bash "$script" "$@"; }
std=(--state-bucket raftkv-state-stub --artifacts-bucket raftkv-artifacts-stub --domain example.test)
b2s() { [ "$1" = 0 ] && echo true || echo false; }

setup; export STUB_VERSIONS=7; run "${std[@]}"
expect_ok "non-empty buckets: exit 0" "$(b2s "$rc")" "rc=$rc: $(cat "$sb/out")"
expect_ok "terraform is never called (stub)" "$(! grep -q FORBIDDEN "$STUB_LOG" && echo true || echo false)"
expect_ok "only read verbs: sts, s3api list-object-versions, ssm describe-parameters" \
  "$(! grep '^aws' "$STUB_LOG" | grep -Eqv '^aws \[(sts)\] \[get-caller-identity\]|^aws \[s3api\] \[list-object-versions\]|^aws \[ssm\] \[describe-parameters\]' && grep -q 'list-object-versions' "$STUB_LOG" && grep -q 'describe-parameters' "$STUB_LOG" && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "no delete, rm, put, get-parameter or --with-decryption in any call" "$(! grep -Eq 'delete|\brm\b|put-|get-parameter|with-decryption' "$STUB_LOG" && echo true || echo false)"
expect_ok "both buckets are inspected" "$(grep -q 'raftkv-state-stub' "$STUB_LOG" && grep -q 'raftkv-artifacts-stub' "$STUB_LOG" && echo true || echo false)"
out=$(cat "$sb/out")
pos() { printf '%s\n' "$out" | grep -n -- "$1" | head -1 | cut -d: -f1; }
p1=$(pos 'STEP 1'); p2=$(pos 'STEP 2'); p3=$(pos 'STEP 3'); p4=$(pos 'STEP 4'); p5=$(pos 'STEP 5'); p6=$(pos 'STEP 6')
expect_ok "the six steps are printed in order 1..6" "$([ -n "$p1" ] && [ "$p1" -lt "${p2:-0}" ] && [ "${p2:-0}" -lt "${p3:-0}" ] && [ "${p3:-0}" -lt "${p4:-0}" ] && [ "${p4:-0}" -lt "${p5:-0}" ] && [ "${p5:-0}" -lt "${p6:-0}" ] && echo true || echo false)" "$out"
step() { printf '%s\n' "$out" | sed -n "/STEP $1/,/STEP $(( $1 + 1 ))/p"; }
expect_ok "step 1 is the terraform destroy in infra/envs/demo; step 3 the one in infra/bootstrap" \
  "$(step 1 | grep -q 'terraform destroy' && step 1 | grep -q 'infra/envs/demo' && step 3 | grep -q 'terraform destroy' && step 3 | grep -q 'infra/bootstrap' && echo true || echo false)"
expect_ok "step 2 covers versions and delete markers; step 4 the three parameters; step 5 the DNS record" \
  "$(step 2 | grep -qi 'delete markers' && step 4 | grep -q '/raftkv/admin-token' && step 4 | grep -q '/raftkv/cluster-token' && step 5 | grep -q 'api.example.test' && echo true || echo false)"
expect_ok "step 6 leaves the dashboard in replay mode and updates the README" "$(printf '%s\n' "$out" | sed -n '/STEP 6/,$p' | grep -qi 'replay' && echo true || echo false)"
expect_ok "live status is shown (object versions remain, parameters found)" "$(grep -qi 'versions remain' "$sb/out" && grep -qi 'parameter' "$sb/out" && echo true || echo false)" "$out"
expect_ok "the state bucket is emptied only after step 1" "$(step 2 | grep -qi 'state bucket' && step 2 | grep -qi 'after step 1' && echo true || echo false)"
expect_ok "no token value and no account id in the output" "$(! grep -Eq 'TESTTOKEN|000000000000' "$sb/out" "$STUB_LOG" && echo true || echo false)"

setup; export STUB_VERSIONS=0; run "${std[@]}"
expect_ok "empty buckets: exit 0 as well" "$(b2s "$rc")"

refuses() { expect_ok "refuses: $1" "$([ "$rc" = 1 ] && grep -q 'REFUSED:' "$sb/out" && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)" "rc=$rc: $(cat "$sb/out")"; }
setup; unset AWS_PROFILE; run "${std[@]}"; refuses "no profile"
setup; export AWS_SECRET_ACCESS_KEY=x; run "${std[@]}"; refuses "static key"; unset AWS_SECRET_ACCESS_KEY
setup; export CI=true; run "${std[@]}"; refuses "CI is set"; unset CI
setup; run --artifacts-bucket raftkv-artifacts-stub --domain example.test; refuses "no state bucket"
setup; run --state-bucket raftkv-state-stub --domain example.test; refuses "no artifacts bucket"
setup; run --state-bucket raftkv-state-stub --artifacts-bucket raftkv-artifacts-stub; refuses "no domain"
setup; run --state-bucket 'Bad;Bucket' --artifacts-bucket raftkv-artifacts-stub --domain example.test; refuses "bad bucket name"
setup; run --state-bucket same-bucket-name --artifacts-bucket same-bucket-name --domain example.test; refuses "the two buckets are the same"
setup; run --state-bucket raftkv-state-stub --artifacts-bucket raftkv-artifacts-stub --domain 'x;rm -rf y'; refuses "bad domain"
setup; run "${std[@]}" --bogus; refuses "unknown flag"

# --- static: no script in the repo executes terraform apply or destroy ------------------------------------------
cd "$repo" || exit 1
offenders=$(for f in scripts/*.sh infra/scripts/*.sh; do
  grep -nEv '^[[:space:]]*#' "$f" | grep -E '(^|[0-9]+:)[[:space:]]*(exec[[:space:]]+|command[[:space:]]+|sudo[[:space:]]+)?terraform[[:space:]]+(-[^ ]+[[:space:]]+)*(apply|destroy)|[;&|(`][[:space:]]*(exec[[:space:]]+|command[[:space:]]+)?terraform[[:space:]]+(-[^ ]+[[:space:]]+)*(apply|destroy)|\$\([[:space:]]*terraform[[:space:]]+(apply|destroy)' | sed "s|^|$f:|"
done)
expect_ok "static: no script line executes terraform apply or destroy" "$([ -z "$offenders" ] && echo true || echo false)" "$offenders"
expect_ok "static: demo-down.sh never mentions calling terraform other than in printed text (no bare terraform command)" "$(! grep -Ev '^[[:space:]]*#' "$script" | grep -Eq '^[[:space:]]*(command[[:space:]]+)?terraform[[:space:]]' && echo true || echo false)"

echo "demo-down_test: $fails failure(s)"
[ "$fails" -eq 0 ]
