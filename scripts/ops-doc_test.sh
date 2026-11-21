#!/usr/bin/env bash
# Tests for docs/OPS.md (D9): the eight incident sections, the plain "no backups" statement, only scripts that
# exist, no secret-like literal, and the unverified wire-compatibility statement.
set -uo pipefail
. "$(dirname "$0")/ops_testlib.sh"
doc="$repo/docs/OPS.md"

expect_ok "docs/OPS.md exists" "$([ -s "$doc" ] && echo true || echo false)"
for h in "Node down" "Leader flapping" "Gateway unreachable" "Certificate renewal failure" "Token rotation" "Cost alarms" "Stuck chaos slot" "Backups"; do
  expect_ok "heading: $h" "$(grep -qE "^## $h" "$doc" 2>/dev/null && echo true || echo false)"
done
expect_ok "says plainly there are no backups" "$(grep -qi 'no backups' "$doc" 2>/dev/null && echo true || echo false)"
expect_ok "states that mixed-version wire compatibility is UNVERIFIED" "$(grep -qi 'wire compatibility' "$doc" 2>/dev/null && grep -q 'UNVERIFIED' "$doc" && echo true || echo false)"
expect_ok "cost alarms name 20, 30 and 40" "$(sed -n '/^## Cost alarms/,/^## Stuck/p' "$doc" 2>/dev/null | grep -q '\$20' && sed -n '/^## Cost alarms/,/^## Stuck/p' "$doc" | grep -q '\$30' && sed -n '/^## Cost alarms/,/^## Stuck/p' "$doc" | grep -q '\$40' && echo true || echo false)"
expect_ok "token rotation names rolling-update.sh --restart-only and a gateway reboot" "$(sed -n '/^## Token rotation/,/^## Cost/p' "$doc" 2>/dev/null | grep -q 'rolling-update.sh --restart-only' && sed -n '/^## Token rotation/,/^## Cost/p' "$doc" | grep -qi 'reboot the gateway' && echo true || echo false)"

missing=""
for s in $(grep -oE '(infra/)?scripts/[A-Za-z0-9_.-]+\.sh' "$doc" 2>/dev/null | sort -u); do
  [ -f "$repo/$s" ] || missing="$missing $s"
done
expect_ok "every script mentioned exists" "$([ -z "$missing" ] && [ -n "$(grep -oE 'scripts/[A-Za-z0-9_.-]+\.sh' "$doc" 2>/dev/null)" ] && echo true || echo false)" "missing:$missing"
expect_ok "no 40+ character secret-like literal" "$(! grep -Eq '[A-Za-z0-9+/=_-]{40,}' "$doc" 2>/dev/null && echo true || echo false)" "$(grep -oE '[A-Za-z0-9+/=_-]{40,}' "$doc" 2>/dev/null | head -2)"
expect_ok "no account id, access key, or token value" "$(! grep -Eq 'AKIA[A-Z0-9]{12,}|[0-9]{12}|TESTTOKEN' "$doc" 2>/dev/null && echo true || echo false)"

echo "ops-doc_test: $fails failure(s)"
[ "$fails" -eq 0 ]
