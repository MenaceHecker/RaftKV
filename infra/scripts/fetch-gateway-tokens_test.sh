#!/usr/bin/env bash
# Tests for fetch-gateway-tokens.sh with a STUB aws (see _testlib.sh): nothing touches AWS.
set -uo pipefail
. "$(dirname "$0")/_testlib.sh"
script="$here/fetch-gateway-tokens.sh"

setup() { new_sandbox; export RUNTIME_DIR="$sb/run"; mkdir -p "$RUNTIME_DIR"; chmod 0700 "$RUNTIME_DIR"; }

# --- success ---------------------------------------------------------------------------------
setup
run_script bash "$script"
expect_ok "success: exit 0" "$([ "$rc" = 0 ] && echo true || echo false)" "rc=$rc: $(cat "$sb/out")"
expect_ok "admin token file holds the parameter value" "$([ "$(cat "$RUNTIME_DIR/admin.token" 2>/dev/null)" = "$TOK_ADMIN" ] && echo true || echo false)"
expect_ok "node token file holds the parameter value" "$([ "$(cat "$RUNTIME_DIR/node.token" 2>/dev/null)" = "$TOK_NODE" ] && echo true || echo false)"
expect_ok "admin token file is mode 600" "$([ "$(mode_of "$RUNTIME_DIR/admin.token" 2>/dev/null)" = 600 ] && echo true || echo false)"
expect_ok "node token file is mode 600" "$([ "$(mode_of "$RUNTIME_DIR/node.token" 2>/dev/null)" = 600 ] && echo true || echo false)"
expect_ok "files are chowned to the gateway user" "$([ "$(grep -c '^chown \[raftkv-gw:raftkv-gw\]' "$STUB_LOG")" = 2 ] && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "no token in any logged command line, output or temp file" "$(no_secret_anywhere && echo true || echo false)"
expect_ok "only the two exact parameter names are read, with decryption, no --value" \
  "$([ "$(grep -c '^aws \[ssm\] \[get-parameter\]' "$STUB_LOG")" = 2 ] && grep -q '\[/raftkv/admin-token\]' "$STUB_LOG" && grep -q '\[/raftkv/node-admin-token\]' "$STUB_LOG" && ! grep -q '/raftkv/cluster-token' "$STUB_LOG" && ! grep -q -- '--value' "$STUB_LOG" && grep -q -- '--with-decryption' "$STUB_LOG" && echo true || echo false)" "$(cat "$STUB_LOG")"
expect_ok "no leftover temp or partial files in the runtime dir" "$([ "$(ls -A "$RUNTIME_DIR" | sort | tr '\n' ' ')" = "admin.token node.token " ] && echo true || echo false)" "$(ls -A "$RUNTIME_DIR")"
expect_ok "never traces (no set -x in the script)" "$(! grep -Eq 'set[[:space:]]+-[a-z]*x|xtrace' "$script" && echo true || echo false)"

# --- fail closed -----------------------------------------------------------------------------
setup; export STUB_FAIL="/raftkv/node-admin-token"
run_script bash "$script"
expect_ok "second parameter missing: non-zero exit" "$([ "$rc" != 0 ] && echo true || echo false)"
expect_ok "second parameter missing: no token file left, not even the first" "$([ -z "$(ls -A "$RUNTIME_DIR")" ] && echo true || echo false)" "$(ls -A "$RUNTIME_DIR")"
expect_ok "second parameter missing: no token in log or output" "$(no_secret_anywhere && echo true || echo false)"

setup; export STUB_EMPTY=admin
run_script bash "$script"
expect_ok "empty parameter value: non-zero exit and nothing left" "$([ "$rc" != 0 ] && [ -z "$(ls -A "$RUNTIME_DIR")" ] && echo true || echo false)"
unset STUB_EMPTY

setup; export STUB_TOK_NODE_OVERRIDE="$TOK_ADMIN"
run_script bash "$script"
expect_ok "admin and node token equal: refused, nothing left (the gateway would refuse them anyway)" "$([ "$rc" != 0 ] && [ -z "$(ls -A "$RUNTIME_DIR")" ] && echo true || echo false)"
unset STUB_TOK_NODE_OVERRIDE

setup; rm -rf "$RUNTIME_DIR"
run_script bash "$script"
expect_ok "runtime dir missing: refused (it must be the tmpfs RuntimeDirectory, never created here)" "$([ "$rc" != 0 ] && [ ! -e "$RUNTIME_DIR" ] && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)"

setup; chmod 0755 "$RUNTIME_DIR"
run_script bash "$script"
expect_ok "runtime dir readable by others: refused before any aws call" "$([ "$rc" != 0 ] && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)"

setup; export RUNTIME_DIR="relative/dir"
run_script bash "$script"
expect_ok "relative RUNTIME_DIR refused" "$([ "$rc" != 0 ] && [ "$(grep -c '^aws' "$STUB_LOG")" = 0 ] && echo true || echo false)"

echo "fetch-gateway-tokens_test: $fails failure(s)"
[ "$fails" -eq 0 ]
