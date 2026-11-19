#!/usr/bin/env bash
# Shared helpers for the infra shell tests (sourced, not run). Every test builds a sandbox with
# STUB aws, ssh, sudo, chown and systemctl first on PATH: nothing here can reach AWS or a host.
# The stubs log one line per call to $STUB_LOG, never a value read from the environment.
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
fails=0

pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }
expect_ok() { if [ "$2" = true ]; then pass "$1"; else fail "$1" "${3:-assertion false}"; fi; }

# The fake token values. They are what the stub `aws ssm get-parameter` prints; the tests assert
# they never show up in a log line, in script output or in a temp file.
TOK_ADMIN="TESTTOKEN-admin-3c1f9a7e5b2d4086a1c7"
TOK_NODE="TESTTOKEN-node-8d2e6b0f4a1c7395e2b8"
TOK_CLUSTER="TESTTOKEN-cluster-5a9c3e1b7d0f2468c4a6"

mode_of() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }

# new_sandbox: creates $sb with stub binaries in $sb/bin and exports STUB_LOG, STUB_FAIL.
new_sandbox() {
  sb=$(mktemp -d "$work/sb.XXXXXX")
  mkdir -p "$sb/bin" "$sb/tmp" "$sb/remote"
  STUB_LOG="$sb/calls.log"; : > "$STUB_LOG"
  export STUB_LOG TMPDIR="$sb/tmp" STUB_SANDBOX="$sb"
  unset STUB_FAIL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN CI GITHUB_ACTIONS
  cat > "$sb/bin/aws" <<'STUB'
#!/usr/bin/env bash
# STUB aws. Logs argv (one call per line). ssm get-parameter prints a fake token; the
# ec2-instance-connect ssh call runs the remote command LOCALLY in the sandbox with stdin passed through.
printf 'aws' >> "$STUB_LOG"; printf ' [%s]' "$@" >> "$STUB_LOG"; printf '\n' >> "$STUB_LOG"
case "${1:-} ${2:-}" in
  "ssm get-parameter")
    name=""; while [ "$#" -gt 0 ]; do [ "$1" = "--name" ] && name=$2; shift; done
    case "$STUB_FAIL" in *"$name"*) echo "An error occurred (ParameterNotFound)" >&2; exit 254 ;; esac
    case "$name" in
      /raftkv/admin-token) [ "${STUB_EMPTY:-}" = admin ] && { echo; exit 0; }; printf '%s\n' "$STUB_TOK_ADMIN" ;;
      /raftkv/node-admin-token) printf '%s\n' "${STUB_TOK_NODE_OVERRIDE:-$STUB_TOK_NODE}" ;;
      /raftkv/cluster-token) printf '%s\n' "${STUB_TOK_CLUSTER_OVERRIDE:-$STUB_TOK_CLUSTER}" ;;
      *) echo "unknown parameter" >&2; exit 254 ;;
    esac ;;
  "ec2-instance-connect ssh")
    id=""; remote=""; seen=0
    for a in "$@"; do
      if [ "$seen" = 1 ]; then remote="${remote:+$remote }$a"; elif [ "$a" = "--" ]; then seen=1; fi
    done
    prev=""; for a in "$@"; do [ "$prev" = "--instance-id" ] && id=$a; prev=$a; done
    case "$STUB_FAIL" in *"$id"*) echo "connection refused" >&2; exit 255 ;; esac
    [ -n "$remote" ] || { echo "stub: no remote command after --" >&2; exit 2; }
    printf 'remote(%s): %s\n' "$id" "$remote" >> "$STUB_LOG.remote"
    exec bash -c "$remote" ;;
  *) echo "stub aws: unexpected call" >&2; exit 2 ;;
esac
STUB
  export STUB_TOK_ADMIN="$TOK_ADMIN" STUB_TOK_NODE="$TOK_NODE" STUB_TOK_CLUSTER="$TOK_CLUSTER"
  for b in ssh scp; do
    cat > "$sb/bin/$b" <<'STUB'
#!/usr/bin/env bash
printf 'FORBIDDEN %s\n' "$(basename "$0")" >> "$STUB_LOG"; echo "stub: direct ssh/scp is not allowed" >&2; exit 99
STUB
  done
  cat > "$sb/bin/sudo" <<'STUB'
#!/usr/bin/env bash
printf 'sudo' >> "$STUB_LOG"; printf ' [%s]' "$@" >> "$STUB_LOG"; printf '\n' >> "$STUB_LOG"; exec "$@"
STUB
  for b in chown systemctl; do
    cat > "$sb/bin/$b" <<'STUB'
#!/usr/bin/env bash
printf '%s' "$(basename "$0")" >> "$STUB_LOG"; printf ' [%s]' "$@" >> "$STUB_LOG"; printf '\n' >> "$STUB_LOG"
STUB
  done
  chmod +x "$sb"/bin/*
  # Stubs first, then only the system tools the scripts need. No real aws on this PATH.
  PATH="$sb/bin:/usr/bin:/bin"; export PATH
}

# run_script <script> [args...]: runs with the sandbox PATH, captures stdout+stderr in $sb/out, sets rc.
run_script() {
  rc=0
  "$@" > "$sb/out" 2>&1 < /dev/null || rc=$?
}

# no_secret_anywhere: none of the three token values appears in the call log, the script output,
# or any file under the sandbox temp dir.
no_secret_anywhere() {
  local t
  for t in "$TOK_ADMIN" "$TOK_NODE" "$TOK_CLUSTER"; do
    grep -qF -- "$t" "$STUB_LOG" "$sb/out" 2>/dev/null && return 1
    [ -z "$(find "$sb/tmp" -type f 2>/dev/null)" ] || return 1
  done
  return 0
}
