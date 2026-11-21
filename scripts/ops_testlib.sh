#!/usr/bin/env bash
# Shared helpers for the ops script tests (D9): sourced, not run. Builds on infra/scripts/_testlib.sh
# (pass/fail/expect_ok, TOK_* canaries, no_secret_anywhere) and adds a sandbox with STUB aws, sleep,
# curl, and FORBIDDEN loggers for ssh, scp, terraform and raftkvctl, all first on PATH. Nothing here can
# reach AWS or a host.
#
# The stub `aws ec2-instance-connect ssh` does NOT execute the remote command. It logs
# `remote(<instance id>): <command>` and answers from a small cluster model in $sb/cluster/<slot>/:
# binary sha, down polls left, broken flag. Test knobs (environment, read by the stub only):
#   STUB_LEADER=n      leader slot at start (default 1)
#   STUB_FAIL_S3=n     node n: the staging `s3 cp` fails (remote exit 11)
#   STUB_FAIL_SHA=n    node n: the staged file does not match the hash (remote exit 12)
#   STUB_FAIL_MANIFEST=n  node n: the bucket MANIFEST disagrees (exit 13)
#   STUB_UNHEALTHY=n   node n never becomes healthy on a changed binary
#   STUB_FAIL_ROLLBACK=n  node n is also never healthy after the rollback restore
#   STUB_DOWN_POLLS=k  health polls that fail after a restart (default 2)
#   STUB_LEADER_MOVES=k + STUB_MOVE_TO=m  after the k-th restart leadership moves to node m
#   STUB_DEGRADED=n    node n is 100 entries behind;  STUB_PRE_DOWN=n  node n never answers
#   STUB_NOLEADER=1    every node reports leader 0;  STUB_WRONG_ID=n  node n reports another id
#   STUB_FAIL_SWAP=n   node n: the swap command fails before any change (live binary untouched)
#   STUB_NO_CURL=1     curl is missing on the nodes (remote exit 127)
# Every restart or stop counts the OTHER nodes that are not healthy and writes `two-down` to
# $sb/violations. Tests assert that file is empty.
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/infra/scripts/_testlib.sh"
repo="$(cd "$here/../.." && pwd)"
ids=(x i-0aaa1111bbbb2222c i-0aaa1111bbbb2222d i-0aaa1111bbbb2222e)
OLD_SHA=$(printf 'old' | { sha256sum 2>/dev/null || shasum -a 256; } | cut -d' ' -f1)
write_re='s3 cp| mv |systemctl|cp -p|install |rm -f'

sha_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

new_ops_sandbox() {
  sb=$(mktemp -d "$work/sb.XXXXXX")
  mkdir -p "$sb/bin" "$sb/tmp" "$sb/cluster"
  STUB_LOG="$sb/calls.log"; : > "$STUB_LOG"; : > "$sb/violations"
  export STUB_LOG TMPDIR="$sb/tmp" STUB_SANDBOX="$sb" AWS_PROFILE=stub-profile
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN CI GITHUB_ACTIONS \
    STUB_LEADER STUB_FAIL_S3 STUB_FAIL_SHA STUB_FAIL_MANIFEST STUB_UNHEALTHY STUB_FAIL_ROLLBACK STUB_DOWN_POLLS \
    STUB_LEADER_MOVES STUB_MOVE_TO STUB_DEGRADED STUB_PRE_DOWN STUB_NOLEADER STUB_WRONG_ID STUB_NO_CURL \
    STUB_VERSIONS STUB_GW_BODY STUB_FAIL_SWAP
  for n in 1 2 3; do
    mkdir -p "$sb/cluster/$n"
    printf '%s' "$OLD_SHA" > "$sb/cluster/$n/sha"; printf '%s' "$OLD_SHA" > "$sb/cluster/$n/sha0"
    echo 0 > "$sb/cluster/$n/down"; echo 0 > "$sb/cluster/$n/broken"; : > "$sb/cluster/$n/staged"
  done
  echo 0 > "$sb/cluster/restarts"
  cat > "$sb/bin/aws" <<'STUB'
#!/usr/bin/env bash
# STUB aws: logs argv (one call per line), models the cluster, never prints an environment value.
printf 'aws' >> "$STUB_LOG"; printf ' [%s]' "$@" >> "$STUB_LOG"; printf '\n' >> "$STUB_LOG"
c="$STUB_SANDBOX/cluster"
slot_of() { case "$1" in *c) echo 1 ;; *d) echo 2 ;; *e) echo 3 ;; *) echo 0 ;; esac; }
leader() { [ -f "$c/leader" ] && cat "$c/leader" || echo "${STUB_LEADER:-1}"; }
healthy() { # healthy <slot>: 0 if it would answer now
  [ "$(cat "$c/$1/down")" = 0 ] && [ "$(cat "$c/$1/broken")" = 0 ] && [ "${STUB_PRE_DOWN:-}" != "$1" ]
}
others_bad() { local m bad=0; for m in 1 2 3; do [ "$m" != "$1" ] && ! healthy "$m" && bad=$((bad + 1)); done; echo "$bad"; }
violation_check() { [ "$(others_bad "$1")" = 0 ] || echo "two-down: $2 on node $1" >> "$STUB_SANDBOX/violations"; }
set_broken() { # set_broken <slot>: after a restart, is the node permanently unhealthy?
  local n=$1 b=0
  [ "${STUB_UNHEALTHY:-}" = "$n" ] && [ "$(cat "$c/$n/sha")" != "$(cat "$c/$n/sha0")" ] && b=1
  [ "${STUB_FAIL_ROLLBACK:-}" = "$n" ] && [ -f "$c/$n/rolledback" ] && b=1
  echo "$b" > "$c/$n/broken"
}
remote_cmd() { # remote_cmd <slot> <command>
  local n=$1 cmd=$2 L
  case "$cmd" in
    *"s3 cp"*)
      [ "${STUB_FAIL_S3:-}" = "$n" ] && { echo "fatal error: An error occurred (403) when calling the HeadObject operation: Forbidden" >&2; exit 11; }
      [ "${STUB_FAIL_SHA:-}" = "$n" ] && { echo "sha256sum: WARNING: 1 computed checksum did NOT match" >&2; exit 12; }
      [ "${STUB_FAIL_MANIFEST:-}" = "$n" ] && exit 13
      printf '%s' "$cmd" | sed -n "s/.*echo '\([0-9a-f]\{64\}\)  .*/\1/p" > "$c/$n/staged"; exit 0 ;;
    *"cp -p /usr/local/bin/raftkvd.prev"*)
      cp "$c/$n/prev" "$c/$n/sha"; : > "$c/$n/rolledback"; exit 0 ;;
    *"cp -p /usr/local/bin/raftkvd /usr/local/bin/raftkvd.prev"*)
      [ "${STUB_FAIL_SWAP:-}" = "$n" ] && exit 1
      cp "$c/$n/sha" "$c/$n/prev"; cp "$c/$n/staged" "$c/$n/sha"; exit 0 ;;
    *"rm -f"*) : > "$c/$n/staged"; exit 0 ;;
    *"systemctl restart"*)
      violation_check "$n" restart
      echo $(( $(cat "$c/restarts") + 1 )) > "$c/restarts"
      echo "${STUB_DOWN_POLLS:-2}" > "$c/$n/down"
      set_broken "$n"
      [ "$(leader)" = "$n" ] && { for m in 1 2 3; do [ "$m" != "$n" ] && { echo "$m" > "$c/leader"; break; }; done; }
      [ -n "${STUB_LEADER_MOVES:-}" ] && [ "$(cat "$c/restarts")" = "$STUB_LEADER_MOVES" ] && echo "${STUB_MOVE_TO:-1}" > "$c/leader"
      exit 0 ;;
    *"systemctl stop"*) violation_check "$n" stop; echo 99 > "$c/$n/down"; exit 0 ;;
    *"sha256sum /usr/local/bin/raftkvd"*) cat "$c/$n/sha"; echo; exit 0 ;;
    *"/health"*)
      [ -n "${STUB_NO_CURL:-}" ] && { echo "curl: not found" >&2; exit 127; }
      if [ "$(cat "$c/$n/down")" -gt 0 ]; then echo $(( $(cat "$c/$n/down") - 1 )) > "$c/$n/down"; echo "curl: (7) Failed to connect" >&2; exit 7; fi
      healthy "$n" || { echo "curl: (7) Failed to connect" >&2; exit 7; }
      L=$(leader); [ -n "${STUB_NOLEADER:-}" ] && L=0
      local st=Follower app=1000 hid=$n
      [ "$L" = "$n" ] && st=Leader
      [ "${STUB_DEGRADED:-}" = "$n" ] && app=900
      [ "${STUB_WRONG_ID:-}" = "$n" ] && hid=$(( n % 3 + 1 ))
      printf '{"id":%s,"state":"%s","term":4,"leader":%s,"commit":%s,"applied":%s}\n' "$hid" "$st" "$L" "$app" "$app"; exit 0 ;;
    *) echo "stub: unrecognised remote command" >&2; exit 2 ;;
  esac
}
case "${1:-} ${2:-}" in
  "ec2-instance-connect ssh")
    id=""; remote=""; seen=0; prev=""
    for a in "$@"; do
      if [ "$seen" = 1 ]; then remote="${remote:+$remote }$a"; elif [ "$a" = "--" ]; then seen=1; fi
      [ "$prev" = "--instance-id" ] && id=$a; prev=$a
    done
    [ -n "$remote" ] || { echo "stub: no remote command after --" >&2; exit 2; }
    printf 'remote(%s): %s\n' "$id" "$remote" >> "$STUB_LOG"
    slot=$(slot_of "$id"); [ "$slot" != 0 ] || { echo "stub: unknown instance" >&2; exit 255; }
    remote_cmd "$slot" "$remote" ;;
  "ec2 stop-instances"|"ec2 start-instances"|"ec2 wait") exit 0 ;;
  "sts get-caller-identity") echo '{"Arn":"arn:aws:iam::000000000000:role/stub"}'; exit 0 ;;
  "s3api list-object-versions") echo "${STUB_VERSIONS:-7}"; exit 0 ;;
  "ssm describe-parameters") printf '/raftkv/admin-token\n/raftkv/node-admin-token\n/raftkv/cluster-token\n'; exit 0 ;;
  *) echo "stub aws: unexpected call" >&2; exit 2 ;;
esac
STUB
  for b in ssh scp terraform raftkvctl; do
    cat > "$sb/bin/$b" <<'STUB'
#!/usr/bin/env bash
printf 'FORBIDDEN %s\n' "$(basename "$0")" >> "$STUB_LOG"; echo "stub: $(basename "$0") must never be called" >&2; exit 99
STUB
  done
  cat > "$sb/bin/sleep" <<'STUB'
#!/usr/bin/env bash
printf 'sleep [%s]\n' "$*" >> "$STUB_LOG"
STUB
  cat > "$sb/bin/curl" <<'STUB'
#!/usr/bin/env bash
printf 'curl' >> "$STUB_LOG"; printf ' [%s]' "$@" >> "$STUB_LOG"; printf '\n' >> "$STUB_LOG"
if [ -n "${STUB_GW_BODY:-}" ]; then echo "$STUB_GW_BODY"; else echo '{"leaderId":1,"chaos":{"state":"idle"}}'; fi
STUB
  chmod +x "$sb"/bin/*
  PATH="$sb/bin:/usr/bin:/bin"; export PATH
}

# make_dist <dir> <version>: a fake dist/<version>/ (clean build). Sets TARGET_SHA and dist.
make_dist() {
  dist="$1/$2"; mkdir -p "$dist"
  printf 'new binary %s' "$2" > "$dist/raftkv-server-linux-arm64"
  printf 'gateway %s' "$2" > "$dist/gateway-linux-arm64"
  TARGET_SHA=$(sha_of "$dist/raftkv-server-linux-arm64")
  echo "$TARGET_SHA  raftkv-server-linux-arm64" > "$dist/raftkv-server-linux-arm64.sha256"
  echo "$(sha_of "$dist/gateway-linux-arm64")  gateway-linux-arm64" > "$dist/gateway-linux-arm64.sha256"
  { echo "version=$2"; echo "commit=abc1234"; echo "date=2026-01-01T00:00:00Z"; echo "dirty=false"
    echo "sha256.raftkv-server-linux-arm64=$TARGET_SHA"
    echo "sha256.gateway-linux-arm64=$(sha_of "$dist/gateway-linux-arm64")"; } > "$dist/MANIFEST"
}

run_script() { rc=0; "$@" > "$sb/out" 2>&1 < /dev/null || rc=$?; }

remote_log() { grep '^remote(' "$STUB_LOG"; }
slot_list() { sed -e 's/^remote(i-0aaa1111bbbb2222\(.\)).*/\1/' -e 's/c/1/;s/d/2/;s/e/3/' | tr '\n' ' ' | sed 's/ $//'; }
restart_order() { remote_log | grep 'systemctl restart' | slot_list; }
writes_count() { remote_log | grep -Ec "$write_re" || true; }
