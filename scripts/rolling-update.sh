#!/usr/bin/env bash
# Rolling update (or restart) of the three Raft nodes, one at a time, health-gated, leader last
# (RUNBOOK 10.8, task D9). Run by the HUMAN on their own machine; never in CI. Binary only: unit and flag
# edits stay manual (docs/OPS.md).
#
#   AWS_PROFILE=<sso profile> scripts/rolling-update.sh --bucket <artifacts bucket> --dist dist/<version> \
#       [--dry-run] [--max-lag 5] [--timeout 180] [--gateway-url https://api.<domain>] <node1> <node2> <node3>
#   AWS_PROFILE=<sso profile> scripts/rolling-update.sh --restart-only [--dry-run] ... <node1> <node2> <node3>
#   (ids: terraform output -json node_instance_ids, in node order 1, 2, 3; --profile P sets AWS_PROFILE)
#
# Why a script: aws_instance.node has ignore_changes=[ami,user_data] (infra/modules/compute/nodes.tf), so
# `terraform apply` never changes a node binary. A restart reuses the WAL on the data volume; no instance
# is ever replaced here (a replacement wipes WAL and vote).
#
# Phases:
#   preflight (read only): all three nodes answer node-local /health over EC2 Instance Connect Endpoint,
#     each id equals its slot, exactly one Leader, nobody lags more than --max-lag; optional gateway check.
#   A. stage: every node that is not already at the target downloads the binary to /var/tmp and verifies
#     its sha256 (the hash comes from YOUR local dist/<version>/MANIFEST, never from the bucket; the bucket
#     MANIFEST is only cross-checked). No service is touched. Any failure: exit 2, cluster unchanged.
#   B. one node at a time, order recomputed from live /health before each step: followers ascending, the
#     leader last. Per node: other two healthy, cp -p live binary to raftkvd.prev, swap, restart, wait
#     until Follower/Leader with a known leader and applied within --max-lag. Strictly sequential.
#   A node already at the target is skipped (idempotent rerun). Downgrade = rerun with the OLD dist dir.
#
# Exit codes: 0 done or nothing to do; 1 refused or preflight failed (nothing written); 2 staging failed
# (nothing restarted); 3 a node failed its health gate and was rolled back (or a gate failed before it was
# touched), later nodes untouched; 4 rollback itself failed: MANUAL (stop; look at the node).
#
# Needs `curl` on the nodes for /health (unverified on AL2023: the script fails clearly if it is missing).
# Wire compatibility between adjacent releases during the mixed-version window is an UNVERIFIED assumption.
set -euo pipefail
umask 077
set +x

refuse() { echo "rolling-update: REFUSED: $*" >&2; exit 1; }
die() { local rc=$1; shift; echo "rolling-update: $*" >&2; exit "$rc"; }

dry=0; restart_only=0; max_lag=5; timeout=180; dist=""; bucket=""; gw_url=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run) dry=1; shift ;;
    --restart-only) restart_only=1; shift ;;
    --max-lag|--timeout|--dist|--bucket|--gateway-url|--profile)
      [ "$#" -ge 2 ] || refuse "$1 needs a value"
      case "$1" in
        --max-lag) max_lag=$2 ;; --timeout) timeout=$2 ;; --dist) dist=$2 ;;
        --bucket) bucket=$2 ;; --gateway-url) gw_url=$2 ;; --profile) export AWS_PROFILE=$2 ;;
      esac
      shift 2 ;;
    --) shift; break ;;
    -*) refuse "unknown flag $1" ;;
    *) break ;;
  esac
done

[ "$#" -eq 3 ] || refuse "usage: rolling-update.sh [flags] <node1 instance id> <node2 instance id> <node3 instance id>"
id_re='^i-[0-9a-f]{8,17}$'
for id in "$@"; do [[ $id =~ $id_re ]] || refuse "'$id' is not an instance id (i-...)"; done
[ "$1" != "$2" ] && [ "$1" != "$3" ] && [ "$2" != "$3" ] || refuse "the three instance ids must be different"
ids=("" "$1" "$2" "$3")
[ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ] || refuse "CI is set; updates are run by a human, never in CI"
[ -n "${AWS_PROFILE:-}" ] || refuse "AWS_PROFILE is empty; set it (or pass --profile) to your SSO profile"
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
  [ -z "${!v:-}" ] || refuse "$v is set; no long-lived keys, use the SSO profile (unset $v)"
done
num_re='^[0-9]+$'
[[ $max_lag =~ $num_re ]] || refuse "--max-lag must be a whole number"
[[ $timeout =~ $num_re ]] && [ "$timeout" -ge 2 ] || refuse "--timeout must be a whole number of seconds, at least 2"
if [ -n "$gw_url" ]; then
  gw_re='^https?://[A-Za-z0-9.-]+(:[0-9]+)?$'
  [[ $gw_url =~ $gw_re ]] || refuse "--gateway-url must look like https://host (no path, no credentials)"
fi
command -v aws >/dev/null 2>&1 || refuse "the aws CLI v2 is required"

sha_local() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }
bin=raftkv-server-linux-arm64
version=""; target=""
if [ "$restart_only" = 0 ]; then
  [ -n "$dist" ] || refuse "--dist dist/<version> is required (or use --restart-only)"
  [ -n "$bucket" ] || refuse "--bucket <artifacts bucket> is required (or use --restart-only)"
  bucket_re='^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'
  [[ $bucket =~ $bucket_re ]] || refuse "'$bucket' is not a valid S3 bucket name"
  [ -d "$dist" ] || refuse "$dist is not a directory (missing)"
  dist=$(cd "$dist" && pwd)
  [ -f "$dist/MANIFEST" ] || refuse "missing MANIFEST in $dist"
  grep -q '^dirty=false$' "$dist/MANIFEST" || refuse "MANIFEST does not say dirty=false: dirty builds are never deployed"
  version=$(sed -n 's/^version=//p' "$dist/MANIFEST" | head -1)
  case "$version" in ""|*/*|*[[:space:]]*|*-dirty) refuse "bad version '$version' in MANIFEST" ;; esac
  target=$(sed -n "s/^sha256\.$bin=//p" "$dist/MANIFEST" | head -1)
  hex_re='^[0-9a-f]{64}$'
  [[ $target =~ $hex_re ]] || refuse "MANIFEST has no valid sha256.$bin"
  [ -f "$dist/$bin.sha256" ] || refuse "missing $bin.sha256 in $dist"
  [ "$(cut -d' ' -f1 "$dist/$bin.sha256")" = "$target" ] || refuse "$bin.sha256 disagrees with MANIFEST (inconsistent dist dir)"
  if [ -f "$dist/$bin" ]; then
    [ "$(sha_local "$dist/$bin")" = "$target" ] || refuse "$dist/$bin does not match its MANIFEST sha256"
  fi
fi
key="artifacts/raftkvd/$version/$bin"
mkey="artifacts/raftkvd/$version/MANIFEST"
live=/usr/local/bin/raftkvd
stage_path=/var/tmp/raftkvd.new

# --- remote commands (none carries a secret; the tokens are never read or touched here) ----------------------
CMD_HEALTH='command -v curl >/dev/null 2>&1 || { echo "curl: not found" >&2; exit 127; }; curl -fsS --max-time 3 http://127.0.0.1:2112/health'
CMD_SHA="sha256sum $live | cut -d' ' -f1"
CMD_STAGE="aws s3 cp --only-show-errors s3://$bucket/$key $stage_path || exit 11; echo '$target  $stage_path' | sha256sum -c - >/dev/null || exit 12; aws s3 cp --only-show-errors s3://$bucket/$mkey - | grep -qx 'sha256.$bin=$target' || exit 13"
CMD_CLEAN="rm -f $stage_path"
CMD_SWAP="echo '$target  $stage_path' | sha256sum -c - >/dev/null && sudo cp -p $live $live.prev && sudo install -m 0755 -o root -g root $stage_path $live.new && sudo mv $live.new $live"
CMD_ROLLBACK="sudo cp -p $live.prev $live.new && sudo mv $live.new $live"
CMD_RESTART="sudo systemctl restart raftkvd"

eice() { # eice <slot> <remote command>; stdin is never forwarded
  aws ec2-instance-connect ssh --instance-id "${ids[$1]}" --os-user ec2-user --connection-type eice -- "$2" < /dev/null
}
jnum() { printf '%s' "$2" | sed -n "s/.*\"$1\":\([0-9][0-9]*\).*/\1/p"; }
jstr() { printf '%s' "$2" | sed -n "s/.*\"$1\":\"\([A-Za-z]*\)\".*/\1/p"; }

st=(); ldr=(); app=(); hid=(); up=()
health_of() { # health_of <slot>: fills st/ldr/app/hid/up for that slot; up=0 if it did not answer
  local out rc=0
  out=$(eice "$1" "$CMD_HEALTH" 2>/dev/null) || rc=$?
  if [ "$rc" = 127 ]; then
    die 1 "REFUSED: curl is not installed on node $1; /health cannot be read (curl on AL2023 is unverified; install it or use a different probe)"
  fi
  if [ "$rc" != 0 ]; then up[$1]=0; st[$1]=down; ldr[$1]=0; app[$1]=0; hid[$1]=0; return 0; fi
  up[$1]=1; st[$1]=$(jstr state "$out"); ldr[$1]=$(jnum leader "$out"); app[$1]=$(jnum applied "$out"); hid[$1]=$(jnum id "$out")
  : "${ldr[$1]:=0}" "${app[$1]:=0}" "${hid[$1]:=0}"
}
snapshot() { local n; for n in 1 2 3; do health_of "$n"; done; }
cluster_problem() { # prints the reason the snapshot is not a healthy 3-node cluster, or nothing
  local n leaders=0 l=0
  for n in 1 2 3; do
    [ "${up[$n]}" = 1 ] || { echo "node $n does not answer /health"; return; }
    [ "${hid[$n]}" = "$n" ] || { echo "node $n reports id ${hid[$n]} (slot and id must match)"; return; }
    case "${st[$n]}" in Leader) leaders=$((leaders + 1)); l=$n ;; Follower) ;; *) echo "node $n is ${st[$n]}, not Follower or Leader"; return ;; esac
  done
  [ "$leaders" = 1 ] || { echo "expected exactly one Leader, saw $leaders"; return; }
  for n in 1 2 3; do
    [ "${ldr[$n]}" = "$l" ] || { echo "node $n reports leader ${ldr[$n]}, not $l"; return; }
    [ $((app[l] - app[n])) -le "$max_lag" ] || { echo "node $n lags by $((app[l] - app[n])) entries (--max-lag $max_lag)"; return; }
  done
}
leader_slot() { local n; for n in 1 2 3; do [ "${st[$n]}" = Leader ] && { echo "$n"; return; }; done; echo 0; }
next_node() { # next_node <done list>: followers ascending, leader last; prints a slot or nothing
  local n l; l=$(leader_slot)
  for n in 1 2 3; do case " $1 " in *" $n "*) continue ;; esac; [ "$n" != "$l" ] && { echo "$n"; return; }; done
  case " $1 " in *" $l "*) ;; *) echo "$l" ;; esac
}
wait_healthy() { # wait_healthy <slot>: 0 once Follower/Leader, leader known, applied within --max-lag
  local n=$1 i=0 polls=$((timeout / 2)) l
  while [ "$i" -lt "$polls" ]; do
    health_of "$n"
    if [ "${up[$n]}" = 1 ] && [ "${hid[$n]}" = "$n" ] && { [ "${st[$n]}" = Leader ] || [ "${st[$n]}" = Follower ]; } && [ "${ldr[$n]}" != 0 ]; then
      l=${ldr[$n]}
      if [ "$l" = "$n" ]; then return 0; fi
      health_of "$l"
      if [ "${up[$l]}" = 1 ] && [ $((app[l] - app[n])) -le "$max_lag" ]; then return 0; fi
    fi
    i=$((i + 1)); sleep 2
  done
  return 1
}

# --- preflight (read only) ---------------------------------------------------------------------------------------------
echo "preflight: reading /health from the three nodes (read only)"
snapshot
problem=$(cluster_problem)
[ -z "$problem" ] || refuse "cluster not ready: $problem; nothing was changed"
echo "preflight: leader is node $(leader_slot); all 3 nodes healthy, lag within $max_lag"
if [ -n "$gw_url" ]; then
  gw=$(curl -fsS --max-time 5 "$gw_url/v1/cluster") || refuse "could not read $gw_url/v1/cluster"
  case "$gw" in *'"recovering"'*) refuse "the gateway reports chaos state recovering (a node may be down); try again later" ;; esac
  echo "preflight: gateway reports no chaos in recovery"
fi

need=""; cur=()
if [ "$restart_only" = 0 ]; then
  for n in 1 2 3; do
    cur[$n]=$(eice "$n" "$CMD_SHA" 2>/dev/null | tr -d '[:space:]') || cur[$n]=""
    [ -n "${cur[$n]}" ] || refuse "could not read the binary hash on node $n; nothing was changed"
    [ "${cur[$n]}" = "$target" ] || need="$need $n"
  done
  need=${need# }
  echo "target: $version ($target)"
  if [ -z "$need" ]; then echo "done: all 3 nodes already run $version; nothing to do"; exit 0; fi
  echo "nodes to update: $need (the others already run the target)"
else
  echo "mode: restart only (no download, no swap)"
fi

if [ "$dry" = 1 ]; then
  echo "DRY RUN: no write command is issued. Order as seen now (recomputed live on a real run):"
  d=""; steps=0
  while [ "$steps" -lt 3 ]; do
    n=$(next_node "$d"); [ -n "$n" ] || break; d="$d $n"; steps=$((steps + 1))
    skip=0; [ "$restart_only" = 0 ] && [ "${cur[$n]}" = "$target" ] && skip=1
    if [ "$skip" = 1 ]; then echo "  node $n (${ids[$n]}): already at target, skipped"; continue; fi
    echo "  node $n (${ids[$n]}) [role now: ${st[$n]}]:"
    if [ "$restart_only" = 0 ]; then
      echo "    stage:   $CMD_STAGE"
      echo "    swap:    $CMD_SWAP"
    fi
    echo "    restart: $CMD_RESTART"
    echo "    then poll: $CMD_HEALTH  (every 2 s, up to $timeout s)"
    [ "$restart_only" = 0 ] && echo "    on failure: $CMD_ROLLBACK ; $CMD_RESTART"
  done
  exit 0
fi

# --- Phase A: stage and verify on every node that needs it; no service is touched --------------------------------------------
if [ "$restart_only" = 0 ]; then
  for n in $need; do
    echo "stage: node $n ($version)"
    rc=0; eice "$n" "$CMD_STAGE" || rc=$?
    if [ "$rc" != 0 ]; then
      for m in $need; do eice "$m" "$CMD_CLEAN" >/dev/null 2>&1 || true; done
      case "$rc" in
        11) msg="the S3 object s3://$bucket/$key is missing or not uploaded (the node role cannot list the bucket, so a missing key looks like access denied)" ;;
        12) msg="the sha256 of the staged file does not match the MANIFEST hash" ;;
        13) msg="the bucket MANIFEST does not carry the expected sha256 (wrong dist directory?)" ;;
        *) msg="staging failed (remote exit $rc)" ;;
      esac
      die 2 "node $n: $msg; nothing was restarted, every service is as it was"
    fi
  done
  echo "stage: all nodes verified; starting the rolling restart"
fi

# --- Phase B: one node at a time -------------------------------------------------------------------------------------------------
done_list=""; updated=0; skipped=0
while :; do
  snapshot
  problem=$(cluster_problem)
  n=$(next_node "$done_list")
  [ -n "$n" ] || break
  done_list="$done_list $n"
  if [ "$restart_only" = 0 ]; then
    now=$(eice "$n" "$CMD_SHA" 2>/dev/null | tr -d '[:space:]') || now=""
    if [ "$now" = "$target" ]; then echo "node $n: already at $version, skipped"; skipped=$((skipped + 1)); continue; fi
  fi
  [ -z "$problem" ] || die 3 "ABORT before node $n: cluster not healthy ($problem); node $n untouched, later nodes untouched"
  echo "node $n (${ids[$n]}), role ${st[$n]}: updating"
  if [ "$restart_only" = 0 ]; then
    rc=0; eice "$n" "$CMD_SWAP" || rc=$?
    if [ "$rc" != 0 ]; then
      now=$(eice "$n" "$CMD_SHA" 2>/dev/null | tr -d '[:space:]') || now=""
      [ "$now" != "${cur[$n]}" ] || die 3 "node $n: swap failed before any change; node $n untouched, later nodes untouched"
      echo "node $n: swap failed midway; restoring the previous binary"
      eice "$n" "$CMD_ROLLBACK" >/dev/null || die 4 "MANUAL: node $n swap failed and the rollback copy failed too; check $live and $live.prev on the node"
      die 3 "node $n: swap failed, previous binary restored; later nodes untouched"
    fi
  fi
  rc=0; eice "$n" "$CMD_RESTART" || rc=$?
  if [ "$rc" = 0 ] && wait_healthy "$n"; then
    echo "node $n: healthy again (applied within $max_lag of the leader)"
    updated=$((updated + 1)); continue
  fi
  if [ "$restart_only" = 1 ]; then
    die 3 "node $n did not become healthy within $timeout s after the restart (nothing to roll back); later nodes untouched; check journalctl -u raftkvd"
  fi
  echo "node $n: NOT healthy within $timeout s; rolling back to the previous binary (this node only)"
  if eice "$n" "$CMD_ROLLBACK" && eice "$n" "$CMD_RESTART" && wait_healthy "$n"; then
    die 3 "node $n failed its health gate on $version and was rolled back to the previous binary; later nodes untouched (nodes already updated stay on $version; rerun with the old dist dir to downgrade them)"
  fi
  die 4 "MANUAL: node $n is unhealthy after the update and after the rollback; nothing else was touched; check journalctl -u raftkvd and $live.prev on the node"
done
echo "done: $updated node(s) updated, $skipped already at target"
