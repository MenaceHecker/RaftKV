#!/usr/bin/env bash
# Stop or start the demo's instances to save money (RUNBOOK 10.9, task D9). Run by the HUMAN.
#
#   AWS_PROFILE=<sso profile> scripts/demo-pause.sh pause|resume [--dry-run] <gateway id> <node1> <node2> <node3>
#
# pause:  stop the gateway first (it stops serving and holds no chaos), then the three nodes, and wait
#         until they are stopped.
# resume: start the nodes, wait until their status checks pass, then start the gateway (its safe start
#         heals the nodes and holds the chaos slot until all 3 answer Status).
# It never terminates or modifies an instance. Stopped instances keep their ids, EBS volumes and private
# IPs, so the node token files (root volume) survive: NO push-node-tokens.sh on resume. The gateway
# refetches its tokens on every start. The data volume is remounted by UUID on boot (nofail): confirm on
# the first resume that raftkvd is active (unverified).
set -euo pipefail
umask 077
set +x

refuse() { echo "demo-pause: REFUSED: $*" >&2; exit 1; }

action=${1:-}
case "$action" in pause|resume) shift ;; *) refuse "usage: demo-pause.sh pause|resume [--dry-run] <gateway id> <node1> <node2> <node3>" ;; esac
dry=0
if [ "${1:-}" = "--dry-run" ]; then dry=1; shift; fi
[ "$#" -eq 4 ] || refuse "usage: demo-pause.sh $action [--dry-run] <gateway id> <node1> <node2> <node3>"
id_re='^i-[0-9a-f]{8,17}$'
for id in "$@"; do [[ $id =~ $id_re ]] || refuse "'$id' is not an instance id (i-...)"; done
for a in 1 2 3 4; do for b in 1 2 3 4; do
  [ "$a" -lt "$b" ] && [ "${!a}" = "${!b}" ] && refuse "the four instance ids must be different"
done; done
gw=$1; n1=$2; n2=$3; n3=$4
[ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ] || refuse "CI is set; pausing is done by a human, never in CI"
[ -n "${AWS_PROFILE:-}" ] || refuse "AWS_PROFILE is empty; set it to your SSO profile"
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
  [ -z "${!v:-}" ] || refuse "$v is set; no long-lived keys, use the SSO profile (unset $v)"
done
command -v aws >/dev/null 2>&1 || refuse "the aws CLI v2 is required"

run() { # run <aws args...>: print it, and issue it unless --dry-run
  echo "+ aws $*"
  [ "$dry" = 1 ] || aws "$@" >/dev/null
}

if [ "$dry" = 1 ]; then echo "DRY RUN: nothing is stopped or started; these are the calls a real run would make."; fi
if [ "$action" = pause ]; then
  run ec2 stop-instances --instance-ids "$gw"
  run ec2 wait instance-stopped --instance-ids "$gw"
  run ec2 stop-instances --instance-ids "$n1" "$n2" "$n3"
  run ec2 wait instance-stopped --instance-ids "$n1" "$n2" "$n3"
  echo "paused: gateway and 3 nodes are stopped (no compute charge)."
  echo "Still billing while paused: the Elastic IP (about \$3.65 a month, RUNBOOK 10.2), the EBS volumes"
  echo "(4 x 8 + 3 x 4 + 2 = 46 GiB gp3, about \$3.7 a month at \$0.08 per GiB, price unverified) and S3 pennies."
  echo "Resume with: scripts/demo-pause.sh resume <gateway id> <node1> <node2> <node3>"
else
  run ec2 start-instances --instance-ids "$n1" "$n2" "$n3"
  run ec2 wait instance-status-ok --instance-ids "$n1" "$n2" "$n3"
  run ec2 start-instances --instance-ids "$gw"
  run ec2 wait instance-status-ok --instance-ids "$gw"
  echo "resumed: nodes first, then the gateway. The gateway holds the chaos slot until all 3 nodes answer Status."
  echo "No token push is needed (the files are on the nodes' root volumes). On the first resume, check over"
  echo "EC2 Instance Connect Endpoint that raftkvd is active: the data volume remount is unverified."
fi
