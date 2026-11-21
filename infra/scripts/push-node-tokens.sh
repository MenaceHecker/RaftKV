#!/usr/bin/env bash
# Push the two node-side tokens to the three Raft nodes, then start the node unit. Run by the
# HUMAN on their own machine, once after `terraform apply` and again after any node is replaced
# (a replaced node is empty: no tokens, no WAL).
#
#   AWS_PROFILE=<sso profile> infra/scripts/push-node-tokens.sh <node1 id> <node2 id> <node3 id>
#   (ids: terraform output -json node_instance_ids, in node order 1, 2, 3)
#
# Nodes have no SSM access and no NAT, so the tokens travel the other way: this script reads the two
# SecureStrings with YOUR credentials, and writes them on each node over EC2 Instance Connect
# Endpoint (aws ec2-instance-connect ssh --connection-type eice), through ssh's stdin:
#
#   /raftkv/node-admin-token -> /etc/raftkv/node-admin.token  (node --admin-token-file)
#   /raftkv/cluster-token    -> /etc/raftkv/cluster.token     (node --cluster-token-file)
#
# A token is never on a command line (local or remote), never echoed, never in a temp file: it
# lives in a shell variable and travels on a pipe. On the node the file is created under umask
# 077, chowned to raftkv, mode 0600, and moved into place. Until both files exist the unit is skipped
# (ConditionPathExists), so the node fails closed. The admin token (/raftkv/admin-token) is the
# gateway's and is never read here.
#
# RAFTKV_TOKEN_DIR (default /etc/raftkv) can be overridden for tests only.
set -euo pipefail
umask 077
set +x

refuse() { echo "push-node-tokens: REFUSED: $*" >&2; exit 1; }
die() { echo "push-node-tokens: $*" >&2; exit 1; }

[ "$#" -eq 3 ] || refuse "usage: push-node-tokens.sh <node1 instance id> <node2 instance id> <node3 instance id>"
id_re='^i-[0-9a-f]{8,17}$'
for id in "$@"; do
  [[ $id =~ $id_re ]] || refuse "'$id' is not an instance id (i-...)"
done
[ "$1" != "$2" ] && [ "$1" != "$3" ] && [ "$2" != "$3" ] || refuse "the three instance ids must be different"
[ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ] || refuse "CI is set; tokens are pushed by a human, never in CI"
[ -n "${AWS_PROFILE:-}" ] || refuse "AWS_PROFILE is empty; set it to your SSO profile"
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
  [ -z "${!v:-}" ] || refuse "$v is set; no long-lived keys, use the SSO profile (unset $v)"
done
dir=${RAFTKV_TOKEN_DIR:-/etc/raftkv}
dir_re='^/[A-Za-z0-9._/-]+$'
[[ $dir =~ $dir_re ]] || refuse "RAFTKV_TOKEN_DIR must be an absolute path of plain characters"
command -v aws >/dev/null 2>&1 || refuse "the aws CLI v2 is required"

read_param() { # read_param <name>: prints the value (captured by the caller, never shown)
  aws ssm get-parameter --name "$1" --with-decryption --query Parameter.Value --output text ||
    die "could not read $1 from SSM Parameter Store"
}
node_admin_token=$(read_param /raftkv/node-admin-token) || exit 1
cluster_token=$(read_param /raftkv/cluster-token) || exit 1
for t in "$node_admin_token" "$cluster_token"; do
  [ -n "$t" ] && [ "$t" != "None" ] || refuse "a token parameter is empty"
done
[ "$node_admin_token" != "$cluster_token" ] || refuse "the node admin token and the cluster token must differ"

# One remote command, no token in it. $0 inside is the target path (the argument after the quoted body).
inner='umask 077 && cat > "$0.new" && chown raftkv:raftkv "$0.new" && chmod 0600 "$0.new" && mv "$0.new" "$0"'

eice() { # eice <instance id> <remote command>
  aws ec2-instance-connect ssh --instance-id "$1" --os-user ec2-user --connection-type eice -- "$2"
}

n=0
for id in "$@"; do
  n=$((n + 1))
  echo "node $n ($id): writing node-admin.token and cluster.token"
  printf '%s' "$node_admin_token" | eice "$id" "sudo sh -c '$inner' $dir/node-admin.token" || die "node $n ($id): could not write node-admin.token; stopped, nothing started on it"
  printf '%s' "$cluster_token" | eice "$id" "sudo sh -c '$inner' $dir/cluster.token" || die "node $n ($id): could not write cluster.token; stopped, nothing started on it"
  eice "$id" "sudo systemctl start raftkvd" < /dev/null || die "node $n ($id): tokens written but the unit did not start (journalctl -u raftkvd)"
  echo "node $n ($id): tokens in place, raftkvd started"
done
unset node_admin_token cluster_token
echo "done: 3 nodes"
