#!/usr/bin/env bash
# Gateway pre-start: read the two SecureStrings the gateway needs and write them to the tmpfs
# RuntimeDirectory as token files. Run by systemd as ExecStartPre=+ (as root), before every start.
#
#   /raftkv/admin-token       -> admin.token  (gateway --admin-token-file, the HTTP admin token)
#   /raftkv/node-admin-token  -> node.token   (gateway --node-token-file, the machine token for the nodes)
#
# The cluster token is NOT read here: no instance may read it (IAM allows exactly these two ARNs).
#
# A token is never on a command line (the aws call carries only the parameter NAME; its output goes
# straight into a file), never echoed, never on disk (the RuntimeDirectory is tmpfs), and umask 077
# makes every file owner-only from the first byte. It fails closed: if either parameter cannot be
# read or is empty, or the two are equal, no token file is left behind and the unit does not start.
#
# RUNTIME_DIR and GATEWAY_USER can be overridden for tests only.
set -euo pipefail
umask 077
export HOME="${HOME:-/root}"

dir=${RUNTIME_DIR:-/run/raftkv-gateway}
user=${GATEWAY_USER:-raftkv-gw}

die() { echo "fetch-gateway-tokens: $*" >&2; exit 1; }

case "$dir" in /*) ;; *) die "RUNTIME_DIR must be an absolute path" ;; esac
[ -d "$dir" ] || die "$dir does not exist: it must be the unit's RuntimeDirectory (tmpfs), it is never created here"
mode=$(stat -c %a "$dir" 2>/dev/null || stat -f %Lp "$dir")
[ $((8#$mode & 077)) -eq 0 ] || die "$dir is accessible to group or others (mode $mode)"

tmp_admin="$dir/.admin.token.new"
tmp_node="$dir/.node.token.new"
cleanup() { rm -f "$tmp_admin" "$tmp_node"; }
trap cleanup EXIT

fetch() { # fetch <parameter name> <file>: the value goes straight into the file
  aws ssm get-parameter --name "$1" --with-decryption --query Parameter.Value --output text > "$2" ||
    die "could not read $1 from SSM Parameter Store (does it exist, and may this role read it?)"
  [ -n "$(tr -d '[:space:]' < "$2")" ] || die "$1 is empty"
  [ "$(tr -d '[:space:]' < "$2")" != "None" ] || die "$1 has no value"
}

fetch /raftkv/admin-token "$tmp_admin"
fetch /raftkv/node-admin-token "$tmp_node"
# The gateway refuses equal tokens; fail here with a clearer message.
if cmp -s "$tmp_admin" "$tmp_node"; then die "the admin token and the node admin token must differ"; fi

for f in "$tmp_admin" "$tmp_node"; do
  chown "$user:$user" "$f"
  chmod 0600 "$f"
done
mv -f "$tmp_admin" "$dir/admin.token"
mv -f "$tmp_node" "$dir/node.token"
