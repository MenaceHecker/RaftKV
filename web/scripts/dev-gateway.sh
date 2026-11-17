#!/usr/bin/env bash
# Run the real gateway against its in-memory DEMO cluster (no AWS) for local dashboard work.
#   scripts/dev-gateway.sh [origin]        origin defaults to http://localhost:5173 (vite dev)
# Use http://localhost:4173 for `npm run preview`. Token files are temporary, mode 0600, and the
# tokens are never printed. The admin token is in $STATE/admin.token if you need it (read the
# file yourself); it is deleted on exit.
set -euo pipefail
origin="${1:-http://localhost:5173}"
listen="${GATEWAY_LISTEN:-127.0.0.1:8080}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
umask 077
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$tmp/admin.token"
head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$tmp/node.token"
chmod 0600 "$tmp/admin.token" "$tmp/node.token"
mkdir -p "$tmp/state"
(cd "$root" && go build -tags fakecluster -o "$tmp/gateway" ./cmd/gateway)
echo "gateway on http://$listen, dashboard origin $origin, token dir $tmp" >&2
"$tmp/gateway" --fake-cluster --listen "$listen" \
  --admin-token-file "$tmp/admin.token" --node-token-file "$tmp/node.token" \
  --state-dir "$tmp/state" --dashboard-origin "$origin"
