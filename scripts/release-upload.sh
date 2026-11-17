#!/usr/bin/env bash
# Upload a verified dist/<version>/ to S3 (RUNBOOK 10.3 D3). Run by a human, never in CI:
#
#   RELEASE_AWS_PROFILE=<sso profile> RELEASE_BUCKET=<bucket> scripts/release-upload.sh dist/<version>
#
# Every refusal happens BEFORE the first aws call. No long-lived keys: the profile must be
# SSO, so AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN in the environment
# are refused. Objects are immutable per version: an existing key is never overwritten.
# No --acl, no delete, no sync.
#
# Layout:  s3://$RELEASE_BUCKET/artifacts/raftkvd/<version>/  raftkv-server-linux-arm64 (+.sha256), MANIFEST
#          s3://$RELEASE_BUCKET/artifacts/gateway/<version>/  gateway-linux-arm64 (+.sha256), MANIFEST
# MANIFEST goes last under each prefix.
set -euo pipefail
cd "$(dirname "$0")/.."

region=us-east-1
server=raftkv-server-linux-arm64
gateway=gateway-linux-arm64

refuse() { echo "release-upload: REFUSED: $*" >&2; exit 1; }

dir=${1:-}
[ -n "$dir" ] || refuse "usage: release-upload.sh <dist/version dir> (make release-upload RELEASE_DIR=dist/<version>): the directory is required"
[ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ] || refuse "CI or GITHUB_ACTIONS is set; uploads are done by a human, never in CI"
[ -n "${RELEASE_AWS_PROFILE:-}" ] || refuse "RELEASE_AWS_PROFILE is empty; set it to an SSO profile"
[ -n "${RELEASE_BUCKET:-}" ] || refuse "RELEASE_BUCKET is empty"
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
  [ -z "${!v:-}" ] || refuse "$v is set in the environment; no long-lived keys, use the SSO profile (unset $v)"
done
[ -d "$dir" ] || refuse "$dir is not a directory (missing)"
dir=$(cd "$dir" && pwd)
[ -f "$dir/MANIFEST" ] || refuse "missing MANIFEST in $dir"

grep -q '^dirty=false$' "$dir/MANIFEST" || refuse "MANIFEST does not say dirty=false: dirty builds are never uploaded"
scripts/verify-release.sh "$dir" >&2 || refuse "verify-release.sh failed for $dir (sha256 or build checks)"

version=$(sed -n 's/^version=//p' "$dir/MANIFEST" | head -1)
case "$version" in ""|*/*|*[[:space:]]*|*-dirty) refuse "bad version '$version' in MANIFEST" ;; esac

node_prefix="artifacts/raftkvd/$version"
gw_prefix="artifacts/gateway/$version"
# Upload order: file, its .sha256, MANIFEST last.
plan=(
  "$dir/$server|$node_prefix/$server"
  "$dir/$server.sha256|$node_prefix/$server.sha256"
  "$dir/MANIFEST|$node_prefix/MANIFEST"
  "$dir/$gateway|$gw_prefix/$gateway"
  "$dir/$gateway.sha256|$gw_prefix/$gateway.sha256"
  "$dir/MANIFEST|$gw_prefix/MANIFEST"
)

aws_() { aws --profile "$RELEASE_AWS_PROFILE" --region "$region" "$@"; }

# Immutable: check every key first, upload nothing if any exists. Only a clear "Not Found"
# counts as absent; any other failure (denied, expired SSO, network) refuses too.
for item in "${plan[@]}"; do
  key=${item#*|}
  rc=0
  out=$(aws_ s3api head-object --bucket "$RELEASE_BUCKET" --key "$key" 2>&1) || rc=$?
  if [ "$rc" -eq 0 ]; then
    refuse "s3://$RELEASE_BUCKET/$key already exists; releases are immutable, never overwritten (bump the version)"
  fi
  case "$out" in
    *"(404)"*|*"Not Found"*) ;;
    *) refuse "could not tell whether s3://$RELEASE_BUCKET/$key exists: $out" ;;
  esac
done

for item in "${plan[@]}"; do
  src=${item%%|*}; key=${item#*|}
  aws_ s3 cp "$src" "s3://$RELEASE_BUCKET/$key"
  echo "release-upload: s3://$RELEASE_BUCKET/$key"
done
