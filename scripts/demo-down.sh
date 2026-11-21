#!/usr/bin/env bash
# Guided teardown (RUNBOOK 10.9, task D9). PRINT ONLY: it runs read-only AWS checks and prints the six
# manual steps in order. It never calls terraform, never deletes, never empties a bucket. A script an agent
# wrote and tested with stubs must not own an irreversible destroy; you read the plan and run each step.
#
#   AWS_PROFILE=<sso profile> scripts/demo-down.sh --state-bucket <B> --artifacts-bucket <B2> --domain <D>
#
# Read-only calls only: sts get-caller-identity, s3api list-object-versions, ssm describe-parameters
# (names only: never get-parameter, never --with-decryption). Exit 0 always, except refusals (exit 1).
set -euo pipefail
umask 077
set +x

refuse() { echo "demo-down: REFUSED: $*" >&2; exit 1; }
say() { printf '%s\n' "$*"; }

state_b=""; art_b=""; domain=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --state-bucket|--artifacts-bucket|--domain)
      [ "$#" -ge 2 ] || refuse "$1 needs a value"
      case "$1" in --state-bucket) state_b=$2 ;; --artifacts-bucket) art_b=$2 ;; --domain) domain=$2 ;; esac
      shift 2 ;;
    *) refuse "unknown argument '$1' (usage: demo-down.sh --state-bucket B --artifacts-bucket B2 --domain D)" ;;
  esac
done
[ -n "$state_b" ] || refuse "--state-bucket is required"
[ -n "$art_b" ] || refuse "--artifacts-bucket is required"
[ -n "$domain" ] || refuse "--domain is required"
bucket_re='^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'
for b in "$state_b" "$art_b"; do [[ $b =~ $bucket_re ]] || refuse "'$b' is not a valid S3 bucket name"; done
[ "$state_b" != "$art_b" ] || refuse "the state bucket and the artifacts bucket must be different"
domain_re='^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$'
[[ $domain =~ $domain_re ]] || refuse "'$domain' is not a plain domain name"
[ -z "${CI:-}" ] && [ -z "${GITHUB_ACTIONS:-}" ] || refuse "CI is set; teardown is done by a human, never in CI"
[ -n "${AWS_PROFILE:-}" ] || refuse "AWS_PROFILE is empty; set it to your SSO profile"
for v in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; do
  [ -z "${!v:-}" ] || refuse "$v is set; no long-lived keys, use the SSO profile (unset $v)"
done
command -v aws >/dev/null 2>&1 || refuse "the aws CLI v2 is required"

say "demo-down: read-only checks (nothing is deleted, no terraform is run)"
if aws sts get-caller-identity --query Account --output text >/dev/null 2>&1; then
  say "  credentials: the profile works"
else
  say "  credentials: the profile does NOT work (sign in with aws sso login); the status lines below are unknown"
fi

versions() { # versions <bucket>: object versions plus delete markers, counted up to 1000, or "unknown"
  local n
  n=$(aws s3api list-object-versions --bucket "$1" --max-items 1000 \
    --query 'length(Versions || `[]`) + length(DeleteMarkers || `[]`)' --output text 2>/dev/null) || n=unknown
  echo "$n"
}
vstatus() { # vstatus <label> <bucket>
  local n; n=$(versions "$2")
  case "$n" in
    unknown|"") say "  $1 bucket $2: could not be read (missing, or no permission)" ;;
    0|None) say "  $1 bucket $2: empty (no object versions, no delete markers)" ;;
    *) say "  $1 bucket $2: $n object versions remain (delete markers included, counted up to 1000)" ;;
  esac
}
vstatus state "$state_b"
vstatus artifacts "$art_b"

found=$(aws ssm describe-parameters \
  --parameter-filters "Key=Name,Option=Equals,Values=/raftkv/admin-token,/raftkv/node-admin-token,/raftkv/cluster-token" \
  --query 'Parameters[].Name' --output text 2>/dev/null) || found=unknown
if [ "$found" = unknown ]; then
  say "  parameters: could not be listed"
else
  say "  parameters: $(printf '%s\n' "$found" | tr -s ' \t' '\n\n' | grep -c '^/raftkv/') of the 3 SSM parameters still exist (names only; values are never read)"
fi

say ""
say "Teardown, in this order. You run each step; read every plan before you confirm."
say ""
say "STEP 1: destroy the demo environment."
say "  cd infra/envs/demo, read the plan (terraform plan -destroy), then run terraform destroy yourself."
say "  Wrong directory or profile destroys the wrong stack: check 'aws sts get-caller-identity' first."
say ""
say "STEP 2: empty both buckets, including ALL object versions and delete markers (S3 console, Empty)."
say "  The artifacts bucket now. The state bucket only after step 1 (the demo state lives in it)."
say "  The buckets have force_destroy = false on purpose; versioning keeps old objects."
say ""
say "STEP 3: destroy the bootstrap stack."
say "  cd infra/bootstrap, read the plan, then run terraform destroy yourself (its state is a local file, kept until now)."
say ""
say "STEP 4: delete the three SSM parameters by hand."
say "  aws ssm delete-parameter --name /raftkv/admin-token"
say "  aws ssm delete-parameter --name /raftkv/node-admin-token"
say "  aws ssm delete-parameter --name /raftkv/cluster-token"
say ""
say "STEP 5: delete the DNS A record for api.$domain at your DNS provider."
say "  A dangling record pointing at a released Elastic IP is a takeover risk."
say ""
say "STEP 6: leave the dashboard in replay mode and update the README to say the live demo is down (RUNBOOK 10.9)."
say ""
say "Nothing above was executed."
