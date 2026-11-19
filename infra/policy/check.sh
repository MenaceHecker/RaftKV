#!/usr/bin/env bash
# Offline policy checks for the RaftKV infra (RUNBOOK 2.9, 10.4). Needs only bash and jq; no
# AWS credentials and no network.
#
#   check.sh <plan.json>...     check `terraform show -json <planfile>` output
#   check.sh --hcl <dir>        grep the .tf sources under <dir> for forbidden resource types
#   selftest.sh                 run check.sh over testdata/ (bad-*.json must fail, good.json pass)
#
# Exit 0: clean. Exit 1: at least one violation (each printed as `VIOLATION: ...`). Exit 2:
# usage error or unreadable input (never reads as clean).
#
# Plan-JSON rules, evaluated on resource_changes[].change.after (resources being deleted are
# ignored):
#   a. IAM and bucket policies (aws_iam_policy, aws_iam_role_policy, aws_iam_user_policy,
#      aws_iam_group_policy, aws_s3_bucket_policy, aws_iam_role assume/inline policies): no "*" in
#      Action, no NotAction/NotResource, no "*" in Resource. Principal is exempt. Two narrow
#      exemptions, both needed by S3 itself:
#        - Resource may end in a key wildcard on a named bucket: arn:aws:s3:::<bucket>/<prefix>/*
#          (s3:GetObject cannot be granted any other way; a wildcard in the bucket part or in the
#          middle of the key is still a violation);
#        - Action "s3:*" is allowed only on a Deny statement whose Condition mentions
#          aws:SecureTransport (the TLS-only bucket policy).
#      A policy whose text is unknown at plan time, or is not valid JSON, is a violation: it
#      cannot be checked, so it must be written with jsonencode from known values.
#   b. Ingress open to 0.0.0.0/0 or ::/0 is allowed only for tcp 80 or tcp 443 (one port per
#      rule) on the gateway security group: aws_vpc_security_group_ingress_rule.gateway_world /
#      aws_security_group_rule.gateway_world, or an ingress block in aws_security_group.gateway.
#      Rule resources are recognised by their Terraform address because the group they attach to
#      is an unknown id at plan time.
#   c. No aws_nat_gateway. No route to an internet gateway or NAT, no 0.0.0.0/0 or ::/0 route,
#      and no unknown route target, except on resources named public*; route tables not named
#      public* must hold no routes at all.
#   d. No aws_iam_user*, aws_iam_access_key or aws_iam_service_specific_credential.
set -uo pipefail

die() { echo "check.sh: $*" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || die "jq is required"

if [ "${1:-}" = "--hcl" ]; then
  dir=${2:-}
  [ -n "$dir" ] && [ -d "$dir" ] || die "usage: check.sh --hcl <dir>"
  rc=0
  # Forbidden resource types, anywhere in the sources.
  if hits=$(grep -rEn --include='*.tf' 'resource[[:space:]]+"(aws_nat_gateway|aws_iam_user[a-z_]*|aws_iam_access_key|aws_iam_service_specific_credential)"' "$dir" --exclude-dir=.terraform); then
    echo "$hits" | sed 's/^/VIOLATION: forbidden resource type: /'; rc=1
  fi
  # A wildcard written as a literal Action or Resource value in HCL (jsonencode maps and
  # aws_iam_policy_document blocks alike). Key wildcards inside an ARN string are fine.
  if hits=$(grep -rEn --include='*.tf' --exclude-dir=.terraform '(^|[^A-Za-z_])(Action|Resource|actions|resources|not_actions|not_resources)[[:space:]]*=[[:space:]]*\[?[[:space:]]*"[^"]*\*"' "$dir" | grep -vE '"(arn:aws:s3:::[^"*]+/[^"*]*\*|s3:\*)"|Deny'); then
    echo "$hits" | sed 's/^/VIOLATION: wildcard Action or Resource in HCL: /'; rc=1
  fi
  exit "$rc"
fi

[ "$#" -ge 1 ] || die "usage: check.sh <plan.json>... | --hcl <dir>"

read -r -d '' PROGRAM <<'JQ'
def arr: if . == null then [] elif type == "array" then . else [.] end;
def addr_is($re): (.address // "") | test($re);
def s3_object_pattern: test("^arn:aws:s3:::[a-z0-9][a-z0-9.-]*/[^*]*\\*$");
def tls_deny: .Effect == "Deny" and ((.Condition // {}) | tostring | contains("aws:SecureTransport"));

def stmt_violations($addr):
  tls_deny as $tls
  | [
      (if (has("NotAction") or has("NotResource")) then "\($addr): NotAction/NotResource is not allowed" else empty end),
      (.Action | arr | .[] | select(type == "string" and contains("*")) | select(($tls and . == "s3:*") | not) | "\($addr): wildcard Action \(.)"),
      (.Resource | arr | .[] | select(type == "string" and contains("*")) | select(s3_object_pattern | not) | "\($addr): wildcard Resource \(.)")
    ] | .[];

# One policy field of one resource: name is the attribute name, v its value in `after`.
def policy_violations($rc; $name; $v):
  if $v == null then
    (if (($rc.change.after_unknown // {})[$name]) == true then "\($rc.address): \($name) is unknown at plan time, cannot be checked (build it with jsonencode from known values)" else empty end)
  else
    (try ($v | fromjson) catch null) as $doc
    | if $doc == null or ($doc | type) != "object" then "\($rc.address): \($name) is not valid JSON, cannot be checked"
      else ($doc.Statement | arr | .[] | stmt_violations($rc.address))
      end
  end;

def is_world: ((.cidr_ipv4 // "") == "0.0.0.0/0") or ((.cidr_ipv6 // "") == "::/0")
  or (((.cidr_blocks // []) | index("0.0.0.0/0")) != null) or (((.ipv6_cidr_blocks // []) | index("::/0")) != null);
def only_80_443($proto; $from; $to): $proto == "tcp" and $from != null and $from == $to and ($from == 80 or $from == 443);

[.resource_changes // [] | .[] | select((.change.actions // []) != ["delete"]) | . as $rc | (.change.after // {}) as $a |
  # (a) policies
  (if (.type | IN("aws_iam_policy", "aws_iam_role_policy", "aws_iam_user_policy", "aws_iam_group_policy", "aws_s3_bucket_policy", "aws_iam_role"))
   then (policy_violations($rc; "policy"; $a.policy), (if .type == "aws_iam_role" then policy_violations($rc; "assume_role_policy"; $a.assume_role_policy) else empty end),
         ($a.inline_policy // [] | .[] | policy_violations($rc; "inline_policy"; .policy)))
   else empty end),
  # (b) world-open ingress
  (if (.type | IN("aws_vpc_security_group_ingress_rule", "aws_security_group_rule")) and (($a.type // "ingress") == "ingress") and ($a | is_world)
   then (if ($rc | addr_is("(^|\\.)aws_(vpc_)?security_group(_ingress)?_rule\\.gateway_world(\\[|$)")) and only_80_443(($a.ip_protocol // $a.protocol); $a.from_port; $a.to_port) then empty
        else "\($rc.address): ingress open to the world; only tcp 80 or 443 on gateway_world is allowed" end)
   else empty end),
  (if .type == "aws_security_group"
   then ($a.ingress // [] | .[] | select(is_world) | select((($rc | addr_is("(^|\\.)aws_security_group\\.gateway$")) and only_80_443(.protocol; .from_port; .to_port)) | not)
         | "\($rc.address): inline ingress open to the world; only tcp 80 or 443 on the gateway security group is allowed")
   else empty end),
  # (c) NAT and internet routes
  (if .type == "aws_nat_gateway" then "\($rc.address): NAT gateways are not allowed (no NAT, RUNBOOK 10.1)" else empty end),
  (if .type == "aws_route" and ($rc | addr_is("(^|\\.)aws_route\\.public") | not)
   then (if ((($a.gateway_id // "") | startswith("igw-")) or $a.nat_gateway_id != null or $a.transit_gateway_id != null
             or (($rc.change.after_unknown // {}).gateway_id == true) or (($rc.change.after_unknown // {}).nat_gateway_id == true)
             or ($a.destination_cidr_block // "") == "0.0.0.0/0" or ($a.destination_ipv6_cidr_block // "") == "::/0")
         then "\($rc.address): route to an internet gateway, NAT or the default destination outside a public route table" else empty end)
   else empty end),
  (if .type == "aws_route_table" and ($rc | addr_is("(^|\\.)aws_route_table\\.public") | not) and (($a.route // []) | length) > 0
   then "\($rc.address): route tables other than public* must hold no routes (private tables have no default route)"
   else empty end),
  # (d) IAM users and keys
  (if (.type | startswith("aws_iam_user")) or .type == "aws_iam_access_key" or .type == "aws_iam_service_specific_credential"
   then "\($rc.address): IAM users and access keys are never created (no long-lived credentials, RUNBOOK 2.9)" else empty end)
] | .[]
JQ

status=0
for f in "$@"; do
  [ -r "$f" ] || die "cannot read $f"
  jq -e . "$f" >/dev/null 2>&1 || die "$f is not valid JSON"
  out=$(jq -r "$PROGRAM" "$f" 2>&1) || { echo "check.sh: jq failed on $f: $out" >&2; exit 2; }
  if [ -n "$out" ]; then
    echo "$out" | sed "s|^|VIOLATION: $f: |"
    status=1
  fi
done
exit "$status"
