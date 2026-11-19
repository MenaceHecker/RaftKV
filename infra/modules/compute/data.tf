data "aws_region" "current" {}

# Only used to put the account id into the exact SSM parameter ARNs below. It ends up in the
# (private) state and in the IAM policy documents, never in git.
data "aws_caller_identity" "current" {}

# The public AL2023 arm64 AMI parameter is the ONLY SSM parameter this module reads. No /raftkv/*
# parameter is ever a data source or a resource here: that would put the secret value in the state
# (infra/policy/check.sh fails the build if one appears).
data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

# Zone and CIDR of each subnet come from the subnets themselves, so a node's fixed address
# (.10 of its own subnet) and its zone can never disagree with the network module.
data "aws_subnet" "private" {
  for_each = var.private_subnet_ids

  id = each.value
}

data "aws_subnet" "public" {
  id = var.public_subnet_id
}

locals {
  region     = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id

  # Ports from the code, not from RUNBOOK 10.5: one shared gRPC listener (7000), the admin API
  # (9000), metrics (2112). The network module's security groups open exactly these.
  raft_port    = 7000
  admin_port   = 9000
  metrics_port = 2112

  # Node id N+1 lives in private subnet N at the fixed address .10 of that subnet.
  nodes = {
    for k, s in data.aws_subnet.private : k => {
      id        = tonumber(k) + 1
      subnet_id = var.private_subnet_ids[k]
      az        = s.availability_zone
      ip        = cidrhost(s.cidr_block, 10)
    }
  }
  node_keys = sort(keys(local.nodes))

  # --peers as cmd/raftkv-server/main.go parsePeers reads it: id=host:port,id=host:port,...
  peers = join(",", [for k in local.node_keys : "${local.nodes[k].id}=${local.nodes[k].ip}:${local.raft_port}"])

  # --nodes as cmd/gateway/nodes.go parseNodes reads it: id=az=addr=adminaddr,...
  gateway_nodes = join(",", [for k in local.node_keys : "${local.nodes[k].id}=${local.nodes[k].az}=${local.nodes[k].ip}:${local.raft_port}=${local.nodes[k].ip}:${local.admin_port}"])

  # Object ARNs follow the key layout of scripts/release-upload.sh.
  node_artifacts_arn    = "arn:aws:s3:::${var.artifact_bucket}/artifacts/raftkvd/*"
  gateway_artifacts_arn = "arn:aws:s3:::${var.artifact_bucket}/artifacts/gateway/*"
  ssm_param_arn_prefix  = "arn:aws:ssm:${local.region}:${local.account_id}:parameter"

  node_binary_key    = "artifacts/raftkvd/${var.release_version}/raftkv-server-linux-arm64"
  gateway_binary_key = "artifacts/gateway/${var.release_version}/gateway-linux-arm64"
}
