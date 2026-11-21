# Instance roles. A HUMAN MUST READ THIS FILE BEFORE APPLY (RUNBOOK 10.4); the same table is in
# infra/README.md and in the pull request.
#
#   role      allowed                                                    resource
#   node      s3:GetObject                                               artifacts/raftkvd/* of the artifacts bucket
#   gateway   s3:GetObject                                               artifacts/gateway/* of the artifacts bucket
#   gateway   ssm:GetParameter                                           /raftkv/admin-token, /raftkv/node-admin-token (exact ARNs)
#
# Nodes get NO SSM access: the human pushes their two tokens over EC2 Instance Connect Endpoint
# (infra/scripts/push-node-tokens.sh). No instance can read /raftkv/cluster-token. No kms:*: the
# SecureStrings are expected to use the default aws/ssm key, whose key policy lets principals that
# may call ssm:GetParameter decrypt through the SSM service. UNVERIFIED until the first boot (D8): if
# the gateway cannot decrypt, the fix is kms:Decrypt on that one key ARN, not a wildcard.
#
# Policies are jsonencode of literal values (never aws_iam_policy_document) so the tests and
# infra/policy/check.sh can read them from a plan. No "*" in an Action, none in a Resource except
# the trailing key wildcard of an S3 object ARN. No managed policy is attached.

resource "aws_iam_role" "node" {
  name = "${var.name}-node"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "node" {
  name = "${var.name}-node-artifacts"
  role = aws_iam_role.node.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = [local.node_artifacts_arn]
    }]
  })
}

resource "aws_iam_instance_profile" "node" {
  name = "${var.name}-node"
  role = aws_iam_role.node.name
}

resource "aws_iam_role" "gateway" {
  name = "${var.name}-gateway"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "gateway" {
  name = "${var.name}-gateway"
  role = aws_iam_role.gateway.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [local.gateway_artifacts_arn]
      },
      {
        Effect = "Allow"
        Action = ["ssm:GetParameter"]
        Resource = [
          "${local.ssm_param_arn_prefix}/raftkv/admin-token",
          "${local.ssm_param_arn_prefix}/raftkv/node-admin-token",
        ]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "gateway" {
  name = "${var.name}-gateway"
  role = aws_iam_role.gateway.name
}
