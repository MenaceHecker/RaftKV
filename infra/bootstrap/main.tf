# Bootstrap (RUNBOOK 10.3 D1). Applied once, by a human, with LOCAL state: the state bucket
# cannot hold the state that creates it. Keep bootstrap's terraform.tfstate (gitignored) safe;
# losing it only means the two buckets and the budget must be imported or recreated by hand.
#
# Nothing here creates an IAM user, an access key or a role. The artifacts bucket carries no
# grants at all; D4/D5 add the instance-role policies (s3:GetObject on the two prefixes that
# outputs.tf names).

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "raftkv"
      Env       = "bootstrap"
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # S3 bucket names are global. The account id makes them unique without being written in git.
  buckets = {
    state     = "${var.name_prefix}-tfstate-${data.aws_caller_identity.current.account_id}-${var.region}"
    artifacts = "${var.name_prefix}-artifacts-${data.aws_caller_identity.current.account_id}-${var.region}"
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket = each.value
  # Never let a destroy silently delete state or release history. Teardown empties the
  # buckets by hand (see infra/README.md).
  force_destroy = false
}

# Bucket owner enforced: ACLs are disabled, so no object can be made public through one.
resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# State history for the state bucket; immutable, recoverable releases for the artifacts bucket.
resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    bucket_key_enabled = true

    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = local.buckets

  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# TLS only. The statement is a Deny that names its own bucket and objects by explicit ARN; the
# ARNs are built from the bucket name (not the .arn attribute) so the policy text is known at
# plan time and infra/policy/check.sh can read it. Action "s3:*" is the one wildcard, allowed
# only on a Deny conditioned on aws:SecureTransport (the standard TLS-only pattern).
resource "aws_s3_bucket_policy" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureTransport"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          "arn:aws:s3:::${each.value}",
          "arn:aws:s3:::${each.value}/*",
        ]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      }
    ]
  })

  # The public access block must exist first, or the policy call can race it.
  depends_on = [aws_s3_bucket_public_access_block.this]
}

# Native state locking (use_lockfile) writes <key>.tflock next to the state object and deletes
# it afterwards; it needs only PutObject/DeleteObject on the bucket, no DynamoDB. Noncurrent
# versions (state history, lock-file churn) expire after 90 days; failed uploads after 7.
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = local.buckets

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}
