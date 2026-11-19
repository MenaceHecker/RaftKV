# STUB (test commit): resources exist with deliberately wrong values so the tests fail on
# their assertions. The real configuration replaces this.

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  buckets = {
    state     = "stub-state"
    artifacts = "stub-artifacts"
  }
}

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id

  versioning_configuration {
    status = "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = { state = "stub-state" }
  bucket   = aws_s3_bucket.this[each.key].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = { state = "stub-state" }
  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = "*", Action = "s3:GetObject", Resource = "arn:aws:s3:::stub/key" }]
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = { state = "stub-state" }
  bucket   = aws_s3_bucket.this[each.key].id

  rule {
    id     = "stub"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
