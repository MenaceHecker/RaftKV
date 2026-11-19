# Offline tests for the bootstrap root module: mock_provider "aws", no credentials, command = plan.
# The caller identity is mocked; 123456789012 is the AWS documentation placeholder, not a real account.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }
}

variables {
  alert_email = "alerts@example.invalid"
}

run "state_bucket_versioned_and_encrypted" {
  command = plan

  assert {
    condition     = aws_s3_bucket.this["state"].bucket == "raftkv-tfstate-123456789012-us-east-1" && aws_s3_bucket.this["artifacts"].bucket == "raftkv-artifacts-123456789012-us-east-1"
    error_message = "bucket names must be raftkv-{tfstate,artifacts}-<account>-<region>"
  }
  assert {
    condition     = alltrue([for b in aws_s3_bucket.this : b.force_destroy == false])
    error_message = "force_destroy must be false on both buckets"
  }
  assert {
    condition     = alltrue([for v in aws_s3_bucket_versioning.this : v.versioning_configuration[0].status == "Enabled"])
    error_message = "versioning must be Enabled on both buckets (state history, immutable artifacts)"
  }
  assert {
    condition     = length(aws_s3_bucket_versioning.this) == 2
    error_message = "expected a versioning resource for each of the two buckets"
  }
  assert {
    condition     = alltrue([for e in aws_s3_bucket_server_side_encryption_configuration.this : one(one(e.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"]) && length(aws_s3_bucket_server_side_encryption_configuration.this) == 2
    error_message = "both buckets need server-side encryption (SSE-S3)"
  }
  assert {
    condition     = alltrue([for p in aws_s3_bucket_policy.this : jsondecode(p.policy).Statement[0].Effect == "Deny" && jsondecode(p.policy).Statement[0].Condition.Bool["aws:SecureTransport"] == "false"])
    error_message = "each bucket policy must deny requests made without TLS"
  }
  assert {
    condition     = alltrue([for k, p in aws_s3_bucket_policy.this : jsondecode(p.policy).Statement[0].Resource == ["arn:aws:s3:::${aws_s3_bucket.this[k].bucket}", "arn:aws:s3:::${aws_s3_bucket.this[k].bucket}/*"]])
    error_message = "the TLS deny must name its own bucket and objects by explicit ARN"
  }
  assert {
    condition     = alltrue([for l in aws_s3_bucket_lifecycle_configuration.this : length(l.rule) >= 1]) && length(aws_s3_bucket_lifecycle_configuration.this) == 2
    error_message = "both buckets need a lifecycle rule (noncurrent versions, incomplete uploads)"
  }
}

run "public_access_fully_blocked_both_buckets" {
  command = plan

  assert {
    condition     = length(aws_s3_bucket_public_access_block.this) == 2
    error_message = "expected a public access block for each of the two buckets"
  }
  assert {
    condition = alltrue([
      for p in aws_s3_bucket_public_access_block.this :
      p.block_public_acls && p.block_public_policy && p.ignore_public_acls && p.restrict_public_buckets
    ])
    error_message = "all four public access block flags must be true on both buckets"
  }
}

run "budget_excludes_credits" {
  command = plan

  assert {
    condition     = aws_budgets_budget.monthly.name == "raftkv-monthly" && aws_budgets_budget.monthly.budget_type == "COST"
    error_message = "budget must be the COST budget named raftkv-monthly"
  }
  assert {
    condition     = aws_budgets_budget.monthly.limit_amount == "40" && aws_budgets_budget.monthly.limit_unit == "USD" && aws_budgets_budget.monthly.time_unit == "MONTHLY"
    error_message = "budget must be USD 40 per month"
  }
  assert {
    condition     = aws_budgets_budget.monthly.cost_types[0].include_credit == false
    error_message = "credits must be EXCLUDED so the budget shows gross spend"
  }
}

run "budget_has_20_30_40_actual_and_100_forecast" {
  command = plan

  assert {
    condition = toset([
      for n in aws_budgets_budget.monthly.notification :
      "${n.notification_type}/${n.threshold_type}/${n.comparison_operator}/${n.threshold}"
      ]) == toset([
      "ACTUAL/ABSOLUTE_VALUE/GREATER_THAN/20",
      "ACTUAL/ABSOLUTE_VALUE/GREATER_THAN/30",
      "ACTUAL/ABSOLUTE_VALUE/GREATER_THAN/40",
      "FORECASTED/PERCENTAGE/GREATER_THAN/100",
    ])
    error_message = "notifications must be ACTUAL 20/30/40 USD (absolute) and FORECASTED 100 percent"
  }
  assert {
    condition     = length(aws_budgets_budget.monthly.notification) == 4
    error_message = "expected exactly four notifications"
  }
  assert {
    condition     = alltrue([for n in aws_budgets_budget.monthly.notification : n.subscriber_email_addresses == toset(["alerts@example.invalid"])])
    error_message = "every notification must go to var.alert_email"
  }
}

run "anomaly_monitor_exists" {
  command = plan

  assert {
    condition     = aws_ce_anomaly_monitor.service.monitor_type == "DIMENSIONAL" && aws_ce_anomaly_monitor.service.monitor_dimension == "SERVICE"
    error_message = "expected a DIMENSIONAL monitor on SERVICE"
  }
  assert {
    condition     = aws_ce_anomaly_subscription.email.frequency == "DAILY"
    error_message = "email subscriptions are DAILY (IMMEDIATE needs SNS)"
  }
  assert {
    condition     = one(aws_ce_anomaly_subscription.email.subscriber).type == "EMAIL" && one(aws_ce_anomaly_subscription.email.subscriber).address == "alerts@example.invalid"
    error_message = "the anomaly subscription must email var.alert_email"
  }
  assert {
    condition     = one(aws_ce_anomaly_subscription.email.threshold_expression).dimension[0].key == "ANOMALY_TOTAL_IMPACT_ABSOLUTE" && one(aws_ce_anomaly_subscription.email.threshold_expression).dimension[0].values == toset(["5"])
    error_message = "the default anomaly threshold is USD 5 of absolute total impact"
  }
}

run "no_iam_user_or_access_key_resource" {
  command = plan

  # The resource types themselves are barred by `check.sh --hcl` (grep over the source). What this
  # run can show at plan time: the only policies in bootstrap are bucket policies, and every
  # statement in them is a Deny, so nothing grants any principal (user, key or role) anything.
  assert {
    condition     = alltrue([for p in aws_s3_bucket_policy.this : alltrue([for s in jsondecode(p.policy).Statement : s.Effect == "Deny"])])
    error_message = "bucket policies may only contain Deny statements in this card; instance access is D4's job"
  }
}

run "outputs_name_buckets_and_object_prefixes" {
  command = plan

  assert {
    condition     = output.state_bucket == "raftkv-tfstate-123456789012-us-east-1" && output.artifacts_bucket == "raftkv-artifacts-123456789012-us-east-1"
    error_message = "state_bucket / artifacts_bucket outputs wrong"
  }
  assert {
    condition     = output.artifacts_bucket_arn == "arn:aws:s3:::raftkv-artifacts-123456789012-us-east-1"
    error_message = "artifacts_bucket_arn output wrong"
  }
  assert {
    condition     = output.artifacts_node_object_arn_pattern == "arn:aws:s3:::raftkv-artifacts-123456789012-us-east-1/artifacts/raftkvd/*" && output.artifacts_gateway_object_arn_pattern == "arn:aws:s3:::raftkv-artifacts-123456789012-us-east-1/artifacts/gateway/*"
    error_message = "object ARN patterns must match the layout scripts/release-upload.sh writes: artifacts/raftkvd/<v>/ and artifacts/gateway/<v>/"
  }
}

run "alert_email_must_be_an_email" {
  command = plan

  variables {
    alert_email = "not-an-email"
  }

  expect_failures = [var.alert_email]
}

run "bucket_names_derive_from_the_prefix" {
  command = plan

  variables {
    name_prefix = "demo"
  }

  assert {
    condition     = aws_s3_bucket.this["state"].bucket == "demo-tfstate-123456789012-us-east-1"
    error_message = "name_prefix must flow into the bucket names"
  }
}
