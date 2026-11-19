# STUB (test commit): wrong on purpose.

resource "aws_budgets_budget" "monthly" {
  name         = "stub"
  budget_type  = "USAGE"
  limit_amount = "1"
  limit_unit   = "USD"
  time_unit    = "DAILY"

  cost_types {
    include_credit = true
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 1
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["nobody@example.invalid"]
  }
}

resource "aws_ce_anomaly_monitor" "service" {
  name                  = "stub"
  monitor_type          = "CUSTOM"
  monitor_specification = jsonencode({ Tags = { Key = "x", Values = ["y"] } })
}

resource "aws_ce_anomaly_subscription" "email" {
  name             = "stub"
  frequency        = "WEEKLY"
  monitor_arn_list = [aws_ce_anomaly_monitor.service.arn]

  subscriber {
    type    = "EMAIL"
    address = "nobody@example.invalid"
  }

  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_PERCENTAGE"
      match_options = ["GREATER_THAN_OR_EQUAL"]
      values        = ["1"]
    }
  }
}
