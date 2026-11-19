# Cost guard rails (RUNBOOK 10.2, 10.9). Credits are EXCLUDED so the budget shows gross burn,
# not the figure after credits. Notification emails need confirming by the human (AWS mails a
# confirmation link to var.alert_email). A forecast alert needs some spend history before AWS
# can produce a forecast, so it may stay quiet for the first days.

resource "aws_budgets_budget" "monthly" {
  name         = "raftkv-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_types {
    include_credit = false
  }

  # ACTUAL spend crossing USD 20, 30 and 40.
  dynamic "notification" {
    for_each = [20, 30, 40]

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "ABSOLUTE_VALUE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }

  # FORECASTED spend reaching 100 percent of the limit. A forecast is a percentage of the
  # budget, never an absolute value.
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

resource "aws_ce_anomaly_monitor" "service" {
  name              = "raftkv-service-anomalies"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"
}

# Email subscribers get DAILY or WEEKLY digests (IMMEDIATE is for SNS topics only).
resource "aws_ce_anomaly_subscription" "email" {
  name             = "raftkv-anomaly-email"
  frequency        = "DAILY"
  monitor_arn_list = [aws_ce_anomaly_monitor.service.arn]

  subscriber {
    type    = "EMAIL"
    address = var.alert_email
  }

  threshold_expression {
    dimension {
      key           = "ANOMALY_TOTAL_IMPACT_ABSOLUTE"
      match_options = ["GREATER_THAN_OR_EQUAL"]
      values        = [tostring(var.anomaly_threshold_usd)]
    }
  }
}
