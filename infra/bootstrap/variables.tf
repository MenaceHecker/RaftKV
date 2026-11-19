variable "region" {
  type        = string
  description = "AWS region for the state and artifacts buckets and the provider (RUNBOOK 10.3 D0: us-east-1). Budgets and Cost Explorer are global but are reached through this region."
  default     = "us-east-1"
}

variable "alert_email" {
  type        = string
  description = "Email that receives the budget and cost-anomaly alerts. REQUIRED, no default: supply it at apply time (-var or TF_VAR_alert_email), never commit it."

  validation {
    condition     = can(regex("^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$", var.alert_email))
    error_message = "alert_email must look like an email address."
  }
}

variable "name_prefix" {
  type        = string
  description = "Prefix of the bucket names. Buckets are named <prefix>-tfstate-<account id>-<region> and <prefix>-artifacts-<account id>-<region>; the account id comes from the caller identity, so it is never written in git."
  default     = "raftkv"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,20}$", var.name_prefix))
    error_message = "name_prefix must be 2-21 characters: lowercase letters, digits and hyphens, starting with a letter or digit."
  }
}

variable "budget_limit_usd" {
  type        = number
  description = "Monthly cost budget in USD. Credits are EXCLUDED so the budget shows gross spend (RUNBOOK 10.9 ceiling: 40)."
  default     = 40

  validation {
    condition     = var.budget_limit_usd > 0
    error_message = "budget_limit_usd must be positive."
  }
}

variable "anomaly_threshold_usd" {
  type        = number
  description = "Cost Anomaly Detection alerts when the absolute total impact of an anomaly is at least this many USD."
  default     = 5

  validation {
    condition     = var.anomaly_threshold_usd > 0
    error_message = "anomaly_threshold_usd must be positive."
  }
}
