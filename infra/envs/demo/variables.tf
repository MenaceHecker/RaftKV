variable "region" {
  type        = string
  description = "AWS region (RUNBOOK 10.3 D0: us-east-1). Availability zones are looked up in it, never hard-coded."
  default     = "us-east-1"
}

variable "name" {
  type        = string
  description = "Name prefix for resources in this environment."
  default     = "raftkv"
}

variable "domain" {
  type        = string
  description = "Public domain of the demo (RUNBOOK 10.3 D6: api.<domain> for the gateway, <domain> for the dashboard). TO BE CONFIRMED by the human; nothing in D1/D2 uses it yet, it is exported for D6."
  default     = "raftkv.tushar25.com"
}

variable "ssh_debug" {
  type        = bool
  description = "Create the EC2 Instance Connect Endpoint and the ssh rules. false removes every ssh path."
  default     = true
}
