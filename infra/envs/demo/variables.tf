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
  description = "Public domain of the demo (RUNBOOK 10.3 D6: api.<domain> for the gateway, <domain> for the dashboard). The default is a PLACEHOLDER TO BE CONFIRMED by the human. The gateway serves HTTPS for api.<domain> and allows CORS from https://<domain>."
  default     = "raftkv.tushar25.com"
}

variable "ssh_debug" {
  type        = bool
  description = "Create the EC2 Instance Connect Endpoint and the ssh rules. false removes every ssh path."
  default     = true
}

variable "artifact_bucket" {
  type        = string
  description = "Release artifacts bucket: the `artifacts_bucket` output of infra/bootstrap. Account-specific, no default."
}

variable "release_version" {
  type        = string
  description = "Release uploaded with `make release-upload` (artifacts/raftkvd/<version>/ and artifacts/gateway/<version>/). No default."
}

variable "release_sha256" {
  type = object({
    node    = string
    gateway = string
  })
  description = "sha256 of raftkv-server-linux-arm64 and of gateway-linux-arm64, from dist/<version>/*.sha256 on the build machine. The instances verify the download against these, not against anything in the bucket. No default."
}

variable "tls_staging" {
  type        = bool
  description = "First run only: add --tls-staging to the gateway (Let's Encrypt staging CA), then set false. Changing it replaces the gateway instance."
  default     = false
}
