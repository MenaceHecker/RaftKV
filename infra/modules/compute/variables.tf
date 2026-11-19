variable "name" {
  type        = string
  description = "Name prefix for every resource and Name tag in the compute stack."
  default     = "raftkv"
}

variable "private_subnet_ids" {
  type        = map(string)
  description = "Private subnet ids (Raft nodes) keyed \"0\", \"1\", \"2\": the network module's private_subnet_ids output. Node N+1 lives in subnet N at the fixed address .10 of that subnet."

  validation {
    condition     = join(",", sort(keys(var.private_subnet_ids))) == "0,1,2"
    error_message = "private_subnet_ids must have exactly the keys \"0\", \"1\" and \"2\": three Raft nodes, one per subnet."
  }
}

variable "public_subnet_id" {
  type        = string
  description = "Public subnet id (gateway): the network module's public_subnet_id output."
}

variable "sg_nodes_id" {
  type        = string
  description = "Security group of the Raft nodes (network module output). Defined by the network module, never here."
}

variable "sg_gateway_id" {
  type        = string
  description = "Security group of the gateway (network module output). Defined by the network module, never here."
}

variable "artifact_bucket" {
  type        = string
  description = "Name of the release artifacts bucket (bootstrap output artifacts_bucket). Account-specific, so no default."

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{2,62}$", var.artifact_bucket))
    error_message = "artifact_bucket must be a plain S3 bucket name."
  }
}

variable "release_version" {
  type        = string
  description = "Release version uploaded by scripts/release-upload.sh (the <version> in artifacts/raftkvd/<version>/ and artifacts/gateway/<version>/). No default: a release is always chosen on purpose."

  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._+-]*$", var.release_version))
    error_message = "release_version must be a single path segment of letters, digits and . _ + - (no slash, no spaces)."
  }
}

variable "release_sha256" {
  type = object({
    node    = string
    gateway = string
  })
  description = "Expected sha256 (64 hex characters) of raftkv-server-linux-arm64 and of gateway-linux-arm64, copied by the human from dist/<version>/*.sha256 on the build machine. The instances verify the download against THIS value, never against a hash stored in the bucket. No default."

  validation {
    condition     = can(regex("^[0-9a-f]{64}$", var.release_sha256.node)) && can(regex("^[0-9a-f]{64}$", var.release_sha256.gateway))
    error_message = "release_sha256.node and release_sha256.gateway must each be 64 lower-case hex characters."
  }
}

variable "gateway_domain" {
  type        = string
  description = "Public host name the gateway serves HTTPS for (--tls-domain). Account-specific, so no default here; envs/demo derives it from its domain variable."
}

variable "dashboard_origin" {
  type        = string
  description = "The one origin CORS allows (--dashboard-origin), e.g. https://raftkv.tushar25.com. No default here; envs/demo derives it from its domain variable."
}

variable "tls_staging" {
  type        = bool
  description = "Add --tls-staging to the gateway unit (Let's Encrypt staging CA, separate cache). Use true for the first run, then false. Changing it replaces the gateway instance (its volume and Elastic IP are kept)."
  default     = false
}
