variable "name" {
  type        = string
  description = "Name prefix for every resource and Name tag in the network."
  default     = "raftkv"
}

variable "vpc_cidr" {
  type        = string
  description = "CIDR block of the VPC (RUNBOOK 10.3 D2: 10.20.0.0/16)."
  default     = "10.20.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "vpc_cidr must be a valid IPv4 CIDR block."
  }
}

variable "az_count" {
  type        = number
  description = "Number of private subnets, each in its own distinct availability zone. The cluster is three nodes, one per AZ, so this must be 3."
  default     = 3

  validation {
    condition     = var.az_count == 3
    error_message = "az_count must be 3: three Raft nodes, one per availability zone."
  }
}

variable "public_cidr" {
  type        = string
  description = "CIDR block of the single public subnet (gateway), in the first availability zone."
  default     = "10.20.0.0/24"

  validation {
    condition     = can(cidrhost(var.public_cidr, 0))
    error_message = "public_cidr must be a valid IPv4 CIDR block."
  }
}

variable "private_cidrs" {
  type        = list(string)
  description = "CIDR blocks of the private subnets (Raft nodes), one per availability zone, in order."
  default     = ["10.20.1.0/24", "10.20.2.0/24", "10.20.3.0/24"]

  validation {
    condition     = length(var.private_cidrs) == 3 && alltrue([for c in var.private_cidrs : can(cidrhost(c, 0))])
    error_message = "private_cidrs must hold exactly three valid IPv4 CIDR blocks."
  }
}

variable "ssh_debug" {
  type        = bool
  description = "Create the EC2 Instance Connect Endpoint and the SSH rules (sg_eice to sg_nodes tcp/22). false removes every SSH path."
  default     = true
}
