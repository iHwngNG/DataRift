# =============================================================================
# Single Job Variables
# =============================================================================

variable "name" {
  description = "Name of the Cloud Run Job"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]{1,63}$", var.name))
    error_message = "Job name must be lowercase, 1-63 characters, and contain only hyphens."
  }
}

variable "location" {
  description = "GCP region for the job"
  type        = string
}

variable "image" {
  description = "Container image URL to deploy"
  type        = string
}

variable "service_account" {
  description = "Service account email to run the job with"
  type        = string
}

variable "task_count" {
  description = "Number of parallel task executions"
  type        = number
  default     = 1

  validation {
    condition     = var.task_count >= 1 && var.task_count <= 100
    error_message = "Task count must be between 1 and 100."
  }
}

variable "cpu" {
  description = "CPU limit (e.g., '1', '2')"
  type        = string
  default     = "1"
}

variable "memory" {
  description = "Memory limit (e.g., '512Mi', '1Gi')"
  type        = string
  default     = "512Mi"

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.memory))
    error_message = "Memory must be in Mi or Gi format (e.g., '512Mi', '1Gi')."
  }
}

variable "timeout" {
  description = "Maximum execution timeout in seconds (max 3600 for Cloud Run Jobs)"
  type        = number
  default     = 300
}

variable "env" {
  description = "List of environment variables as objects with name and value"
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "secret_env" {
  description = "List of environment variables from Secret Manager"
  type = list(object({
    name        = string
    secret_name = string
    version     = string
  }))
  default = []
}

variable "volume_mounts" {
  description = "List of volume mounts as objects with name and mount_path"
  type = list(object({
    name       = string
    mount_path = string
  }))
  default = []
}

variable "vpc_connector" {
  description = "VPC Access Connector ID for private networking"
  type        = string
  default     = null
}

variable "vpc_egress" {
  description = "VPC egress setting (ALL_TRAFFIC, PRIVATE_RANGES_ONLY)"
  type        = string
  default     = "PRIVATE_RANGES_ONLY"

  validation {
    condition     = var.vpc_egress == "ALL_TRAFFIC" || var.vpc_egress == "PRIVATE_RANGES_ONLY"
    error_message = "VPC egress must be ALL_TRAFFIC or PRIVATE_RANGES_ONLY."
  }
}

variable "labels" {
  description = "Labels to apply to the job"
  type        = map(string)
  default     = {}
}

# =============================================================================
# Dev-specific Configuration Variables
# =============================================================================

variable "is_dev_environment" {
  description = "Enable dev-specific hardcoded configuration (platform=vn2, tier=diamond, division=III, page=1)"
  type        = bool
  default     = false
}

variable "platform" {
  description = "Target platform for jobs (e.g., 'vn2'). When is_dev_environment is true, uses hardcoded 'vn2'"
  type        = string
  default     = null
}

variable "tier" {
  description = "Target tier for jobs (e.g., 'diamond'). When is_dev_environment is true, uses hardcoded 'diamond'"
  type        = string
  default     = null
}

variable "division" {
  description = "Target division for jobs (e.g., 'III'). When is_dev_environment is true, uses hardcoded 'III'"
  type        = string
  default     = null
}

variable "page_limit" {
  description = "Limit of pages to fetch (1-100). When is_dev_environment is true, uses hardcoded 1"
  type        = number
  default     = null

  validation {
    condition     = var.page_limit == null || (var.page_limit >= 1 && var.page_limit <= 100)
    error_message = "Page limit must be between 1 and 100, or null."
  }
}
