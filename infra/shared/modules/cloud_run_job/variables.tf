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

# NOTE: min_instances and max_instances apply to Cloud Run Services (v2), not Jobs.
# Jobs are transient and run to completion. Remove these if you don't need them for Services.

# variable "min_instances" {
#   description = "Minimum number of instances (0 for scale-to-zero)"
#   type        = number
#   default     = 0
#
#   validation {
#     condition     = var.min_instances >= 0 && var.min_instances <= 100
#     error_message = "Min instances must be between 0 and 100."
#   }
# }

# variable "max_instances" {
#   description = "Maximum number of instances"
#   type        = number
#   default     = 10
#
#   validation {
#     condition     = var.max_instances >= 1 && var.max_instances <= 100
#     error_message = "Max instances must be between 1 and 100."
#   }
# }

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

variable "ingress" {
  description = "Ingress settings for the job (ALL, INTERNAL, INTERNAL_LOAD_BALANCER)"
  type        = string
  default     = "INGRESS_TRAFFIC_ALL"
}

variable "labels" {
  description = "Labels to apply to the job"
  type        = map(string)
  default     = {}
}

variable "invoker_members" {
  description = "IAM members who can invoke the job (e.g., 'serviceAccount:sa@project.iam.gserviceaccount.com')"
  type        = string
  default     = "allAuthenticatedUsers"
}
