# =============================================================================
# Cloud Run Job Module - Parent
# =============================================================================
# This module creates all DataRift pipeline jobs by calling the job/ child module.
# Variables prefixed with job_ are for the individual job configurations.

variable "location" {
  description = "GCP region for the jobs"
  type        = string
}

variable "service_account" {
  description = "Service account email to run the jobs with"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "gcp_project" {
  description = "GCP Project ID"
  type        = string
}

variable "image_tag" {
  description = "Docker image tag for all jobs"
  type        = string
}

variable "gcs_bucket" {
  description = "GCS bucket name for data lakehouse"
  type        = string
}

variable "shard_count" {
  description = "Number of shards for worker jobs"
  type        = number
  default     = 1
}

variable "is_dev_environment" {
  description = "Enable dev-specific hardcoded configuration"
  type        = bool
  default     = false
}

variable "platform" {
  description = "Target platform for jobs"
  type        = string
  default     = null
}

variable "tier" {
  description = "Target tier for jobs"
  type        = string
  default     = null
}

variable "division" {
  description = "Target division for jobs"
  type        = string
  default     = null
}

variable "page_limit" {
  description = "Limit of pages to fetch"
  type        = number
  default     = null
}

variable "base_labels" {
  description = "Base labels shared across all jobs"
  type        = map(string)
  default     = {}
}

# -----------------------------------------------------------------------------
# Job A Variables
# -----------------------------------------------------------------------------

variable "job_a_task_count" {
  description = "Task count for Job A"
  type        = number
  default     = 1
}

variable "job_a_cpu" {
  description = "CPU limit for Job A"
  type        = string
  default     = "1"
}

variable "job_a_memory" {
  description = "Memory limit for Job A"
  type        = string
  default     = "512Mi"
}

variable "job_a_timeout" {
  description = "Timeout for Job A"
  type        = number
  default     = 300
}

# -----------------------------------------------------------------------------
# Job B Distributor Variables
# -----------------------------------------------------------------------------

variable "job_b_distributor_task_count" {
  description = "Task count for Job B distributor"
  type        = number
  default     = 1
}

variable "job_b_distributor_cpu" {
  description = "CPU limit for Job B distributor"
  type        = string
  default     = "1"
}

variable "job_b_distributor_memory" {
  description = "Memory limit for Job B distributor"
  type        = string
  default     = "512Mi"
}

variable "job_b_distributor_timeout" {
  description = "Timeout for Job B distributor"
  type        = number
  default     = 300
}

# -----------------------------------------------------------------------------
# Job B Worker Variables
# -----------------------------------------------------------------------------

variable "job_b_worker_task_count" {
  description = "Task count for Job B worker"
  type        = number
  default     = 1
}

variable "job_b_worker_cpu" {
  description = "CPU limit for Job B worker"
  type        = string
  default     = "1"
}

variable "job_b_worker_memory" {
  description = "Memory limit for Job B worker"
  type        = string
  default     = "512Mi"
}

variable "job_b_worker_timeout" {
  description = "Timeout for Job B worker"
  type        = number
  default     = 300
}

# -----------------------------------------------------------------------------
# Job C Distributor Variables
# -----------------------------------------------------------------------------

variable "job_c_distributor_task_count" {
  description = "Task count for Job C distributor"
  type        = number
  default     = 1
}

variable "job_c_distributor_cpu" {
  description = "CPU limit for Job C distributor"
  type        = string
  default     = "1"
}

variable "job_c_distributor_memory" {
  description = "Memory limit for Job C distributor"
  type        = string
  default     = "512Mi"
}

variable "job_c_distributor_timeout" {
  description = "Timeout for Job C distributor"
  type        = number
  default     = 300
}

# -----------------------------------------------------------------------------
# Job C Worker Variables
# -----------------------------------------------------------------------------

variable "job_c_worker_task_count" {
  description = "Task count for Job C worker"
  type        = number
  default     = 1
}

variable "job_c_worker_cpu" {
  description = "CPU limit for Job C worker"
  type        = string
  default     = "1"
}

variable "job_c_worker_memory" {
  description = "Memory limit for Job C worker"
  type        = string
  default     = "512Mi"
}

variable "job_c_worker_timeout" {
  description = "Timeout for Job C worker"
  type        = number
  default     = 300
}
