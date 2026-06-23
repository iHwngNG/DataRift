# =============================================================================
# DataRift Dev Environment - Variables
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_id))
    error_message = "Project ID must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "region" {
  description = "GCP Region for resources"
  type        = string

  validation {
    condition = contains([
      "asia-southeast1",
      "asia-southeast3",
      "asia-east1",
      "us-central1",
      "us-east1",
      "europe-west1"
    ], var.region)
    error_message = "Region must be a valid GCP region."
  }
}

variable "image_tag" {
  description = "Docker image tag to deploy"
  type        = string
}

# -----------------------------------------------------------------------------
# GCS Buckets
# -----------------------------------------------------------------------------

variable "lakehouse_bucket" {
  description = "GCS bucket name for data lakehouse"
  type        = string
}

variable "terraform_state_bucket" {
  description = "GCS bucket name for Terraform state storage"
  type        = string
}

# -----------------------------------------------------------------------------
# Cloud Run Job Configurations
# -----------------------------------------------------------------------------

variable "job_b_distributor" {
  description = "Job B Distributor Cloud Run Job configuration"
  type = object({
    cpu        = string
    memory     = string
    timeout    = number
    task_count = number
  })

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.job_b_distributor.memory))
    error_message = "Memory must be in Mi or Gi format (e.g., '512Mi', '1Gi')."
  }

  validation {
    condition     = var.job_b_distributor.timeout > 0 && var.job_b_distributor.timeout <= 3600
    error_message = "Timeout must be between 1 and 3600 seconds."
  }

  validation {
    condition     = var.job_b_distributor.task_count >= 1 && var.job_b_distributor.task_count <= 100
    error_message = "Task count must be between 1 and 100."
  }
}

variable "job_b_worker" {
  description = "Job B Worker Cloud Run Job configuration"
  type = object({
    cpu        = string
    memory     = string
    timeout    = number
    task_count = number
  })

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.job_b_worker.memory))
    error_message = "Memory must be in Mi or Gi format (e.g., '512Mi', '1Gi')."
  }

  validation {
    condition     = var.job_b_worker.timeout > 0 && var.job_b_worker.timeout <= 3600
    error_message = "Timeout must be between 1 and 3600 seconds."
  }

  validation {
    condition     = var.job_b_worker.task_count >= 1 && var.job_b_worker.task_count <= 100
    error_message = "Task count must be between 1 and 100."
  }
}

# -----------------------------------------------------------------------------
# Pub/Sub Configuration
# -----------------------------------------------------------------------------

variable "pubsub_topic_prefix" {
  description = "Prefix for Pub/Sub topic names"
  type        = string
}

variable "job_b_topic_name" {
  description = "Job B trigger topic name"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9-_.~+%]+$", var.job_b_topic_name))
    error_message = "Topic name must contain only letters, numbers, hyphens, dots, tildes, underscores, and percent signs."
  }
}

variable "job_b_dlq_topic_name" {
  description = "Job B dead-letter queue topic name"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9-_.~+%]+$", var.job_b_dlq_topic_name))
    error_message = "Topic name must contain only letters, numbers, hyphens, dots, tildes, underscores, and percent signs."
  }
}

# -----------------------------------------------------------------------------
# Sharding Configuration
# -----------------------------------------------------------------------------

variable "shard_count" {
  description = "Number of shards (equals number of platforms)"
  type        = number
  default     = 1

  validation {
    condition     = var.shard_count >= 1
    error_message = "Shard count must be at least 1."
  }
}

# -----------------------------------------------------------------------------
# Service Account
# -----------------------------------------------------------------------------

variable "service_account_id" {
  description = "Service account ID for Cloud Run Jobs"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.service_account_id))
    error_message = "Service account ID must contain only lowercase letters, numbers, and hyphens."
  }
}

# -----------------------------------------------------------------------------
# Labels
# -----------------------------------------------------------------------------

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

# -----------------------------------------------------------------------------
# Job A: User Ingestion Configuration
# -----------------------------------------------------------------------------

variable "job_a_user_ingestion" {
  description = "Job A User Ingestion Cloud Run Job configuration"
  type = object({
    cpu        = string
    memory     = string
    timeout    = number
    task_count = number
  })

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.job_a_user_ingestion.memory))
    error_message = "Memory must be in Mi or Gi format (e.g., '512Mi', '1Gi')."
  }

  validation {
    condition     = var.job_a_user_ingestion.timeout > 0 && var.job_a_user_ingestion.timeout <= 3600
    error_message = "Timeout must be between 1 and 3600 seconds."
  }

  validation {
    condition     = var.job_a_user_ingestion.task_count >= 1 && var.job_a_user_ingestion.task_count <= 100
    error_message = "Task count must be between 1 and 100."
  }
}

# -----------------------------------------------------------------------------
# Job C: Match Ingestion Configuration
# -----------------------------------------------------------------------------

variable "job_c_distributor" {
  description = "Job C Distributor Cloud Run Job configuration"
  type = object({
    cpu        = string
    memory     = string
    timeout    = number
    task_count = number
  })

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.job_c_distributor.memory))
    error_message = "Memory must be in Mi or Gi format (e.g., '512Mi', '1Gi')."
  }

  validation {
    condition     = var.job_c_distributor.timeout > 0 && var.job_c_distributor.timeout <= 3600
    error_message = "Timeout must be between 1 and 3600 seconds."
  }

  validation {
    condition     = var.job_c_distributor.task_count >= 1 && var.job_c_distributor.task_count <= 100
    error_message = "Task count must be between 1 and 100."
  }
}

variable "job_c_worker" {
  description = "Job C Worker Cloud Run Job configuration"
  type = object({
    cpu        = string
    memory     = string
    timeout    = number
    task_count = number
  })

  validation {
    condition     = can(regex("^[0-9]+(Mi|Gi)$", var.job_c_worker.memory))
    error_message = "Memory must be in Mi or Gi format (e.g., '512Mi', '1Gi')."
  }

  validation {
    condition     = var.job_c_worker.timeout > 0 && var.job_c_worker.timeout <= 3600
    error_message = "Timeout must be between 1 and 3600 seconds."
  }

  validation {
    condition     = var.job_c_worker.task_count >= 1 && var.job_c_worker.task_count <= 100
    error_message = "Task count must be between 1 and 100."
  }
}

# -----------------------------------------------------------------------------
# Pub/Sub Configuration for Job C
# -----------------------------------------------------------------------------

variable "job_c_topic_name" {
  description = "Job C trigger topic name"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9-_.~+%]+$", var.job_c_topic_name))
    error_message = "Topic name must contain only letters, numbers, hyphens, dots, tildes, underscores, and percent signs."
  }
}

variable "job_c_dlq_topic_name" {
  description = "Job C dead-letter queue topic name"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9-_.~+%]+$", var.job_c_dlq_topic_name))
    error_message = "Topic name must contain only letters, numbers, hyphens, dots, tildes, underscores, and percent signs."
  }
}
