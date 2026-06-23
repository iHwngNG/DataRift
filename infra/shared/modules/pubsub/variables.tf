variable "topic_name" {
  description = "Name of the Pub/Sub topic"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9-_.~+%]+$", var.topic_name))
    error_message = "Topic name must contain only letters, numbers, hyphens, dots, tildes, underscores, and percent signs."
  }
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "message_retention_duration" {
  description = "How long to retain messages (in seconds, max 604800 for 7 days)"
  type        = string
  default     = "604800s"

  validation {
    condition     = can(regex("^[0-9]+s$", var.message_retention_duration))
    error_message = "Message retention duration must be in seconds (e.g., '604800s')."
  }
}

variable "labels" {
  description = "Labels to apply to topic and subscriptions"
  type        = map(string)
  default     = {}
}

variable "publisher_members" {
  description = "IAM members who can publish to the topic"
  type        = string
  default     = "allUsers"
}

variable "subscriptions" {
  description = "List of subscription configurations"
  type = list(object({
    name                 = string
    push_endpoint        = optional(string)
    bigquery_table       = optional(string)
    ack_deadline_seconds = optional(number)
  }))
  default = []
}

variable "default_ack_deadline_seconds" {
  description = "Default ack deadline for subscriptions (10-600 seconds)"
  type        = number
  default     = 300

  validation {
    condition     = var.default_ack_deadline_seconds >= 10 && var.default_ack_deadline_seconds <= 600
    error_message = "Ack deadline must be between 10 and 600 seconds."
  }
}

variable "retain_acked_messages" {
  description = "Whether to retain acknowledged messages"
  type        = bool
  default     = false
}

variable "dlq_topic_name" {
  description = "Name for the dead-letter topic (null to disable DLQ)"
  type        = string
  default     = null
}

variable "max_delivery_attempts" {
  description = "Maximum delivery attempts before sending to DLQ"
  type        = number
  default     = 5

  validation {
    condition     = var.max_delivery_attempts >= 1 && var.max_delivery_attempts <= 100
    error_message = "Max delivery attempts must be between 1 and 100."
  }
}

variable "min_backoff" {
  description = "Minimum retry backoff (e.g., '10s', '60s')"
  type        = string
  default     = "10s"

  validation {
    condition     = can(regex("^[0-9]+s$", var.min_backoff))
    error_message = "Min backoff must be in seconds format (e.g., '10s')."
  }
}

variable "max_backoff" {
  description = "Maximum retry backoff (e.g., '600s', '3600s')"
  type        = string
  default     = "600s"

  validation {
    condition     = can(regex("^[0-9]+s$", var.max_backoff))
    error_message = "Max backoff must be in seconds format (e.g., '600s')."
  }
}

variable "push_service_account" {
  description = "Service account email for push subscription OIDC tokens"
  type        = string
  default     = null
}

variable "subscription_expiration_policy" {
  description = "When subscriptions expire (TTL format, use empty string for never)"
  type        = string
  default     = ""

  validation {
    condition     = var.subscription_expiration_policy == "" || can(regex("^[0-9]+s$", var.subscription_expiration_policy))
    error_message = "Expiration policy must be empty (never) or in seconds format."
  }
}

variable "enable_dlq_subscription" {
  description = "Create a subscription for the DLQ topic for monitoring"
  type        = bool
  default     = false
}
