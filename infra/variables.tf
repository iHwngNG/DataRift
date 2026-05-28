variable "project_id" {
  description = "The GCP project ID to deploy resources to."
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in."
  type        = string
  default     = "asia-southeast3"
}

variable "environment" {
  description = "The environment name (e.g. dev, prod)."
  type        = string
  default     = "dev"
}

variable "storage_class" {
  description = "The default storage class for the raw data GCS bucket."
  type        = string
  default     = "STANDARD"
}

variable "fetch_users_jobs" {
  description = "Configuration for fetch_users Cloud Run Jobs"
  type = map(object({
    platform  = string
    region    = string
    tiers     = string
    divisions = string
    max_pages = string
  }))
  default = {}
}

variable "parquet_writer_jobs" {
  description = "Configuration for parquet_writer Cloud Run Jobs"
  type = map(object({
    platform        = string
    subscription_id = string
    max_messages    = optional(string, "500")
    max_runtime_s   = optional(string, "3600")
  }))
  default = {}
}
