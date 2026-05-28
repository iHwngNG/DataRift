variable "project_id" {
  description = "The GCP project ID to deploy resources to for the development environment."
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in for the development environment."
  type        = string
  default     = "asia-southeast3"
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
