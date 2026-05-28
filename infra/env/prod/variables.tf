variable "project_id" {
  description = "The GCP project ID to deploy resources to for the production environment."
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in for the production environment."
  type        = string
  default     = "us-central1"
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
