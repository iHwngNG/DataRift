variable "project_id" {
  description = "The GCP project ID to deploy resources to for the production environment."
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in for the production environment."
  type        = string
  default     = "us-central1"
}
