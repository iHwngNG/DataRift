variable "project_id" {
  description = "The GCP project ID to deploy resources to for the development environment."
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in for the development environment."
  type        = string
  default     = "asia-southeast3"
}
