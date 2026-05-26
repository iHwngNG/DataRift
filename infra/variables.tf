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
