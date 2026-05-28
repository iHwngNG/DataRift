# ==============================================================================
# Development Environment - Main Configuration
# ==============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

# The provider config belongs here in the root orchestration directory for Dev
provider "google" {
  project = var.project_id
  region  = var.region
}

# Instantiate the GCS base module from the parent directory
module "gcs_base" {
  source      = "../../"
  project_id  = var.project_id
  region      = var.region
  environment = "dev"
}
