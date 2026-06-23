# DataRift Dev Environment
# Infrastructure for Job A (User Ingestion) pipeline only

terraform {
  required_version = "~> 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "datarift-terraform-state" # Update this to your bucket
    prefix = "dev"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# =============================================================================
# Service Account
# =============================================================================

resource "google_service_account" "dev_job_sa" {
  account_id   = var.service_account_id
  display_name = "DataRift ${title(var.environment)} Job Service Account"
  description  = "Service account for running DataRift Cloud Run Jobs in ${var.environment}"
}

# =============================================================================
# IAM Roles
# =============================================================================

resource "google_project_iam_member" "job_sa_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dev_job_sa.email}"
}

resource "google_project_iam_member" "job_sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dev_job_sa.email}"
}

# =============================================================================
# GCS Bucket
# =============================================================================

resource "google_storage_bucket" "lakehouse" {
  name          = var.lakehouse_bucket
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30 # Move to Nearline after 30 days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    application = "datarift"
    environment = var.environment
  }
}

# =============================================================================
# Job A: User Ingestion
# =============================================================================

module "job_a_user_ingestion" {
  source = "../shared/modules/cloud_run_job"

  name            = "job-a-user-ingestion"
  location        = var.region
  image           = "${var.region}-docker.pkg.dev/${var.project_id}/datarift/jobs/job-a-user-ingestion:${var.image_tag}"
  service_account = google_service_account.dev_job_sa.email

  task_count = var.job_a_user_ingestion.task_count
  cpu        = var.job_a_user_ingestion.cpu
  memory     = var.job_a_user_ingestion.memory
  timeout    = var.job_a_user_ingestion.timeout

  env = [
    { name = "ENV", value = var.environment },
    { name = "GCS_BUCKET", value = google_storage_bucket.lakehouse.name },
    { name = "GCP_PROJECT", value = var.project_id },
  ]

  labels = {
    application = "datarift"
    environment = var.environment
    job         = "ingestion"
    pipeline    = "a"
  }
}
