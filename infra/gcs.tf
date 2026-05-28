# ==============================================================================
# Google Cloud Storage Buckets (DataRift Lakehouse Layers)
# ==============================================================================

# ------------------------------------------------------------------------------
# Bronze Layer: Raw JSON payloads from Riot API
# Expected Logical Partition Schema (application-managed paths):
# - lol/user/{region}/{platform}/{year}/{month}/{date}/*
# - lol/match/{region}/{platform}/{year}/{month}/{date}/*
# - lol/runes/*
# - lol/skills/*
# - lol/champion/*
#
# NOTE: year, month, and date must be based on the game/data creation datetime,
# not the ingestion execution datetime.
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "bronze_raw" {
  name          = "datarift-lol-lakehouse-${var.project_id}"
  location      = var.region
  storage_class = var.storage_class

  # Prevent accidental deletion in production
  force_destroy = var.environment == "prod" ? false : true

  # Security best practices
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Keep previous versions of raw files for safety / recovery
  versioning {
    enabled = true
  }

  # Cost Optimization: Transition old raw data to Nearline storage after 90 days
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 90
    }
  }

  labels = {
    project     = "datarift"
    layer       = "bronze"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# Silver Layer: Structured data in Parquet format with Apache Iceberg metadata
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "silver_lakehouse" {
  name          = "datarift-silver-${var.project_id}"
  location      = var.region
  storage_class = "STANDARD"

  # Prevent accidental deletion in production
  force_destroy = var.environment == "prod" ? false : true

  # Security best practices
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Versioning for silver is generally disabled or managed differently for Iceberg.
  # Iceberg manages its own snapshots and historical states (time-travel),
  # so standard GCS bucket versioning can lead to unnecessary storage overhead.
  versioning {
    enabled = false
  }

  labels = {
    project     = "datarift"
    layer       = "silver"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# GCS IAM Access Control: Grant Compute Service Account storage object admin access
# ------------------------------------------------------------------------------
resource "google_storage_bucket_iam_member" "bronze_raw_writer" {
  bucket = google_storage_bucket.bronze_raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "silver_lakehouse_writer" {
  bucket = google_storage_bucket.silver_lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

# Grant project-level Storage Object Viewer to the Default Compute Service Account
# This is required for Google Cloud Build to read uploaded source archives from the staging bucket.
resource "google_project_iam_member" "compute_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}
