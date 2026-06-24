# DataRift Dev Environment
# Infrastructure for Job A, Job B, and Job C pipelines

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
# Cloud Run Jobs (using shared module with dev-specific configuration)
# =============================================================================

module "datarift_jobs" {
  source = "../shared/modules/cloud_run_job"

  # Location and identity
  location        = var.region
  service_account = google_service_account.dev_job_sa.email
  environment     = var.environment

  # Shared resources
  gcp_project = var.project_id
  gcs_bucket  = google_storage_bucket.lakehouse.name
  shard_count = var.shard_count

  # Dev-specific configuration
  is_dev_environment = true

  # Image tag for all jobs
  image_tag = var.image_tag

  # Job A User Ingestion
  job_a_task_count = var.job_a_user_ingestion.task_count
  job_a_cpu        = var.job_a_user_ingestion.cpu
  job_a_memory     = var.job_a_user_ingestion.memory
  job_a_timeout    = var.job_a_user_ingestion.timeout

  # Job B Distributor
  job_b_distributor_task_count = var.job_b_distributor.task_count
  job_b_distributor_cpu        = var.job_b_distributor.cpu
  job_b_distributor_memory     = var.job_b_distributor.memory
  job_b_distributor_timeout    = var.job_b_distributor.timeout

  # Job B Worker
  job_b_worker_task_count = var.job_b_worker.task_count
  job_b_worker_cpu        = var.job_b_worker.cpu
  job_b_worker_memory     = var.job_b_worker.memory
  job_b_worker_timeout    = var.job_b_worker.timeout

  # Job C Distributor
  job_c_distributor_task_count = var.job_c_distributor.task_count
  job_c_distributor_cpu        = var.job_c_distributor.cpu
  job_c_distributor_memory     = var.job_c_distributor.memory
  job_c_distributor_timeout    = var.job_c_distributor.timeout

  # Job C Worker
  job_c_worker_task_count = var.job_c_worker.task_count
  job_c_worker_cpu        = var.job_c_worker.cpu
  job_c_worker_memory     = var.job_c_worker.memory
  job_c_worker_timeout    = var.job_c_worker.timeout

  base_labels = {
    application = "datarift"
    environment = var.environment
  }
}

# =============================================================================
# Pub/Sub for Job C
# =============================================================================

module "job_c_pubsub" {
  source = "../shared/modules/pubsub"

  topic_name = var.job_c_topic_name
  project_id = var.project_id

  subscriptions = [
    for i in range(var.shard_count) : {
      name          = "${var.pubsub_topic_prefix}-job-c-worker-${i}"
      push_endpoint = "https://${module.datarift_jobs.job_c_worker_names[i]}-${var.region}.a.run.app"
    }
  ]

  dlq_topic_name          = var.job_c_dlq_topic_name
  max_delivery_attempts   = 5
  min_backoff             = "10s"
  max_backoff             = "600s"
  enable_dlq_subscription = true
  push_service_account    = google_service_account.dev_job_sa.email

  labels = {
    application = "datarift"
    environment = var.environment
    pipeline    = "c"
  }
}

module "job_b_pubsub" {
  source = "../shared/modules/pubsub"

  topic_name = var.job_b_topic_name
  project_id = var.project_id

  subscriptions = [
    for i in range(var.shard_count) : {
      name          = "${var.pubsub_topic_prefix}-job-b-worker-${i}"
      push_endpoint = "https://${module.datarift_jobs.job_b_worker_names[i]}-${var.region}.a.run.app"
    }
  ]

  dlq_topic_name          = var.job_b_dlq_topic_name
  max_delivery_attempts   = 5
  min_backoff             = "10s"
  max_backoff             = "600s"
  enable_dlq_subscription = true
  push_service_account    = google_service_account.dev_job_sa.email

  labels = {
    application = "datarift"
    environment = var.environment
  }
}

# =============================================================================
# Pub/Sub IAM
# =============================================================================

resource "google_pubsub_topic_iam_member" "distributor_publisher" {
  topic  = module.job_b_pubsub.topic_name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.dev_job_sa.email}"
}

resource "google_pubsub_subscription_iam_member" "worker_subscriber" {
  for_each = toset([for i in range(var.shard_count) : "${var.pubsub_topic_prefix}-job-b-worker-${i}"])

  subscription = each.key
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.dev_job_sa.email}"

  depends_on = [
    module.job_b_pubsub.subscription_names
  ]
}

# =============================================================================
# Job C Pub/Sub IAM
# =============================================================================

resource "google_pubsub_topic_iam_member" "job_c_distributor_publisher" {
  topic  = module.job_c_pubsub.topic_name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.dev_job_sa.email}"
}

resource "google_pubsub_subscription_iam_member" "job_c_worker_subscriber" {
  for_each = toset([for i in range(var.shard_count) : "${var.pubsub_topic_prefix}-job-c-worker-${i}"])

  subscription = each.key
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.dev_job_sa.email}"

  depends_on = [
    module.job_c_pubsub.subscription_names
  ]
}

