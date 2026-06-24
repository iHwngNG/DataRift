# =============================================================================
# Cloud Run Job Definitions
# =============================================================================
# This file contains all DataRift pipeline job definitions.
# Dev-specific values (platform=vn2, tier=diamond, division=III, page=1) are
# applied conditionally when is_dev_environment = true.

locals {
  image_registry = "${var.location}-docker.pkg.dev/${var.gcp_project}/datarift/jobs"
}

# =============================================================================
# Job A: User Ingestion
# =============================================================================

module "job_a_user_ingestion" {
  source = "./job"

  name            = "job-a-user-ingestion"
  location        = var.location
  image           = "${local.image_registry}/job-a-user-ingestion:${var.image_tag}"
  service_account = var.service_account

  task_count = var.job_a_task_count
  cpu        = var.job_a_cpu
  memory     = var.job_a_memory
  timeout    = var.job_a_timeout

  is_dev_environment = var.is_dev_environment
  platform           = var.platform
  tier               = var.tier
  division           = var.division
  page_limit         = var.page_limit

  env = [
    { name = "ENV", value = var.environment },
    { name = "GCS_BUCKET", value = var.gcs_bucket },
    { name = "GCP_PROJECT", value = var.gcp_project },
  ]

  labels = merge(var.base_labels, {
    job      = "ingestion"
    pipeline = "a"
  })
}

# =============================================================================
# Job B Distributor
# =============================================================================

module "job_b_distributor" {
  source = "./job"

  name            = "job-b-distributor"
  location        = var.location
  image           = "${local.image_registry}/job-b-distributor:${var.image_tag}"
  service_account = var.service_account

  task_count = var.job_b_distributor_task_count
  cpu        = var.job_b_distributor_cpu
  memory     = var.job_b_distributor_memory
  timeout    = var.job_b_distributor_timeout

  is_dev_environment = var.is_dev_environment
  platform           = var.platform
  tier               = var.tier
  division           = var.division
  page_limit         = var.page_limit

  env = [
    { name = "ENV", value = var.environment },
    { name = "SHARD_COUNT", value = tostring(var.shard_count) },
    { name = "GCS_BUCKET", value = var.gcs_bucket },
    { name = "GCP_PROJECT", value = var.gcp_project },
  ]

  labels = merge(var.base_labels, {
    job = "distributor"
  })
}

# =============================================================================
# Job B Workers (one per shard)
# =============================================================================

module "job_b_worker" {
  source = "./job"
  count  = var.shard_count

  name            = "job-b-worker-${count.index}"
  location        = var.location
  image           = "${local.image_registry}/job-b-worker:${var.image_tag}"
  service_account = var.service_account

  task_count = var.job_b_worker_task_count
  cpu        = var.job_b_worker_cpu
  memory     = var.job_b_worker_memory
  timeout    = var.job_b_worker_timeout

  is_dev_environment = var.is_dev_environment
  platform           = var.platform
  tier               = var.tier
  division           = var.division
  page_limit         = var.page_limit

  env = [
    { name = "SHARD_ID", value = tostring(count.index) },
    { name = "ENV", value = var.environment },
    { name = "GCS_BUCKET", value = var.gcs_bucket },
    { name = "GCP_PROJECT", value = var.gcp_project },
  ]

  labels = merge(var.base_labels, {
    job   = "worker"
    shard = tostring(count.index)
  })
}

# =============================================================================
# Job C Distributor
# =============================================================================

module "job_c_distributor" {
  source = "./job"

  name            = "job-c-distributor"
  location        = var.location
  image           = "${local.image_registry}/job-c-distributor:${var.image_tag}"
  service_account = var.service_account

  task_count = var.job_c_distributor_task_count
  cpu        = var.job_c_distributor_cpu
  memory     = var.job_c_distributor_memory
  timeout    = var.job_c_distributor_timeout

  is_dev_environment = var.is_dev_environment
  platform           = var.platform
  tier               = var.tier
  division           = var.division
  page_limit         = var.page_limit

  env = [
    { name = "ENV", value = var.environment },
    { name = "SHARD_COUNT", value = tostring(var.shard_count) },
    { name = "GCS_BUCKET", value = var.gcs_bucket },
    { name = "GCP_PROJECT", value = var.gcp_project },
  ]

  labels = merge(var.base_labels, {
    job      = "distributor"
    pipeline = "c"
  })
}

# =============================================================================
# Job C Workers (one per shard)
# =============================================================================

module "job_c_worker" {
  source = "./job"
  count  = var.shard_count

  name            = "job-c-worker-${count.index}"
  location        = var.location
  image           = "${local.image_registry}/job-c-worker:${var.image_tag}"
  service_account = var.service_account

  task_count = var.job_c_worker_task_count
  cpu        = var.job_c_worker_cpu
  memory     = var.job_c_worker_memory
  timeout    = var.job_c_worker_timeout

  is_dev_environment = var.is_dev_environment
  platform           = var.platform
  tier               = var.tier
  division           = var.division
  page_limit         = var.page_limit

  env = [
    { name = "SHARD_ID", value = tostring(count.index) },
    { name = "ENV", value = var.environment },
    { name = "GCS_BUCKET", value = var.gcs_bucket },
    { name = "GCP_PROJECT", value = var.gcp_project },
  ]

  labels = merge(var.base_labels, {
    job      = "worker"
    pipeline = "c"
    shard    = tostring(count.index)
  })
}
