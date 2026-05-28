# ==============================================================================
# Google Cloud Run Jobs (DataRift Player Data Fetchers)
# ==============================================================================

# ------------------------------------------------------------------------------
# fetch_users — Riot API producer; publishes records to Pub/Sub
# ------------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "fetch_users" {
  for_each = var.fetch_users_jobs

  name                = "fetch-users-${each.key}-${var.environment}"
  location            = var.region
  deletion_protection = false

  template {
    template {
      max_retries = 3

      containers {
        # Dynamically build image URI using the Artifact Registry repository created in the module
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/fetch-users:latest"

        # Platform override
        env {
          name  = "RIOT_PLATFORM"
          value = each.value.platform
        }

        # Regional routing for match API requests
        env {
          name  = "RIOT_REGION"
          value = each.value.region
        }

        # Target tiers to fetch (e.g. DIAMOND)
        env {
          name  = "RIOT_TIERS"
          value = each.value.tiers
        }

        # Target divisions to fetch (e.g. III)
        env {
          name  = "RIOT_DIVISIONS"
          value = each.value.divisions
        }

        # Limit pages fetched per execution
        env {
          name  = "PIPELINE_MAX_PAGES"
          value = each.value.max_pages
        }

        # GCP project — used to build the Pub/Sub topic path
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }

        # Pub/Sub topic ID for the league-entries data pipeline
        env {
          name  = "PUBSUB_TOPIC_ID"
          value = google_pubsub_topic.league_entries.name
        }

        # Raw GCS landing bucket (kept for potential future direct-write fallback)
        env {
          name  = "GCS_BRONZE_BUCKET"
          value = google_storage_bucket.bronze_raw.name
        }

        # Riot developer key sourced securely from Secret Manager
        env {
          name = "RIOT_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.riot_api_key.secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  # Allow external orchestration to update image tag without Terraform reverting it
  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].image,
    ]
  }
}

# ------------------------------------------------------------------------------
# parquet_writer — Pub/Sub consumer; writes Parquet files to GCS
# ------------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "parquet_writer" {
  for_each = var.parquet_writer_jobs

  name                = "parquet-writer-${each.key}-${var.environment}"
  location            = var.region
  deletion_protection = false

  template {
    template {
      max_retries = 2

      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/parquet-writer:latest"

        # Platform tag (used for partition path in GCS)
        env {
          name  = "RIOT_PLATFORM"
          value = each.value.platform
        }

        # GCP project ID for Pub/Sub subscription path
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }

        # Pub/Sub subscription to consume from
        env {
          name  = "PUBSUB_SUBSCRIPTION_ID"
          value = each.value.subscription_id
        }

        # Max messages to pull per batch
        env {
          name  = "PUBSUB_MAX_MESSAGES"
          value = each.value.max_messages
        }

        # Wall-clock job budget in seconds
        env {
          name  = "MAX_RUNTIME_SECONDS"
          value = each.value.max_runtime_s
        }

        # Raw GCS landing bucket
        env {
          name  = "GCS_BRONZE_BUCKET"
          value = google_storage_bucket.bronze_raw.name
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].image,
    ]
  }
}
