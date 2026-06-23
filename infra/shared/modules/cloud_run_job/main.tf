# Cloud Run Job Module
# Creates a Google Cloud Run Job with configurable resources and environment

resource "google_cloud_run_v2_job" "this" {
  name     = var.name
  location = var.location

  template {
    template {
      containers {
        image = var.image
        resources {
          limits = {
            cpu    = var.cpu
            memory = var.memory
          }
        }

        # Non-sensitive environment variables
        dynamic "env" {
          for_each = var.env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        # Secret environment variables from Secret Manager
        dynamic "env" {
          for_each = var.secret_env
          content {
            name = env.value.name
            value_source {
              secret_key_ref {
                secret  = env.value.secret_name
                version = env.value.version
              }
            }
          }
        }

        dynamic "volume_mounts" {
          for_each = var.volume_mounts
          content {
            name       = volume_mounts.value.name
            mount_path = volume_mounts.value.mount_path
          }
        }
      }

      timeout = "${var.timeout}s"

      dynamic "vpc_access" {
        for_each = var.vpc_connector != null ? [1] : []
        content {
          connector = var.vpc_connector
          egress    = var.vpc_egress
        }
      }
    }
  }

  labels = var.labels

  lifecycle {
    ignore_changes = [
      labels["run.googleapis.com/ingress"],
    ]
  }
}
