# =============================================================================
# Single Cloud Run Job Resource
# =============================================================================

locals {
  # Dev-specific environment variables (applied when is_dev_environment = true)
  dev_env = var.is_dev_environment ? [
    { name = "PLATFORM", value = "vn2" },
    { name = "TIER", value = "diamond" },
    { name = "DIVISION", value = "III" },
    { name = "PAGE", value = "1" },
  ] : []

  # Override environment variables (used when is_dev_environment = false)
  override_env = flatten([
    var.platform != null ? [{ name = "PLATFORM", value = var.platform }] : [],
    var.tier != null ? [{ name = "TIER", value = var.tier }] : [],
    var.division != null ? [{ name = "DIVISION", value = var.division }] : [],
    var.page_limit != null ? [{ name = "PAGE", value = tostring(var.page_limit) }] : [],
  ])

  # Final environment variables: dev hardcoded values take precedence when in dev mode
  computed_env = concat(local.dev_env, local.override_env, var.env)
}

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

        # Non-sensitive environment variables (including dev-specific)
        dynamic "env" {
          for_each = local.computed_env
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
