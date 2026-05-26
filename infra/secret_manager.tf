# ==============================================================================
# Google Cloud Secret Manager Resources (DataRift Security)
# ==============================================================================

# ------------------------------------------------------------------------------
# Secret Manager: Secret for the Riot Developer API Key
# Ensures credentials are never hardcoded or exposed in configuration repositories.
# ------------------------------------------------------------------------------
resource "google_secret_manager_secret" "riot_api_key" {
  secret_id = "riot-api-key-${var.environment}"

  replication {
    auto {}
  }

  labels = {
    project     = "datarift"
    component   = "security"
    environment = var.environment
  }
}
