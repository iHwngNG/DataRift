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

# Fetch the active Google Cloud project details to retrieve the project number
data "google_project" "project" {}

# Grant the Default Compute Service Account permission to access this secret at runtime
resource "google_secret_manager_secret_iam_member" "riot_api_key_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.riot_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}
