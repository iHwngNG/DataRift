# ==============================================================================
# Google Cloud Artifact Registry Repository for DataRift
# ==============================================================================

resource "google_artifact_registry_repository" "docker_repo" {
  repository_id = "datarift-docker-repo"
  location      = var.region
  format        = "DOCKER"
  description   = "Docker repository for DataRift container images"

  labels = {
    project     = "datarift"
    component   = "compute"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# IAM: Compute Service Account → Writer on the Artifact Registry repository
# Allows Google Cloud Build (running under compute service account) to push images.
# ------------------------------------------------------------------------------
resource "google_artifact_registry_repository_iam_member" "docker_repo_writer" {
  project    = google_artifact_registry_repository.docker_repo.project
  location   = google_artifact_registry_repository.docker_repo.location
  repository = google_artifact_registry_repository.docker_repo.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}

