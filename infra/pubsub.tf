# ==============================================================================
# Google Cloud Pub/Sub Resources (DataRift Orchestration & Messaging)
# ==============================================================================

# ------------------------------------------------------------------------------
# Pub/Sub Topic: Ingestion trigger topic
# Decouples scheduling/triggers from the active compute ingestion job.
# ------------------------------------------------------------------------------
resource "google_pubsub_topic" "ingest_trigger" {
  name = "lol-ingest-trigger-${var.environment}"

  labels = {
    project     = "datarift"
    component   = "messaging"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# Pub/Sub Subscription: Ingestion trigger subscription
# ------------------------------------------------------------------------------
resource "google_pubsub_subscription" "ingest_subscription" {
  name                 = "lol-ingest-subscription-${var.environment}"
  topic                = google_pubsub_topic.ingest_trigger.name
  ack_deadline_seconds = 60

  # Retain unacknowledged messages for 7 days
  message_retention_duration = "604800s"

  # Retain acknowledged messages? No (saves storage)
  retain_acked_messages = false

  # Expiration policy: Never expire due to inactivity
  expiration_policy {
    ttl = ""
  }

  # Retry policy: Retry with exponential backoff
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = {
    project     = "datarift"
    component   = "messaging"
    environment = var.environment
  }
}

# ==============================================================================
# Pub/Sub Topic: League Entries data pipeline
# Carries raw Riot league-entry records from fetch_users → parquet_writer.
# ==============================================================================
resource "google_pubsub_topic" "league_entries" {
  name = "lol-league-entries-${var.environment}"

  # Messages older than 7 days are discarded
  message_retention_duration = "604800s"

  labels = {
    project     = "datarift"
    component   = "messaging"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# Pub/Sub Subscription: parquet_writer consumer
# ------------------------------------------------------------------------------
resource "google_pubsub_subscription" "league_entries_subscription" {
  name                 = "lol-league-entries-subscription-${var.environment}"
  topic                = google_pubsub_topic.league_entries.name
  ack_deadline_seconds = 300 # 5 min — gives the writer time to flush a batch

  # Retain unacknowledged messages for 7 days
  message_retention_duration = "604800s"

  retain_acked_messages = false

  expiration_policy {
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  labels = {
    project     = "datarift"
    component   = "messaging"
    environment = var.environment
  }
}

# ------------------------------------------------------------------------------
# IAM: Compute Service Account → Publisher on league_entries topic
# Allows fetch_users Cloud Run Jobs to publish messages.
# ------------------------------------------------------------------------------
data "google_project" "current" {}

resource "google_pubsub_topic_iam_member" "league_entries_publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.league_entries.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}

# ------------------------------------------------------------------------------
# IAM: Compute Service Account → Subscriber on league_entries subscription
# Allows parquet_writer Cloud Run Job to pull and ack messages.
# ------------------------------------------------------------------------------
resource "google_pubsub_subscription_iam_member" "league_entries_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.league_entries_subscription.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}
