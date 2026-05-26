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
