# Pub/Sub Module
# Creates a topic with push subscription(s) and dead-letter queue

resource "google_pubsub_topic" "main" {
  name                       = var.topic_name
  message_retention_duration = var.message_retention_duration
  labels                     = var.labels
}

resource "google_pubsub_topic_iam_member" "publisher" {
  topic  = google_pubsub_topic.main.name
  role   = "roles/pubsub.publisher"
  member = var.publisher_members
}

# Dead Letter Topic
resource "google_pubsub_topic" "dlq" {
  count                      = var.dlq_topic_name != null ? 1 : 0
  name                       = var.dlq_topic_name
  message_retention_duration = var.message_retention_duration
  labels                     = merge(var.labels, { purpose = "dead-letter-queue" })
}

# Subscriptions
resource "google_pubsub_subscription" "main" {
  for_each = { for sub in var.subscriptions : sub.name => sub }

  name                       = each.value.name
  topic                      = google_pubsub_topic.main.name
  ack_deadline_seconds       = each.value.ack_deadline_seconds != null ? each.value.ack_deadline_seconds : var.default_ack_deadline_seconds
  message_retention_duration = var.message_retention_duration
  retain_acked_messages      = var.retain_acked_messages
  labels                     = var.labels

  dead_letter_policy {
    dead_letter_topic     = var.dlq_topic_name != null ? google_pubsub_topic.dlq[0].id : null
    max_delivery_attempts = var.max_delivery_attempts
  }

  retry_policy {
    minimum_backoff = var.min_backoff
    maximum_backoff = var.max_backoff
  }

  dynamic "push_config" {
    for_each = each.value.push_endpoint != null && var.push_service_account != null ? [1] : []
    content {
      push_endpoint = each.value.push_endpoint
      oidc_token {
        service_account_email = var.push_service_account
      }
    }
  }

  dynamic "bigquery_config" {
    for_each = each.value.bigquery_table != null ? [1] : []
    content {
      table = each.value.bigquery_table
    }
  }

  expiration_policy {
    ttl = var.subscription_expiration_policy
  }
}

# Note: Subscription IAM is handled in the calling module (dev/main.tf)
# to avoid known-after-apply issues with service_account_email

# DLQ Subscription for monitoring/retries
resource "google_pubsub_subscription" "dlq" {
  count                       = var.dlq_topic_name != null && var.enable_dlq_subscription ? 1 : 0
  name                        = "${var.dlq_topic_name}-subscription"
  topic                       = google_pubsub_topic.dlq[0].name
  ack_deadline_seconds        = 600
  message_retention_duration  = var.message_retention_duration

  retry_policy {
    minimum_backoff = "300s"
    maximum_backoff  = "600s"
  }

  expiration_policy {
    ttl = "604800s" # 7 days
  }
}
