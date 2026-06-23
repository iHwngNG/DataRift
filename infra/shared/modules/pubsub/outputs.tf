output "topic_name" {
  description = "Name of the main Pub/Sub topic"
  value       = google_pubsub_topic.main.name
}

output "topic_id" {
  description = "Full resource ID of the main Pub/Sub topic"
  value       = google_pubsub_topic.main.id
}

output "subscription_names" {
  description = "List of created subscription names"
  value       = [for sub in google_pubsub_subscription.main : sub.name]
}

output "subscription_urls" {
  description = "Map of subscription name to full subscription URL"
  value       = { for sub in google_pubsub_subscription.main : sub.name => sub.id }
}

output "dlq_topic_name" {
  description = "Name of the dead-letter topic (null if DLQ disabled)"
  value       = try(google_pubsub_topic.dlq[0].name, null)
}

output "dlq_subscription_name" {
  description = "Name of the DLQ subscription (null if DLQ disabled)"
  value       = try(google_pubsub_subscription.dlq[0].name, null)
}
