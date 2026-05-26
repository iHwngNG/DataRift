output "bronze_bucket_name" {
  description = "The name of the Bronze GCS bucket."
  value       = google_storage_bucket.bronze_raw.name
}

output "bronze_bucket_url" {
  description = "The URL of the Bronze GCS bucket."
  value       = google_storage_bucket.bronze_raw.url
}

output "silver_bucket_name" {
  description = "The name of the Silver GCS bucket."
  value       = google_storage_bucket.silver_lakehouse.name
}

output "silver_bucket_url" {
  description = "The URL of the Silver GCS bucket."
  value       = google_storage_bucket.silver_lakehouse.url
}

output "bigquery_dataset_id" {
  description = "The ID of the BigQuery lakehouse catalog dataset."
  value       = google_bigquery_dataset.lakehouse_catalog.dataset_id
}

output "pubsub_topic_name" {
  description = "The name of the Pub/Sub ingestion trigger topic."
  value       = google_pubsub_topic.ingest_trigger.name
}

output "pubsub_subscription_name" {
  description = "The name of the Pub/Sub ingestion subscription."
  value       = google_pubsub_subscription.ingest_subscription.name
}

output "secret_id" {
  description = "The ID of the Secret Manager secret for the Riot API key."
  value       = google_secret_manager_secret.riot_api_key.secret_id
}


