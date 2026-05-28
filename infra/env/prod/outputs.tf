output "bronze_bucket_name" {
  description = "The name of the Bronze GCS bucket in Prod."
  value       = module.gcs_base.bronze_bucket_name
}

output "bronze_bucket_url" {
  description = "The URL of the Bronze GCS bucket in Prod."
  value       = module.gcs_base.bronze_bucket_url
}

output "silver_bucket_name" {
  description = "The name of the Silver GCS bucket in Prod."
  value       = module.gcs_base.silver_bucket_name
}

output "silver_bucket_url" {
  description = "The URL of the Silver GCS bucket in Prod."
  value       = module.gcs_base.silver_bucket_url
}

output "bigquery_dataset_id" {
  description = "The ID of the BigQuery lakehouse catalog dataset in Prod."
  value       = module.gcs_base.bigquery_dataset_id
}

output "pubsub_topic_name" {
  description = "The name of the Pub/Sub ingestion trigger topic in Prod."
  value       = module.gcs_base.pubsub_topic_name
}

output "pubsub_subscription_name" {
  description = "The name of the Pub/Sub ingestion subscription in Prod."
  value       = module.gcs_base.pubsub_subscription_name
}

output "secret_id" {
  description = "The ID of the Secret Manager secret for the Riot API key in Prod."
  value       = module.gcs_base.secret_id
}


