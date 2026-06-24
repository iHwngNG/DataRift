# =============================================================================
# Cloud Run Job Module Outputs
# =============================================================================

output "job_a_user_ingestion_name" {
  description = "Name of Job A user ingestion"
  value       = module.job_a_user_ingestion.job_name
}

output "job_a_user_ingestion_id" {
  description = "Full resource ID of Job A user ingestion"
  value       = module.job_a_user_ingestion.job_id
}

output "job_b_distributor_name" {
  description = "Name of Job B distributor"
  value       = module.job_b_distributor.job_name
}

output "job_b_distributor_id" {
  description = "Full resource ID of Job B distributor"
  value       = module.job_b_distributor.job_id
}

output "job_b_worker_names" {
  description = "Names of all Job B workers"
  value       = [for w in module.job_b_worker : w.job_name]
}

output "job_b_worker_ids" {
  description = "Full resource IDs of all Job B workers"
  value       = [for w in module.job_b_worker : w.job_id]
}

output "job_c_distributor_name" {
  description = "Name of Job C distributor"
  value       = module.job_c_distributor.job_name
}

output "job_c_distributor_id" {
  description = "Full resource ID of Job C distributor"
  value       = module.job_c_distributor.job_id
}

output "job_c_worker_names" {
  description = "Names of all Job C workers"
  value       = [for w in module.job_c_worker : w.job_name]
}

output "job_c_worker_ids" {
  description = "Full resource IDs of all Job C workers"
  value       = [for w in module.job_c_worker : w.job_id]
}
