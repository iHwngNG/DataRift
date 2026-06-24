# =============================================================================
# Single Job Outputs
# =============================================================================

output "job_name" {
  description = "Name of the Cloud Run Job"
  value       = google_cloud_run_v2_job.this.name
}

output "job_id" {
  description = "Full resource ID of the Cloud Run Job"
  value       = google_cloud_run_v2_job.this.id
}

output "job_uid" {
  description = "UID of the Cloud Run Job"
  value       = google_cloud_run_v2_job.this.uid
}

output "service_account_email" {
  description = "Service account email used by the job"
  value       = var.service_account
}

output "location" {
  description = "Location/region of the job"
  value       = google_cloud_run_v2_job.this.location
}
