locals {
  job_short_name = split("/", google_cloud_run_v2_job.this.name)[length(split("/", google_cloud_run_v2_job.this.name)) - 1]
}
