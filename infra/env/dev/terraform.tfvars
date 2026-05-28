# ==============================================================================
# Development Environment - Variable Definitions
# ==============================================================================

project_id = "datarift-dev"
region     = "asia-southeast3"

fetch_users_jobs = {
  vn2 = {
    platform  = "vn2"
    region    = "asia"
    tiers     = "DIAMOND"
    divisions = "I,II,III,IV"
    max_pages = "2"
  }
  kr = {
    platform  = "kr"
    region    = "asia"
    tiers     = "DIAMOND"
    divisions = "I,II,III,IV"
    max_pages = "2"
  }
}

parquet_writer_jobs = {
  shared = {
    platform        = "multi"
    subscription_id = "lol-league-entries-subscription-dev"
    max_messages    = "500"
    max_runtime_s   = "3600"
  }
}
