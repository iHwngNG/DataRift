# ==============================================================================
# Google BigQuery Datasets (DataRift Lakehouse Layers)
# ==============================================================================

# ------------------------------------------------------------------------------
# BigQuery Dataset: lol_lakehouse_catalog
#
# NOTE: GCP BigQuery dataset IDs only allow alphanumeric characters and underscores.
# Hyphens are not permitted. Therefore, we use "lol_lakehouse_catalog" as the ID
# and assign the user-requested "lol-lakehouse-catalog" to the friendly name.
# ------------------------------------------------------------------------------
resource "google_bigquery_dataset" "lakehouse_catalog" {
  dataset_id                  = "lol_lakehouse_catalog"
  friendly_name               = "lol-lakehouse-catalog"
  description                 = "Empty BigQuery dataset serving as the metadata catalog and gold-layer presentation layer for League of Legends data."
  location                    = var.region
  default_table_expiration_ms = null # Tables in the lakehouse catalog are persistent

  labels = {
    project     = "datarift"
    layer       = "catalog"
    environment = var.environment
  }
}
