resource "google_bigquery_dataset" "migrated_data" {
  dataset_id = var.bq_dataset
  location   = var.region
}
