resource "google_storage_bucket" "landing" {
  name          = "${var.project_id}-migration-landing"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 7 }
    action    { type = "Delete" }
  }
}
