resource "google_composer_environment" "migration_composer" {
  name   = "migration-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-2-airflow-2"
    }
  }
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "asia-south1"
}

variable "bq_dataset" {
  type    = string
  default = "migrated_data"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}
