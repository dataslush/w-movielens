provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "movielens-bucket" {
  name                     = "${data.google_project.project.number}-dataslush-waltlabs-movielens"
  location                 = var.region
  force_destroy            = true
  public_access_prevention = "enforced"
}

resource "google_bigquery_dataset" "movielens-dataset" {
  dataset_id = "dataslush_waltlabs_movielens"
  project    = var.project_id
  location   = var.region
}

resource "google_artifact_registry_repository" "dataflow-flex-template" {
  location      = var.region
  repository_id = "dataslush-waltlabs-movielens"
  format        = "DOCKER"
}
