variable "credentials_file" {
  description = "Path to the Google Cloud credentials file"
  default     = "../service-account/dataslush-waltlabs-movielens.json"
}

variable "project_id" {
  description = "Google Cloud Project ID"
}

variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
}

data "google_project" "project" {
}