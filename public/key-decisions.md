# Data Pipeline Documentation

## Pipeline Overview

The data pipeline is designed to automate the extraction, transformation, and loading (ETL) process for raw data. It follows a sequence of steps to download a raw zip file from a given URL, save it to Google Cloud Storage (GCS), extract the contents of the zip file, save the raw CSV files to GCS, and finally, read and parse the CSV files before loading them into BigQuery.

## Choice of GCP Services

### 1. Google Cloud Storage (GCS)

- **Purpose**: GCS serves as the primary storage solution for raw data, intermediate files, and as a staging area between different stages of the pipeline.
- **Rationale**: GCS provides scalable, durable, and highly available object storage. It integrates seamlessly with other GCP services, making it an ideal choice for storing large volumes of data.

### 2. BigQuery

- **Purpose**: BigQuery is used as the data warehouse for storing and querying the processed data.
- **Rationale**: BigQuery is a fully-managed, serverless data warehouse with robust SQL capabilities. It allows for fast and cost-effective analytics on large datasets. It integrates well with GCS and supports schema-on-read, allowing for flexibility in data processing.

### 3. Dataflow

- **Purpose**: Dataflow is utilized to orchestrate and execute the ETL process in a scalable and parallelized manner.
- **Rationale**: Dataflow provides a serverless and fully-managed stream and batch processing service. It scales dynamically, ensuring efficient resource utilization, and allows for the development of flexible and portable data processing pipelines.

### 4. Artifact Registry

- **Purpose**: Artifact Registry is used to store the DataFlow Flex Container Image, enabling versioning and distribution.
- **Rationale**: Storing the DataFlow Flex Container Image in Artifact Registry facilitates version management, and it integrates seamlessly with other Google Cloud services. This ensures consistency in the runtime environment across different pipeline executions.

## Pipeline Design

### 1. Service Account

- A dedicated service account is created with the following roles:
  - `roles/storage.admin`: Full control over GCS buckets.
  - `roles/bigquery.admin`: Full control over BigQuery resources.
  - `roles/artifact-registry.admin`: Full control over Artifact Registry resources.

### 2. Terraform

- Terraform is used for infrastructure as code to provision and manage GCP resources.
- The service account is utilized during Terraform initialization and applies the plan for setting up GCS, BigQuery, and Artifact Registry resources.

### 3. Makefile

- A Makefile is provided with the following commands:
  - `template`: Creates a DataFlow Flex Template and stores metadata information in GCS.
  - `run`: Submits a DataFlow job with the required parameters.
  - `test-template`: Pulls the image from Artifact Registry, ensuring that all dependencies are accessible within the container image.

## Best Practices

### 1. Version Control

- Codebase, including Terraform scripts and DataFlow Flex Template, is maintained in a version control system (Git, GitHub) for traceability and collaboration.

### 2. Infrastructure as Code (IaC)

- Terraform is employed for infrastructure provisioning to ensure consistency, versioning, and reproducibility.

### 3. Flexibility and Scalability

- The pipeline is designed to scale horizontally, accommodating increased data volumes and processing requirements.