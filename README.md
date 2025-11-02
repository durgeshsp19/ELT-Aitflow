# ELT-Aitflow

# ELT Pipeline with Apache Airflow and Google Cloud Platform

This repository contains a set of Apache Airflow DAGs that demonstrate an ELT (Extract, Load, Transform) pipeline using Google Cloud Platform services. The pipeline processes health data, creating country-specific views for analysis.

## Pipeline Overview

The pipeline consists of several DAGs that work together to:
1. Load CSV data from Google Cloud Storage to BigQuery
2. Transform the data into country-specific tables
3. Create filtered views for analysis

## Architecture




### Workflow
1. **Extract**: Check for file existence in GCS.
2. **Load**: Load raw CSV data into a BigQuery staging table.
3. **Transform**:
   - Create country-specific tables in the transform layer.
   - Generate reporting views for each country with filtered insights.

### Data Layers
1. **Staging Layer**: Raw data from the CSV file.
2. **Transform Layer**: Cleaned and transformed tables.
3. **Reporting Layer**: Views optimized for analysis and reporting.

---

## Requirements

### Tools and Services
- **Google Cloud Platform (GCP)**:
  - Google Compute Engine ( for Airflow )
  - BigQuery
  - Cloud Storage
- **Apache Airflow**:
  - Airflow with Google Cloud providers


---

## Setup Instructions

### Prerequisites
1. A Google Cloud project with:
   - BigQuery and Cloud Storage enabled.
   - Service account with required permissions.
2. Apache Airflow installed.


## End Result

### Airflow Pipeline




### Looker Studio Report



