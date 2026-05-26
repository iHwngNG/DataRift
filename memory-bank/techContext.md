# Technical Context: DataRift

## Technology Stack

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Language** | Python 3.14 | Ingestion scripting, API communication, Iceberg writer |
| **Compute** | Cloud Run Job | Containerized python compute executed on-demand |
| **Scheduling** | Cloud Scheduler | Time-based CRON triggers |
| **Messaging** | Cloud Pub/Sub | Decoupling ingestion and event coordination |
| **Storage (Data Lake)** | Google Cloud Storage (GCS) | Bronze raw data bucket & Silver Iceberg lakehouse bucket |
| **Lakehouse Metadata**| Apache Iceberg | Table metadata format on GCS for ACID, schema, partitioning |
| **Warehouse** | Google BigQuery | SQL Analytics warehouse (exposing Gold tables) |
| **Transformation** | dbt (Data Build Tool) | SQL modeling, testing, and lineage on BigQuery |
| **Orchestration** | Cloud Composer | Managed Apache Airflow workflow orchestration |
| **Infrastructure** | Terraform | Infrastructure as Code (IaC) |
| **Observability** | Cloud Logging | Application and infrastructure logging |
| **Log Management** | Log Collector | Collecting and routing logs to analytics |

## Development Setup & Prerequisites

To develop, maintain, or deploy the DataRift pipeline, developers require:

1. **GCP Account & CLI**:
   - Install the [Google Cloud SDK (gcloud CLI)](https://cloud.google.com/sdk/docs/install).
   - Authenticated with appropriate IAM permissions (Editor/Owner for sandbox development).
2. **Python 3.14**:
   - Local python 3.14 installation with `venv` or `poetry` for dependency management.
   - Core libraries: `requests` (API clients), `pyiceberg` (Apache Iceberg Python SDK), `pandas`/`pyarrow` (data manipulation and Parquet creation).
3. **Terraform**:
   - Install Terraform CLI (version 1.5+ recommended).
   - GCS backend configured for state tracking.
4. **dbt CLI**:
   - `dbt-core` and `dbt-bigquery` adapter installed.
5. **Riot Developer API Key**:
   - Developer or production API key from [Riot Developer Portal](https://developer.riotgames.com/).
   - Configure key via GCP Secret Manager for production, or local `.env` for local testing.

## Technical Constraints & Considerations

- **Riot API Rate Limits**:
  - Development keys: 20 requests per 1 second, 100 requests per 2 minutes.
  - Production keys: Significantly higher, but still capped. Ingestion code must respect HTTP 429 responses and implement exponential backoff.
- **Python 3.14 Compatibility**:
  - As Python 3.14 is a modern release, verify library support for packages like PyIceberg, PyArrow, and dbt. Use latest stable versions that support Python 3.14.
- **GCS to BigQuery Integration**:
  - BigQuery must be configured with a BigQuery Omni or external table definition to query Iceberg tables stored in GCS.
- **Cloud Run Job Timeout**:
  - Cloud Run Jobs support runs up to 24 hours. The batch size for ingestion should be designed to finish comfortably within this window.
