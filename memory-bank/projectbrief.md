# Project Brief: DataRift

DataRift is a GCP-based serverless data pipeline designed to ingest, process, and analyze League of Legends gameplay data from the Riot API. It builds a modern lakehouse architecture on Google Cloud Storage (GCS) using Apache Iceberg metadata and exposes clean gold tables in Google BigQuery for analytics.

## Core Requirements & Objectives

- **Data Ingestion**: Programmatically retrieve League of Legends match history, player statistics, and live/historical gameplay details from the Riot API.
- **Lakehouse Storage**: Store raw and processed data in Google Cloud Storage (GCS) using Apache Iceberg format to provide ACID transactions, schema evolution, and hidden partitioning.
- **Data Warehousing & Analytics**: Expose optimized gold tables in BigQuery to facilitate performant SQL analytics, BI tool integration, and machine learning workflows.
- **Infrastructure as Code (IaC)**: Deploy and manage all GCP resources using Terraform.
- **Data Orchestration**: Schedule and orchestrate pipeline runs using Cloud Composer (managed Apache Airflow) and Cloud Scheduler.
- **Real-Time / Event-Driven messaging**: Use Cloud Pub/Sub to trigger/coordinate ingest workflows or notify downstream tasks.
- **Reliable Compute**: Execute ingestion code within containerized Cloud Run Jobs.
- **Observability**: Maintain full visibility with Cloud Logging, integrating with a Log Collector for centralized analysis.
- **Data Transformation**: Perform modular SQL transformations using dbt on top of BigQuery to clean and model silver and gold layers.

## Tech Stack

- **Language**: Python 3.14
- **Compute**: Cloud Run Job, Cloud Scheduler
- **Messaging**: Cloud Pub/Sub
- **Storage & Lakehouse**: GCS, Apache Iceberg
- **Analytics & Warehousing**: BigQuery, dbt
- **Orchestration**: Cloud Composer (Apache Airflow)
- **Infrastructure**: Terraform
- **Observability**: Cloud Logging, Log Collector
