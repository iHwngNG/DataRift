# System Patterns: DataRift

## Architecture Overview

DataRift uses a serverless and managed Medallion Lakehouse architecture on GCP, orchestrated by Cloud Composer and provisioned by Terraform.

```mermaid
flowchart TD
    subgraph Ingestion_Layer [Ingestion Layer]
        CS[Cloud Scheduler] -->|Periodic Trigger| CRJ[Cloud Run Job]
        RiotAPI[(Riot Games API)] <-->|Fetch JSON Data| CRJ
    end

    subgraph Messaging_Layer [Messaging & Orchestration]
        Composer[Cloud Composer / Airflow] -->|Orchestrates Pipeline| CRJ
        Composer -->|Orchestrates dbt| DBT[dbt on BigQuery]
        CRJ -->|Publish events| PubSub[Cloud Pub/Sub]
    end

    subgraph Lakehouse_Storage [Lakehouse GCS]
        CRJ -->|Write Raw JSON| Bronze[Bronze: Raw GCS bucket]
        CRJ -->|Write Parquet & Iceberg Metadata| Silver[Silver: Apache Iceberg GCS bucket]
    end

    subgraph Warehouse_Layer [Warehouse BigQuery]
        DBT -->|Transform Iceberg Data| Gold[Gold: BigQuery Clean Tables]
        Silver -->|BigQuery Iceberg External Tables| BQ_Ext[BigQuery Lakehouse Layer]
        BQ_Ext --> Gold
    end

    subgraph Observability [Observability]
        CRJ -->|Logs| Logging[Cloud Logging]
        Logging --> LogColl[Log Collector]
    end
```

## Key Technical Decisions

1. **Medallion Lakehouse Architecture**:
   - **Bronze (Raw)**: Unmodified JSON payloads from Riot API stored in GCS. Helps in re-processing if needed.
   - **Silver (Structured)**: Parquet format with Apache Iceberg metadata stored in GCS. Exposes tables with schema evolution and partition pruning.
   - **Gold (Aggregated / Cleaned)**: Analytical tables stored directly in BigQuery, optimized for BI queries.
2. **Serverless Compute (Cloud Run Jobs)**:
   - Python 3.14 ingestion containers are executed on-demand. Eliminates the cost of idle VMs.
   - Cloud Run Jobs are ideal for batch ingestion runs that exceed Cloud Run Services' timeouts.
3. **Apache Iceberg on GCS**:
   - Implements transactional metadata on GCS object storage. Enables schema evolution, time-travel, and ACID transactions directly on GCS files, minimizing data warehouse storage costs.
4. **dbt for In-Warehouse Transformations**:
   - Transforms Silver Iceberg external tables in BigQuery into analytical Gold models using SQL, tracking lineage, and performing tests.
5. **Infrastructure as Code (Terraform)**:
   - Every resource (GCS buckets, BigQuery datasets, Cloud Run Jobs, Pub/Sub topics, Composer environments, IAM bindings) is defined declaratively using Terraform modules.

## Component Relationships

- **Cloud Composer** is the central orchestrator. It triggers the Cloud Run Job for data ingestion, monitors it, and then triggers the `dbt` run to perform warehouse transformations.
- **Cloud Scheduler** is used for light, time-based triggers that can kick off ingestion directly or publish to **Pub/Sub** topics.
- **Cloud Run Job** fetches data from Riot API, writes raw files to GCS, and converts them to Apache Iceberg format.
- **Log Collector** aggregates logs from Cloud Logging for ingestion runs, API responses, and errors, ensuring that rate limits or data anomalies are flagged immediately.
