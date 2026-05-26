# Progress: DataRift

## Current Status

The project is currently in the **Initialization & Bootstrapping** phase. No code or infrastructure has been deployed yet.

## What Works

- [x] Memory Bank initialization and architectural planning.
- [x] Architecture design and Medallion Lakehouse layout.
- [x] Multi-environment workspace structure (dev/prod) in Terraform.
- [x] BigQuery lakehouse catalog dataset (`lol_lakehouse_catalog`) defined in Terraform.
- [x] Pub/Sub ingestion trigger topic and subscription defined in Terraform.
- [x] Secret Manager for Riot API key secure storage defined in Terraform.
- [x] Comprehensive root `.gitignore` policy established.

## In Progress

- [ ] Project repository directory structure setup for Python 3.14 ingestion application under `src/ingestion/`.

## What's Left to Build

### Phase 1: Infrastructure (Terraform)
- [x] Initialize Terraform configuration in `infra/`.
- [x] Define variables, providers, and outputs.
- [x] Create GCS buckets for Bronze (raw JSON) and Silver (Iceberg) layers.
- [x] Establish multi-environment directory orchestration (`infra/env/dev` and `infra/env/prod`).
- [x] Create BigQuery dataset for Gold layer.
- [x] Create Pub/Sub topic and subscription for ingestion triggers.
- [x] Set up Secret Manager for the Riot API key.

### Phase 2: Ingestion & Lakehouse (Python 3.14)
- [ ] Write Python modules to fetch Match and Timeline data from Riot API.
- [ ] Integrate Secret Manager API key retrieval.
- [ ] Build raw JSON saver to Bronze GCS bucket.
- [ ] Implement Apache Iceberg schema definition for LoL matches.
- [ ] Develop Parquet writer and Apache Iceberg catalog synchronization for the Silver layer.
- [ ] Create Dockerfile for the Python ingestion application.
- [ ] Deploy Cloud Run Job configuration via Terraform.

### Phase 3: Orchestration (Cloud Composer / Scheduler)
- [ ] Develop Airflow DAGs to coordinate Cloud Run Job trigger and monitor runs.
- [ ] Setup Cloud Scheduler to trigger Composer DAGs or Pub/Sub topics.

### Phase 4: Transformation (dbt)
- [ ] Initialize dbt project.
- [ ] Define dbt source tables pointing to BigQuery Iceberg external tables.
- [ ] Write dbt SQL models to clean, aggregate, and materialize Gold tables.
- [ ] Configure dbt schema tests and document lineage.

### Phase 5: Monitoring & Logging
- [ ] Set up Cloud Logging filters.
- [ ] Configure Log Collector for pipeline health dashboards.

## Known Issues & Blockers

- None currently identified.
