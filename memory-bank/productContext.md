# Product Context: DataRift

## Why DataRift Exists

League of Legends (LoL) is one of the most competitive esports and multiplayer games globally, generating massive volumes of complex game telemetry data (match summaries, player statistics, timeline event logs, etc.). While Riot Games offers a public developer API, extracting high-volume data, handling nested structures, and performing historical trend analysis poses significant technical challenges.

DataRift is created to solve these challenges by building an automated, scalable, production-grade data pipeline and lakehouse. It transforms raw API payloads into clean, actionable, high-performance analytical datasets.

## Problems Solved

1. **Riot API Rate Limiting & Reliability**: Riot API enforces strict rate limits. DataRift uses event-driven queuing and decoupling (Pub/Sub + Cloud Run Jobs) to handle limits gracefully and guarantee delivery without losing data.
2. **Semi-Structured Data Complexity**: Match files from Riot are deeply nested JSONs. DataRift processes, validates, and flattens these files into structured tables using modern formats.
3. **Storage Costs vs. Query Performance**: Traditional architectures load all raw data into a warehouse like BigQuery, which can become prohibitively expensive. DataRift adopts a **Lakehouse architecture**:
   - **Bronze/Silver layers**: Stored cost-effectively in GCS as Parquet files with Apache Iceberg metadata.
   - **Gold layer**: Exposed or loaded into BigQuery for high-performance SQL analytics.
4. **Lack of Orchestration**: Running ad-hoc scripts is error-prone. DataRift uses Cloud Composer to automate the lifecycle of the data.

## User Experience Goals

- **For Data Analysts / Researchers**: Provide clean, fully-typed SQL tables in BigQuery containing player stats, team performance, match timelines, and champion balances.
- **For Data Engineers**: Provide a stable, self-healing pipeline with detailed observability (Cloud Logging/Log Collector) and reproducible infrastructure (Terraform).
- **For Decision Makers**: Ensure minimal operational overhead through serverless GCP tools and automated orchestration.
