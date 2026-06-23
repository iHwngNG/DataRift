# DataRift — Project SPEC

**Version:** 2.1
**Last Updated:** 2026-06-17
**Status:** Draft

## 1. Project Summary

DataRift là một GCP-native Data Lakehouse pipeline thu thập dữ liệu League of Legends (ranked ladder, match IDs, match details) từ Riot Games API. Pipeline ingest data theo từng layer riêng biệt vào GCS dưới dạng Parquet, sử dụng Apache Iceberg (catalog backend: BigQuery) để quản lý metadata cho toàn bộ lakehouse.

## 2. Goals

- Ingest ranked ladder data (user/summoner theo tier & division) cho 4 platform: `vn2`, `kr`, `euw1`, `eun1`
- Ingest match ID list cho mỗi summoner (PUUID-based), tracking incremental qua `last_read`
- Ingest match detail data dựa trên match ID chưa được fetch (`is_ingested` flag)
- Sync Iceberg metadata + catalog (BigQuery) hàng ngày từ toàn bộ GCS lakehouse
- Toàn bộ pipeline chạy bằng Cloud Run Jobs, trigger độc lập theo schedule

## 3. Out of Scope

- Live/in-game data
- Champion mastery
- Tournament/Esports data
- ML serving, dashboards (BI layer sẽ được spec ở phase sau)

## 4. Spec File Index

| File | Section |
|------|---------|
| `00-overview.md` | Project summary, goals |
| `01-architecture.md` | High-level architecture, job topology |
| `02-gcs-layout.md` | GCS bucket structure, file naming, schema per layer |
| `03-job-a-user-ingestion.md` | Job A spec |
| `04-job-b-matchid-ingestion.md` | Job B Distributor + Worker spec |
| `05-job-c-match-ingestion.md` | Job C Distributor + Worker spec |
| `06-job-d-iceberg-sync.md` | Job D (Iceberg metadata sync) spec |
| `07-project-structure.md` | Repo layout: src/, jobs/, infra/, conf/ |
| `08-shared-modules.md` | Shared `src/` module specs (hashing, buffering, riot client, etc.) |
| `09-sprints.md` | Sprint breakdown with checkboxes |

## 5. Core Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.14 |
| Compute | Cloud Run Jobs |
| Storage | Google Cloud Storage (`datarift-lakehouse`) |
| File Format | Apache Parquet (via PyArrow) |
| Table Format | Apache Iceberg (pyiceberg) |
| Iceberg Catalog | BigQuery (Iceberg REST Catalog) |
| Messaging / Orchestration | Google Pub/Sub |
| Delayed Job Scheduling | Google Cloud Tasks |
| Scheduling | Cloud Scheduler |
| IaC | Terraform |
| Config management | `conf/` directory (YAML) |
| Project config | `pyproject.toml` |

## 6. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Sharding via hash(puuid/match_id) mod N | Phân phối workload cho Job B/C đồng đều giữa N=4 platform workers, không cần external coordination service |
| State tracking via `last_read` (Job B) / `is_ingested` (Job C) columns trong Parquet, không dùng Firestore | Zero extra infra, consistent với Parquet-everywhere approach, cost $0 |
| Incremental fetch = `startTime` param + early-stop dedup + cap 1000 matches/run | Tối ưu cả số request (narrow time range) và an toàn (hard cap tránh runaway jobs) |
| Iceberg metadata sync tách thành Job D riêng, chạy daily | Decouple ingestion jobs khỏi catalog management, tránh write contention lên Iceberg metadata khi nhiều job ingest chạy song song |
| Rate-limit checkpoint: khi tất cả threads bị 429, flush RAM → `workspace/tmp/`, ghi state → `state/checkpoint/`, terminate job hiện tại, schedule lại job mới sau `Retry-After + 2s` | Không waste Cloud Run billing time khi ngồi chờ rate limit; job mới resume đúng chỗ đã checkpoint; tách biệt temp data (`workspace/tmp/`) khỏi checkpoint state (`state/checkpoint/`) để rõ mục đích mỗi loại file |
