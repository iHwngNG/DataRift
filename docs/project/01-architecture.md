# 01 — Architecture

## 1.1 High-Level Diagram

```
                         ┌──────────────────────┐
                         │   Cloud Scheduler     │
                         │  (independent crons)  │
                         └──────────┬────────────┘
                                     │
        ┌────────────────┬──────────┴───────────┬──────────────────┐
        ▼                 ▼                      ▼                  ▼
   ┌─────────┐      ┌───────────────┐     ┌───────────────┐  ┌────────────┐
   │  Job A  │      │ Job B-Dist     │     │ Job C-Dist     │  │   Job D     │
   │  User   │      │ (shard puuid)  │     │ (shard matchID)│  │ Iceberg Sync│
   └────┬────┘      └───────┬────────┘     └───────┬────────┘  └──────┬─────┘
        │                   │ Pub/Sub fan-out       │ Pub/Sub fan-out  │
        │                   ▼                       ▼                  │
        │            ┌─────────────┐         ┌─────────────┐          │
        │            │ Job B[0..3]  │         │ Job C[0..3]  │          │
        │            │ (workers)    │         │ (workers)    │          │
        │            └──────┬───────┘         └──────┬───────┘          │
        │                   │                        │                  │
        ▼                   ▼                        ▼                  ▼
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                  GCS Bucket: datarift-lakehouse                          │
  │  ./league/...     ./matchID/...     ./match/...     ./workspace/...      │
  └─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │  Iceberg Metadata      │
                         │  (GCS) + Catalog (BQ)  │
                         └───────────────────────┘
```

## 1.2 Job Topology

| Job | Type | Trigger | Shards | Output |
|-----|------|---------|--------|--------|
| **Job A** | Single Cloud Run Job | Cloud Scheduler (independent cron) | 1 (loops all platforms internally) | `./league/...` (Parquet) |
| **Job B-Distributor** | Single Cloud Run Job | Cloud Scheduler | 1 | `./workspace/puuid/{id}/*.parquet` (4 shards) |
| **Job B Worker** | Cloud Run Job, parameterized by `SHARD_ID` | Pub/Sub (fan-out from B-Dist) | 4 (id=0..3, 1 per platform) | `./matchID/...` (Parquet) + update `workspace/puuid/{id}/*` |
| **Job C-Distributor** | Single Cloud Run Job | Cloud Scheduler | 1 | `./workspace/matchid/{id}/*.parquet` (4 shards) |
| **Job C Worker** | Cloud Run Job, parameterized by `SHARD_ID` | Pub/Sub (fan-out from C-Dist) | 4 (id=0..3, 1 per platform) | `./match/...` (Parquet) + update `matchID/` is_ingested |
| **Job D** | Single Cloud Run Job | Cloud Scheduler (daily) | 1 | Iceberg metadata (GCS) + catalog (BigQuery) |

## 1.3 Independence Principles

- **Jobs A, B-Dist, C-Dist, D đều trigger độc lập** theo cron riêng — không có hard dependency giữa chúng ở schedule level.
- **Job B Worker / Job C Worker** chỉ được trigger bởi Distributor tương ứng qua Pub/Sub — không tự chạy theo cron.
- Mỗi job (A, B-Dist, B-Worker, C-Dist, C-Worker, D) là **1 entrypoint riêng, độc lập về deployment** (own Dockerfile), nhưng **share toàn bộ business logic qua `src/`**.

## 1.4 Sharding Strategy (Job B & Job C)

Cả hai pipeline (B và C) dùng chung pattern:

1. **Distributor** đọc toàn bộ dữ liệu nguồn (Parquet files), convert key (`puuid` hoặc `match_id`) → integer qua module `src/hashing/string_to_int.py`
2. Apply hash function (`src/hashing/shard.py`) → `shard_id = hash(key_int) % N`, với `N = 4` (số platform: vn2, kr, euw1, eun1)
3. Ghi mỗi shard ra `workspace/{puuid|matchid}/{shard_id}/*.parquet`
4. Publish Pub/Sub message để trigger Worker tương ứng với `SHARD_ID` env var
5. Worker đọc đúng shard folder của mình, xử lý bằng `queue.Queue` + thread pool

> **Lưu ý:** `shard_id` không nhất thiết = platform cụ thể; nó chỉ là 1 trong 4 buckets được hash đều. Mapping shard_id ↔ platform là tự nhiên vì N=4=số platform, nhưng logic phân phối không phụ thuộc cứng vào tên platform.
