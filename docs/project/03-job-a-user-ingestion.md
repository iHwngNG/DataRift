# 03 — Job A: User (League) Ingestion

## 3.1 Purpose

Fetch ranked ladder data (summoner entries grouped by tier & division) cho mỗi platform, ghi xuống `./league/` và extract PUUIDs cho downstream jobs.

## 3.2 Trigger

Cloud Scheduler — independent daily cron (e.g. `0 6 * * *` UTC).

## 3.3 Input

- `conf/` — list of `platform` values (`vn2`, `kr`, `euw1`, `eun1`), `tier` values, `division` values
- Riot API credentials (env var)

## 3.4 Process

1. Loop qua từng `platform` trong config.
2. Với mỗi `platform`, loop qua từng `tier` × `division` combination (League-Entries-V4 endpoint hỗ trợ query theo tier/division cho các tier non-apex; Challenger/GrandMaster/Master dùng endpoint riêng — xử lý 2 nhánh logic này trong `src/riot_client/`).
3. Với mỗi response page, transform records → schema `league/` (xem `02-gcs-layout.md`).
4. Buffer records vào PyArrow Table (in-memory) qua `src/parquet/buffer.py`.
5. Khi buffer đạt **4MB**, flush → write Parquet file to `./league/{region}/{platform}/{tier}/{division}/`.
6. Cuối job: flush buffer còn lại (nếu có).

## 3.5 Output

- `./league/{region}/{platform}/{tier}/{division}/*.parquet`

## 3.6 Downstream Note

Job A **không** trực tiếp ghi vào `workspace/puuid/`. Việc extract PUUID và shard hóa là trách nhiệm của **Job B-Distributor** (đọc trực tiếp từ `./league/`). Điều này giữ Job A đơn giản, single-responsibility: chỉ fetch & write league data.

## 3.7 Concurrency

Job A chạy single-threaded hoặc với concurrency thấp (2-4 concurrent requests) vì League-Entries-V4 endpoint số lượng calls nhỏ (1 call per tier/division/platform combination, ~28 combinations × 4 platforms = 112 calls/run). Không cần threading queue phức tạp.

## 3.8 Config Keys (in `conf/`)

```yaml
job_a:
  platforms: [vn2, kr, euw1, eun1]
  tiers: [iron, bronze, silver, gold, platinum, emerald, diamond]
  divisions: [I, II, III, IV]
  buffer_flush_mb: 4
```
