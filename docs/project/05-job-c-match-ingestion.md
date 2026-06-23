# 05 — Job C: Match Data Ingestion

Job C gồm 2 thành phần: **Job C-Distributor** (single job, sharding) và **Job C Worker** (4 instances), workflow đối xứng với Job B nhưng khác biệt ở buffer partitioning và cách update `is_ingested` ngược về source files.

---

## 5.1 Job C-Distributor

### Purpose

Đọc toàn bộ match IDs từ `./matchID/`, shard hóa theo hash của `match_id`, ghi shard files vào `workspace/matchid/{shard_id}/`, trigger 4 Job C Workers.

### Trigger

Cloud Scheduler — independent cron, lệch giờ với Job B-Distributor (e.g. `0 10 * * *` UTC) để tránh chồng load API.

### Input

| Source | Nội dung |
|--------|----------|
| `./matchID/**/*.parquet` | Toàn bộ match ID data từ Job B |
| `conf/job_c.yaml` | `shard_count` |

### Detailed Workflow

**Bước 1 — Load match ID data per file:**
1. List tất cả Parquet files dưới `./matchID/**`.
2. Đọc **từng file riêng biệt** (không concat ngay) để giữ context về `puuid` và `source_file_path` — cần thiết cho bước update ngược lại sau này.
3. Với mỗi file: extract records `{match_id, is_ingested, region, platform, puuid, source_file_path}`.
   - `source_file_path` = GCS path của file đang đọc (để Job C Worker biết cần rewrite file nào khi update `is_ingested`).

**Bước 2 — Shard hóa:**
1. Với mỗi record: gọi `string_to_int(match_id)` → `match_id_int`.
2. Gọi `assign_shard(match_id_int, shard_count=4)` → `shard_id`.
3. Group records theo `shard_id`.

**Bước 3 — Ghi shard files:**
- Với mỗi shard group: overwrite `workspace/matchid/{shard_id}/data.parquet`.
- Schema shard file: `{match_id, is_ingested, region, platform, puuid, source_file_path}`.
- Ghi **toàn bộ** records (cả `is_ingested=0` lẫn `is_ingested=1`) vào shard file — Worker tự filter. Điều này đảm bảo distribute tiếp theo không cần re-merge state.

**Bước 4 — Trigger workers:**
- Publish 4 Pub/Sub messages tới topic Job C Worker trigger.
- Payload: `{"shard_id": <int>}`.

### Output

- `workspace/matchid/{0..3}/data.parquet` (overwritten)

---

## 5.2 Job C Worker

### Purpose

Với shard được assign, fetch match detail data từ Riot Match-V5 API cho các `match_id` có `is_ingested=0`, buffer kết quả in-memory **theo partition** `(region, platform, year, month, date)`, flush xuống `./match/` khi buffer partition đạt ngưỡng 32MB, rồi update `is_ingested=1` cả trong shard file lẫn trong source `./matchID/` files.

### Trigger

- **Normal run:** Pub/Sub message từ Job C-Distributor, payload: `{"shard_id": <int>}`.
- **Resume run:** Cloud Tasks với `RESUME_MODE=true`, `PARENT_JOB_ID=<id>`, `SHARD_ID=<int>`.

### Input

| Source | Nội dung |
|--------|----------|
| `workspace/matchid/{shard_id}/data.parquet` | Shard file từ Distributor |
| Env var `SHARD_ID` | Shard index |
| Env var `JOB_ID` | Execution ID |
| Env var `RESUME_MODE` | `true` nếu resume |
| Env var `PARENT_JOB_ID` | Job ID trước (khi resume) |

### Detailed Workflow

**Bước 1 — Khởi tạo:**
1. Đọc env vars: `SHARD_ID`, `JOB_ID`, `RESUME_MODE`, `PARENT_JOB_ID`.
2. Load `workspace/matchid/{shard_id}/data.parquet` vào memory.
3. Filter records: chỉ giữ `is_ingested == 0` → đây là `work_items`.
4. Build in-memory mapping: `match_id → source_file_path` (dùng để update `is_ingested` cuối job).

**Bước 2 — Resume mode (nếu `RESUME_MODE=true`):**
1. Với mỗi `thread_id`:
   - Gọi `checkpoint.exists(job_type="c", job_id=PARENT_JOB_ID, thread_id)`.
   - Nếu tồn tại: load `CheckpointData` → lấy `remaining_items` (list match_ids chưa fetch của thread đó).
   - Restore in-memory buffer: `tmp_buffer.restore(checkpoint.tmp_parquet_path)` → inject PyArrow Table vào **đúng partition buffer** tương ứng (xác định partition từ records trong tmp table).
   - Xóa tmp file sau restore.
2. Replace `work_items` bằng union `remaining_items` từ tất cả thread checkpoints.

**Bước 3 — Phân phối và khởi động thread pool:**
1. Push `work_items` vào `queue.Queue`.
2. Truyền `JobContext(job_id=JOB_ID, job_type="c", shard_id=SHARD_ID)` vào `threaded_queue.run_threaded()`.
3. Mỗi thread có **tập partition buffers riêng** — `dict[partition_key → ParquetBuffer]`, trong đó `partition_key = (region, platform, year, month, date)`. Mỗi buffer trong tập có threshold 32MB độc lập.

**Bước 4 — Xử lý từng match_id (per thread):**

Thread lấy 1 record `{match_id, region, platform, puuid, source_file_path}` từ queue và thực hiện:

**4a. Fetch match detail:**
- Xác định `cluster` từ `platform` qua `regions.py` (vd: `kr → asia`).
- Gọi `GET /lol/match/v5/matches/{match_id}` trên cluster endpoint.
- Nếu nhận 429 → raise `RateLimitError(retry_after_seconds)` → propagate lên `threaded_queue`.
- Nếu nhận 404 (match không tồn tại) → log warning, mark `is_ingested = 1` (skip permanently), tiếp tục.

**4b. Parse response:**
- Extract `gameStartTimestamp` (unix ms) từ `info.gameStartTimestamp`.
- Gọi `match_fetcher.derive_partition_date(gameStartTimestamp)` → `(year, month, date)`.
- Xác định `partition_key = (region, platform, year, month, date)`.
- Transform toàn bộ response → flat record theo schema `match/` (xem `02-gcs-layout.md`).

**4c. Buffer kết quả — partitioned buffering:**
- Lookup `partition_key` trong `partition_buffers` của thread:
  - Nếu chưa có: tạo `ParquetBuffer` mới cho partition này, add vào dict.
  - Nếu đã có: dùng buffer hiện có.
- Append record vào `partition_buffers[partition_key]`.

**4d. Flush conditions — mỗi partition buffer flush độc lập:**

| Điều kiện | Hành động | Ghi vào |
|-----------|-----------|---------|
| Partition buffer ≥ 32MB | Flush partition đó + reset, tiếp tục thread | `./match/{region}/{platform}/{year}/{month}/{date}/` |
| Hết tất cả match_ids trong queue (job hoàn tất) | Flush tất cả partition buffers còn lại của tất cả threads | `./match/{region}/{platform}/{year}/{month}/{date}/` |
| Tất cả threads bị 429 | Emergency flush tất cả partition buffers → 1 tmp file per thread, save checkpoint, terminate | `workspace/tmp/c/...` |

- **GCS write path khi flush bình thường:** `./match/{region}/{platform}/{year}/{month}/{date}/{job_id}_{thread_id}_{timestamp}.parquet`
- **Emergency flush (rate-limit):** toàn bộ nội dung partition buffers của thread được gộp vào 1 PyArrow Table rồi ghi vào `workspace/tmp/c/{JOB_ID}_{thread_id}.parquet` — giữ cột `partition_key` để khi restore biết cần inject vào partition buffer nào.

**4e. Update `is_ingested` in-memory:**
- Sau khi fetch + buffer xong 1 match_id: mark `is_ingested = 1` cho match_id đó trong in-memory shard table.
- Không ghi GCS ngay — gom toàn bộ updates, ghi 1 lần cuối job.

**Bước 5 — Rate-limit handling:**

Khi `threaded_queue` detect tất cả threads bị `RateLimitError`:
1. Drain queue → `remaining_match_ids`.
2. Per-thread:
   - Gộp tất cả partition buffers của thread thành 1 PyArrow Table (giữ cột `_partition_key` để restore đúng partition sau).
   - `tmp_buffer.flush(merged_table, job_type="c", job_id=JOB_ID, thread_id)` → `workspace/tmp/c/{JOB_ID}_{thread_id}.parquet`.
   - `checkpoint.save(CheckpointData(remaining_items=remaining_match_ids_of_thread, tmp_parquet_path=..., retry_after_seconds=N, shard_id=SHARD_ID, ...))`.
3. `scheduler.schedule_retry(job_name="job-c-worker", delay_seconds=retry_after+2, parent_job_id=JOB_ID, job_type="c")`.
4. Terminate gracefully.

**Bước 6 — Finalize (khi job hoàn tất bình thường):**

1. **Flush tất cả partition buffers còn lại** của mọi thread.
2. **Update shard file:** overwrite `workspace/matchid/{shard_id}/data.parquet` với updated `is_ingested` values từ in-memory shard table.
3. **Propagate `is_ingested=1` về source `./matchID/` files:**
   - Group updated match_ids theo `source_file_path` (từ mapping đã build ở Bước 1).
   - Với mỗi `source_file_path` có ít nhất 1 match_id được update:
     - Đọc file đó từ GCS.
     - Update `is_ingested = 1` cho các match_ids trong group.
     - Overwrite file về GCS.
4. Nếu là resume run: `checkpoint.delete()` cho tất cả thread checkpoints của `PARENT_JOB_ID`.

---

## 5.3 Output

| Path | Mô tả |
|------|-------|
| `./match/{region}/{platform}/{year}/{month}/{date}/*.parquet` | Match detail data |
| `workspace/matchid/{shard_id}/data.parquet` | Updated `is_ingested` |
| `./matchID/{region}/{platform}/{puuid}/*.parquet` | Updated `is_ingested=1` propagated về source |
| `workspace/tmp/c/{job_id}_{thread_id}.parquet` | Tạm thời — chỉ tồn tại khi bị rate-limit |
| `state/checkpoint/c/{job_id}_{thread_id}.json` | Tạm thời — chỉ tồn tại khi bị rate-limit |

---

## 5.4 Flush Conditions Summary

| Điều kiện | Scope | Hành động | Ghi vào |
|-----------|-------|-----------|---------|
| Partition buffer ≥ 32MB | Per partition per thread | Flush partition đó + reset | `./match/...` |
| Hết queue (job hoàn tất) | Toàn bộ partitions, toàn bộ threads | Flush tất cả partition buffers còn lại | `./match/...` |
| Tất cả threads bị 429 | Toàn bộ partitions, toàn bộ threads | Merge → emergency flush → tmp, save checkpoint | `workspace/tmp/c/...` |

> **Không có end-of-match flush:** khác với Job A và Job B, Job C **không** flush khi hoàn thành 1 match riêng lẻ. Buffer chỉ flush theo threshold (32MB) hoặc khi job kết thúc. Lý do: match detail data nhỏ (~10–50KB/match), flush per-match sẽ tạo hàng nghìn file nhỏ trên GCS, làm tăng chi phí GCS operations và giảm query efficiency của Iceberg.

---

## 5.5 Config Keys

```yaml
job_c:
  shard_count: 4
  thread_pool_size: 8
  buffer_flush_mb: 32
  api_concurrency: 8
```
