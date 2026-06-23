# 04 — Job B: Match ID Ingestion

Job B gồm 2 thành phần tách biệt: **Job B-Distributor** (single job, sharding) và **Job B Worker** (4 instances chạy song song, mỗi instance phụ trách 1 shard).

---

## 4.1 Job B-Distributor

### Purpose

Đọc toàn bộ PUUIDs từ `./league/`, merge với `last_read` state hiện có, shard hóa theo hash, ghi shard files vào `workspace/puuid/{shard_id}/`, rồi trigger 4 Job B Workers qua Pub/Sub.

### Trigger

Cloud Scheduler — independent cron, chạy sau Job A đủ thời gian để hoàn tất (e.g. `0 8 * * *` UTC).

### Input

| Source | Nội dung |
|--------|----------|
| `./league/**/*.parquet` | Toàn bộ user data từ Job A |
| `workspace/puuid/{0..3}/data.parquet` | State files từ lần chạy trước (nếu có) — chứa `last_read` per puuid |
| `conf/job_b.yaml` | `shard_count` |

### Detailed Workflow

**Bước 1 — Load existing state:**
1. Với mỗi `shard_id` trong `[0..3]`: cố gắng đọc `workspace/puuid/{shard_id}/data.parquet`.
2. Gộp tất cả shard files thành 1 in-memory dict: `existing_state = {puuid → last_read}`.
3. Nếu không có file nào (lần chạy đầu tiên): `existing_state = {}`.

**Bước 2 — Load và deduplicate PUUIDs từ league data:**
1. List & read toàn bộ Parquet files dưới `./league/**`.
2. Extract tất cả `(puuid, region, platform)` records.
3. Deduplicate theo `puuid` (1 puuid có thể xuất hiện nhiều lần nếu xuất hiện ở nhiều tier/division — giữ 1 record duy nhất, ưu tiên record có tier cao nhất).

**Bước 3 — Merge state:**
- Với mỗi PUUID vừa extract:
  - Nếu `puuid` có trong `existing_state`: gán `last_read = existing_state[puuid]` (giữ nguyên, không reset).
  - Nếu PUUID mới chưa có: gán `last_read = NULL`.
- PUUIDs trong `existing_state` nhưng không còn trong `./league/` (đã rời khỏi ranked ladder) → bỏ qua, không đưa vào shard files.

**Bước 4 — Shard hóa:**
1. Với mỗi PUUID: gọi `string_to_int(puuid)` → `puuid_int`.
2. Gọi `assign_shard(puuid_int, shard_count=4)` → `shard_id`.
3. Group records theo `shard_id`.

**Bước 5 — Ghi shard files:**
- Với mỗi shard group: overwrite `workspace/puuid/{shard_id}/data.parquet` (schema: `puuid, last_read, region, platform`).
- Dùng `parquet/io.py → overwrite_parquet()`.

**Bước 6 — Trigger workers:**
- Publish 4 Pub/Sub messages tới topic Job B Worker trigger.
- Payload mỗi message: `{"shard_id": <int>}`.

### Output

- `workspace/puuid/{0..3}/data.parquet` (overwritten)

---

## 4.2 Job B Worker

### Purpose

Với shard được assign, fetch match ID list từ Riot API cho mỗi PUUID cần update, buffer kết quả in-memory, flush xuống `./matchID/` khi đạt ngưỡng, rồi update `last_read` trong shard file khi hoàn tất.

### Trigger

- **Normal run:** Pub/Sub message từ Job B-Distributor, payload: `{"shard_id": <int>}`. Env var `SHARD_ID` được set từ payload.
- **Resume run:** Cloud Tasks schedule lại với `RESUME_MODE=true`, `PARENT_JOB_ID=<id>`, `SHARD_ID=<int>`.

### Input

| Source | Nội dung |
|--------|----------|
| `workspace/puuid/{shard_id}/data.parquet` | Shard file từ Distributor |
| Env var `SHARD_ID` | Shard index của worker này |
| Env var `JOB_ID` | Execution ID |
| Env var `RESUME_MODE` | `true` nếu resume |
| Env var `PARENT_JOB_ID` | Job ID trước (khi resume) |

### Detailed Workflow

**Bước 1 — Khởi tạo:**
1. Đọc `SHARD_ID`, `JOB_ID`, `RESUME_MODE`, `PARENT_JOB_ID` từ env.
2. Load shard file `workspace/puuid/{shard_id}/data.parquet` vào memory.
3. Filter records: chỉ giữ `last_read IS NULL OR last_read < today`. Skip `last_read == today`.
4. Build `work_items = [filtered puuid records]`.

**Bước 2 — Resume mode (nếu `RESUME_MODE=true`):**
1. Với mỗi `thread_id` trong `[0..pool_size-1]`:
   - Gọi `checkpoint.exists(job_type="b", job_id=PARENT_JOB_ID, thread_id)`.
   - Nếu tồn tại: load `CheckpointData` → lấy `remaining_items` (list PUUIDs chưa xử lý của thread đó).
   - Restore in-memory buffer: `tmp_buffer.restore(checkpoint.tmp_parquet_path)` → buffer của thread nhận lại PyArrow Table từ lần chạy trước.
   - Xóa tmp file ngay sau restore thành công.
2. Replace `work_items` bằng union của `remaining_items` từ tất cả thread checkpoints (đây là PUUIDs chưa xử lý).
3. PUUIDs đã được xử lý xong trước khi rate-limit → không có checkpoint → không có trong `work_items`.

**Bước 3 — Phân phối và khởi động thread pool:**
1. Push `work_items` vào `queue.Queue`.
2. Truyền `JobContext(job_id=JOB_ID, job_type="b", shard_id=SHARD_ID)` vào `threaded_queue.run_threaded()`.
3. Mỗi thread có **buffer riêng** (instance `ParquetBuffer` riêng, flush threshold = 1MB).

**Bước 4 — Xử lý từng PUUID (per thread):**

Thread lấy 1 PUUID record `{puuid, last_read, region, platform}` từ queue và thực hiện:

**4a. Chuẩn bị dedup set (Case B):**
- Nếu `last_read IS NOT NULL`: load tất cả `match_id` hiện có trong `./matchID/{region}/{platform}/{puuid}/` vào Python `set` → dùng để early-stop.
- Nếu `last_read IS NULL`: bỏ qua bước này (không cần dedup, chưa có data).

**4b. Fetch match IDs từ Riot API:**

**Case A — `last_read IS NULL` (first run cho puuid này):**
- `start = 0`, `count = 100` (từ config), không truyền `startTime`.
- Lặp:
  1. Gọi `GET /lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&start={start}&count={count}`.
  2. Nếu response = `[]` (không còn match) → dừng.
  3. Append match IDs vào `collected`.
  4. Nếu `len(collected) >= 1000` → dừng (hard cap).
  5. `start += 100` → tiếp tục.

**Case B — `last_read < today` (incremental):**
- `start = 0`, `count = 100`, truyền `startTime = unix_timestamp(last_read)`.
- Lặp:
  1. Gọi `GET /lol/match/v5/matches/by-puuid/{puuid}/ids?queue=420&start={start}&count={count}&startTime={startTime}`.
  2. Nếu response = `[]` → dừng.
  3. Với mỗi `match_id` trong response:
     - Nếu `match_id` đã có trong dedup set → **early-stop**: dừng toàn bộ pagination cho puuid này.
     - Nếu chưa có → append vào `collected`, add vào dedup set.
  4. Nếu `len(collected) >= 1000` → dừng (hard cap).
  5. `start += 100` → tiếp tục.

**Nếu `RateLimitError` được raise trong bất kỳ API call nào:** propagate lên `threaded_queue` để xử lý (xem Bước 5).

**4c. Buffer kết quả:**
- Với mỗi `match_id` trong `collected`: tạo record `{match_id, puuid, is_ingested=0, region, platform, _ingested_at=now()}`.
- Append vào in-memory buffer của thread qua `parquet/buffer.py`.

**4d. Flush conditions — buffer flush khi bất kỳ điều nào sau đây đúng:**

| Điều kiện | Hành động | Ghi vào |
|-----------|-----------|---------|
| Buffer ≥ 1MB | Flush + reset buffer, tiếp tục thread | `./matchID/{region}/{platform}/{puuid}/` |
| Hoàn tất xử lý 1 PUUID | Flush buffer còn lại của PUUID đó | `./matchID/{region}/{platform}/{puuid}/` |
| Hết queue (thread idle, job hoàn tất) | Flush tất cả buffer còn lại | `./matchID/{region}/{platform}/{puuid}/` |
| Tất cả threads bị 429 | Emergency flush → tmp, save checkpoint, terminate | `workspace/tmp/b/...` |

- **GCS write path:** `./matchID/{region}/{platform}/{puuid}/{job_id}_{thread_id}_{timestamp}.parquet`

**4e. Update `last_read` sau khi xong 1 PUUID:**
- Mark `last_read = today` cho PUUID đó trong in-memory shard table.
- Không ghi GCS ngay — gom toàn bộ updates, ghi 1 lần cuối job.

**Bước 5 — Rate-limit handling:**

Khi `threaded_queue` detect tất cả threads bị `RateLimitError`:
1. Drain queue → `remaining_puuids` (chưa được thread nào pick up).
2. Per-thread:
   - `remaining_items_of_thread` = puuid đang xử lý dở của thread đó (nếu có) + phần còn lại trong queue được chia đều giữa các threads.
   - `tmp_buffer.flush(buffer, job_type="b", job_id=JOB_ID, thread_id)` → `workspace/tmp/b/{JOB_ID}_{thread_id}.parquet`.
   - `checkpoint.save(CheckpointData(remaining_items=remaining_puuids_of_thread, tmp_parquet_path=..., retry_after_seconds=N, shard_id=SHARD_ID, ...))`.
3. `scheduler.schedule_retry(job_name="job-b-worker", delay_seconds=retry_after+2, parent_job_id=JOB_ID, job_type="b")`.
4. Terminate gracefully.

**Bước 6 — Finalize (khi job hoàn tất bình thường):**
1. Flush tất cả buffers còn lại của mọi thread.
2. Overwrite `workspace/puuid/{shard_id}/data.parquet` với updated `last_read` values từ in-memory shard table.
3. Nếu là resume run: gọi `checkpoint.delete()` cho tất cả thread checkpoints của `PARENT_JOB_ID`.

---

## 4.3 Output

| Path | Mô tả |
|------|-------|
| `./matchID/{region}/{platform}/{puuid}/*.parquet` | Match IDs mới với `is_ingested=0` |
| `workspace/puuid/{shard_id}/data.parquet` | Updated `last_read` per puuid |
| `workspace/tmp/b/{job_id}_{thread_id}.parquet` | Tạm thời — chỉ tồn tại khi bị rate-limit |
| `state/checkpoint/b/{job_id}_{thread_id}.json` | Tạm thời — chỉ tồn tại khi bị rate-limit |

---

## 4.4 Config Keys

```yaml
job_b:
  shard_count: 4
  thread_pool_size: 8
  buffer_flush_mb: 1
  api_concurrency: 8
  pagination_count: 100
  max_match_ids_per_puuid_first_run: 1000
  max_match_ids_per_puuid_incremental: 1000
```
