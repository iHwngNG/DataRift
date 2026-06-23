# 03 — Job A: User (League) Ingestion

## 3.1 Purpose

Fetch toàn bộ ranked ladder entries (summoner data) từ Riot League-Entries API cho mỗi `platform × tier × division` combination. Mỗi record được transform, buffer in-memory theo PyArrow, và flush xuống GCS dưới dạng Parquet khi đạt ngưỡng hoặc khi kết thúc nhóm partition.

---

## 3.2 Trigger

- **Normal run:** Cloud Scheduler — independent daily cron (`0 6 * * *` UTC).
- **Resume run (sau rate-limit):** Cloud Tasks schedule lại job với env vars `RESUME_MODE=true` và `PARENT_JOB_ID=<id>`.

---

## 3.3 Input

| Source | Nội dung |
|--------|----------|
| `conf/job_a.yaml` | Danh sách `platforms`, `tiers`, `divisions`, `buffer_flush_mb`, `api_concurrency` |
| `conf/platforms.yaml` | Mapping `platform → region` |
| Env var `RIOT_API_KEY` | API key |
| Env var `JOB_ID` | Cloud Run Job execution ID (unique per invocation) |
| Env var `RESUME_MODE` | `true` nếu đây là resume run sau rate-limit |
| Env var `PARENT_JOB_ID` | Job ID của lần chạy trước (chỉ có khi `RESUME_MODE=true`) |

---

## 3.4 Detailed Workflow

### Bước 1 — Khởi tạo

1. Load config từ `conf/job_a.yaml` + `conf/platforms.yaml`.
2. Đọc `JOB_ID` và `RESUME_MODE` từ env.
3. Build danh sách **tất cả combinations** cần xử lý: `[{platform, region, tier, division}]`.
   - Apex tiers (Challenger, GrandMaster, Master) **không có** `division` — chúng tạo 3 entries riêng per platform, đánh dấu `division=None`.
   - Regular tiers (Iron → Diamond) × 4 divisions = 28 entries per platform.
   - Tổng: `(3 apex + 28 regular) × 4 platforms = 124 combinations`.
4. **Nếu `RESUME_MODE=true`:**
   - Với mỗi `thread_id` (Job A dùng thread pool đơn giản, mỗi thread = 1 combination):
     - Gọi `checkpoint.exists(job_type="a", job_id=PARENT_JOB_ID, thread_id)`.
     - Nếu tồn tại: load `CheckpointData` → lấy `remaining_items` (danh sách combinations chưa xử lý) và `tmp_parquet_path`.
     - Restore in-memory buffer từ `tmp_buffer.restore(tmp_parquet_path)` — inject vào buffer của thread đó trước khi bắt đầu.
     - Xóa tmp file ngay sau khi restore thành công.
   - Filter combinations list: chỉ giữ lại combinations thuộc `remaining_items` của checkpoint. Combinations không có checkpoint (đã xong trước khi rate-limit) → bỏ qua.
5. **Nếu normal run:** toàn bộ 124 combinations đều cần xử lý.

### Bước 2 — Phân phối công việc cho thread pool

1. Push toàn bộ combinations (sau bước 1 filter) vào `queue.Queue`.
2. Khởi động thread pool với `pool_size` từ config.
3. Truyền `JobContext(job_id=JOB_ID, job_type="a", shard_id=None)` vào `threaded_queue.run_threaded()`.

### Bước 3 — Xử lý từng combination (per thread)

Mỗi thread lấy 1 combination `{platform, region, tier, division}` từ queue và thực hiện:

**3a. Fetch từ Riot API:**

- **Apex tier** (`Challenger / GrandMaster / Master`):
  - Gọi endpoint `GET /lol/league/v4/{tier}leagues/by-queue/RANKED_SOLO_5x5`.
  - Response trả về 1 page duy nhất (không phân trang) — lấy toàn bộ `entries[]`.
- **Regular tier** (`Iron → Diamond`):
  - Gọi endpoint `GET /lol/league/v4/entries/{tier}/{division}?queue=RANKED_SOLO_5x5&page={page}`.
  - **Phân trang:** bắt đầu `page=1`, tăng dần. Dừng khi response trả về mảng rỗng `[]`.
  - Mỗi page fetch = 1 API call.

**3b. Transform mỗi record:**
- Map fields từ Riot response → schema `league/` (xem `02-gcs-layout.md`).
- Gán `region` từ mapping `platform → region` (via `src/riot_client/regions.py`).
- Gán `_ingested_at = now()`.

**3c. Buffer và flush:**
- Append từng record vào in-memory PyArrow buffer của thread đó (qua `src/parquet/buffer.py`).
- **Flush trigger — điều kiện flush xảy ra khi bất kỳ điều nào sau đây đúng:**
  1. **Buffer đạt ngưỡng 4MB** (size threshold): flush ngay, ghi Parquet xuống GCS, reset buffer.
  2. **Hết tất cả entries của combination hiện tại** (end-of-combination flush): flush toàn bộ buffer còn lại của combination này trước khi chuyển sang combination tiếp theo — đảm bảo không bao giờ trộn data của 2 `(tier, division)` khác nhau vào cùng 1 Parquet file nếu chúng thuộc 2 GCS paths khác nhau.
  3. **Job kết thúc bình thường** (end-of-job flush): flush tất cả buffers còn lại của mọi thread.
  4. **Rate-limit detected** (emergency flush): xem Bước 4.

- **GCS write path khi flush:**
  ```
  ./league/{region}/{platform}/{tier}/{division}/{job_id}_{thread_id}_{timestamp}.parquet
  ```
  - Apex tiers không có division → path: `./league/{region}/{platform}/{tier}/all/`

**3d. Sau khi xử lý xong 1 combination:**
- Không cần update state file (Job A stateless, mỗi run là full refresh).
- Thread lấy combination tiếp theo từ queue.

### Bước 4 — Rate-limit handling (khi `RateLimitError` được raise)

`threaded_queue` monitor tất cả threads. Khi **tất cả active threads** đồng thời nhận `RateLimitError`:

1. **Drain queue:** lấy toàn bộ combinations còn chưa xử lý.
2. **Per-thread emergency flush:**
   - Lấy in-memory buffer hiện tại của thread (dù chưa đạt 4MB).
   - Gọi `tmp_buffer.flush(table, job_type="a", job_id=JOB_ID, thread_id)` → ghi xuống `workspace/tmp/a/{JOB_ID}_{thread_id}.parquet`.
3. **Per-thread checkpoint save:**
   - Gọi `checkpoint.save(CheckpointData(remaining_items=[combinations chưa xử lý của thread này], tmp_parquet_path=..., retry_after_seconds=N, ...))` → ghi `state/checkpoint/a/{JOB_ID}_{thread_id}.json`.
4. **Schedule retry:**
   - Gọi `scheduler.schedule_retry(job_name="job-a-user-ingestion", delay_seconds=retry_after+2, parent_job_id=JOB_ID, job_type="a")`.
5. **Terminate:** job hiện tại exit với code 0 (graceful).

### Bước 5 — Cleanup sau resume thành công

Sau khi resume run hoàn tất tất cả combinations:
- Gọi `checkpoint.delete()` cho tất cả thread checkpoints của `PARENT_JOB_ID`.
- Tmp files đã xóa ở Bước 1 khi restore, không cần xóa lại.

---

## 3.5 Output

| Path | Mô tả |
|------|-------|
| `./league/{region}/{platform}/{tier}/{division}/*.parquet` | League entries theo từng partition |
| `workspace/tmp/a/{job_id}_{thread_id}.parquet` | Tạm thời — chỉ tồn tại khi bị rate-limit |
| `state/checkpoint/a/{job_id}_{thread_id}.json` | Tạm thời — chỉ tồn tại khi bị rate-limit |

---

## 3.6 Flush Conditions Summary

| Điều kiện | Hành động | Ghi vào |
|-----------|-----------|---------|
| Buffer ≥ 4MB | Flush + reset buffer, tiếp tục job | `./league/...` |
| Hết entries của 1 combination | Flush buffer còn lại của combination đó | `./league/...` |
| Hết toàn bộ queue (job hoàn tất) | Flush tất cả buffers còn lại | `./league/...` |
| Tất cả threads bị 429 | Emergency flush buffer → tmp, save checkpoint, terminate | `workspace/tmp/a/...` |

---

## 3.7 Concurrency Model

Job A dùng `threaded_queue` với `pool_size` thấp (config mặc định: 4 threads). Lý do: League-Entries-V4 là endpoint ít call nhất (~124 combinations, mỗi regular tier có thêm vài pages), không cần concurrency cao. Mỗi thread xử lý 1 combination tại 1 thời điểm, có buffer riêng.

---

## 3.8 Config Keys

```yaml
job_a:
  platforms: [vn2, kr, euw1, eun1]
  tiers: [iron, bronze, silver, gold, platinum, emerald, diamond]
  apex_tiers: [challenger, grandmaster, master]
  divisions: [I, II, III, IV]
  buffer_flush_mb: 4
  thread_pool_size: 4
  api_concurrency: 4         # semaphore limit trong riot_client
```
