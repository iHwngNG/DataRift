# 08 — Shared `src/` Module Specs

## 8.1 `src/datarift/hashing/string_to_int.py`

**Purpose:** Deterministic conversion từ string (`puuid` hoặc `match_id`) → integer, dùng làm input cho shard function.

**Approach:** Dùng một hash function ổn định (e.g. MD5 hoặc CRC32 trên UTF-8 bytes của string), lấy kết quả integer. Phải đảm bảo:
- Deterministic across runs/processes (không dùng Python's built-in `hash()` vì nó randomized per-process)
- Output distribution đủ đều để shard balance tốt

**Interface:**
- `to_int(value: str) -> int`

## 8.2 `src/datarift/hashing/shard.py`

**Purpose:** Map integer → shard ID trong khoảng `[0, N)`.

**Approach:** Modulo operation đơn giản: `shard_id = int_value % shard_count`. `shard_count` đọc từ `conf/`.

**Interface:**
- `assign_shard(int_value: int, shard_count: int) -> int`

## 8.3 `src/datarift/parquet/buffer.py`

**Purpose:** Accumulate records in-memory as PyArrow Table, flush to GCS khi đạt size threshold.

**Behavior:**
- `add(record: dict)` — append record vào internal buffer
- Track estimated size (bytes) sau mỗi `add`
- `should_flush() -> bool` — true khi estimated size >= threshold (configurable per job)
- `flush() -> pa.Table` — convert buffer → PyArrow Table, reset internal state
- Hỗ trợ **partitioned buffering**: Job C cần buffer riêng theo `(region, platform, year, month, date)` — module nên support multiple named buffers trong 1 instance, mỗi buffer flush độc lập khi đạt threshold riêng.

## 8.4 `src/datarift/parquet/io.py`

**Purpose:** Read/write Parquet files tới/từ GCS.

**Interface:**
- `read_parquet_files(gcs_prefix: str) -> pa.Table` — list + read + concat tất cả Parquet files dưới 1 prefix
- `write_parquet(table: pa.Table, gcs_path: str) -> None` — write 1 PyArrow Table → Parquet file tại path cụ thể
- `overwrite_parquet(table: pa.Table, gcs_path: str) -> None` — explicit overwrite (dùng cho `workspace/` state files)

## 8.5 `src/datarift/gcs/paths.py`

**Purpose:** Centralized path builders — tránh string formatting rải rác khắp codebase.

**Interface (examples):**
- `league_path(region, platform, tier, division) -> str`
- `match_id_path(region, platform, puuid) -> str`
- `match_path(region, platform, year, month, date) -> str`
- `puuid_shard_path(shard_id) -> str`
- `matchid_shard_path(shard_id) -> str`
- `tmp_buffer_path(job_type, job_id, thread_id) -> str` — `workspace/tmp/{job_type}/{job_id}_{thread_id}.parquet`
- `checkpoint_path(job_type, job_id, thread_id) -> str` — `state/checkpoint/{job_type}/{job_id}_{thread_id}.json`

## 8.6 `src/datarift/riot_client/client.py`

**Purpose:** Async HTTP client wrapper cho Riot API với rate limiting + retry. Định nghĩa custom exception `RateLimitError` (kèm `retry_after_seconds` từ response header) được raise khi nhận 429 — đây là signal để `threaded_queue` khởi động shutdown sequence.

**Behavior:**
- Async requests via `httpx`
- Raise `RateLimitError(retry_after_seconds=N)` khi nhận 429 — **không tự retry 429** (việc retry là trách nhiệm của scheduling mechanism bên ngoài, không phải trong-process sleep)
- Retry on 5xx via `tenacity` (exponential backoff) — chỉ retry server errors, không retry rate limit
- Semaphore-based concurrency control (configurable per job via `conf/`)

## 8.7 `src/datarift/riot_client/regions.py`

**Purpose:** Static mapping `platform -> region` (cluster) — single source of truth, dùng bởi Job A/B/C để build URLs đúng cluster cho Match-V5 calls.

## 8.8 `src/datarift/workers/threaded_queue.py`

**Purpose:** Generic `queue.Queue` + `ThreadPoolExecutor` runner — dùng chung bởi Job A, Job B Worker và Job C Worker. Bao gồm logic detect khi tất cả threads bị rate-limit và delegate sang `rate_limit/`.

**Behavior:**
- Mỗi thread xử lý 1 item từ queue; nếu worker function raise `RateLimitError` (custom exception từ `riot_client/client.py`), thread report lại vào shared counter.
- `threaded_queue` monitor: khi **tất cả active threads** đều báo `RateLimitError` trong cùng 1 cycle → trigger rate-limit shutdown sequence:
  1. Drain queue → lấy danh sách `remaining_items`
  2. Gọi `rate_limit.tmp_buffer.flush_all(job_id, thread_id)` cho từng thread — flush in-flight buffer xuống `workspace/tmp/`
  3. Gọi `rate_limit.checkpoint.save(...)` cho từng thread — ghi `state/checkpoint/` với `remaining_items` + `tmp_parquet_path` + `retry_after_seconds`
  4. Gọi `rate_limit.scheduler.schedule_retry(job_name, retry_after_seconds + 2)` — enqueue delayed Cloud Run Job via Cloud Tasks
  5. Return signal `RATE_LIMITED` cho caller (job `main.py` tự terminate gracefully)

**Interface:**
- `run_threaded(items: list, worker_fn: Callable, pool_size: int, job_context: JobContext) -> RunResult`
- `RunResult` = `COMPLETED` | `RATE_LIMITED`
- `JobContext` chứa `job_id`, `job_type`, `shard_id` — truyền vào để `threaded_queue` có đủ thông tin khi gọi `rate_limit.*`

---

## Rate Limit Module — `src/datarift/rate_limit/`

Ba files trong module này hoạt động cùng nhau để xử lý toàn bộ rate-limit lifecycle. Mỗi file có 1 responsibility duy nhất, không overlap.

---

## 8.9 `src/datarift/rate_limit/checkpoint.py`

**Purpose:** Serialize và deserialize checkpoint state của từng thread khi job bị rate-limit terminate.

**Behavior:**
- **Save:** Nhận `CheckpointData` (job_id, thread_id, job_type, shard_id, remaining_items, tmp_parquet_path, retry_after_seconds) → serialize thành JSON → write tới `state/checkpoint/{job_type}/{job_id}_{thread_id}.json` qua `parquet/io.py`.
- **Load:** Nhận `job_id` + `thread_id` + `job_type` → đọc JSON từ GCS → deserialize thành `CheckpointData`. Raise `CheckpointNotFoundError` nếu không có checkpoint (job chạy lần đầu, không phải resume).
- **Delete:** Sau khi thread resume và hoàn tất, xóa checkpoint file khỏi GCS.

**Interface:**
- `save(data: CheckpointData) -> None`
- `load(job_type: str, job_id: str, thread_id: int) -> CheckpointData`
- `delete(job_type: str, job_id: str, thread_id: int) -> None`
- `exists(job_type: str, job_id: str, thread_id: int) -> bool`

---

## 8.10 `src/datarift/rate_limit/tmp_buffer.py`

**Purpose:** Flush in-flight Parquet buffer từ RAM xuống `workspace/tmp/` khi bị rate-limit; restore lại buffer từ GCS khi job resume.

**Behavior:**
- **Flush:** Nhận in-flight `pa.Table` + `job_id` + `thread_id` + `job_type` → write Parquet tới `workspace/tmp/{job_type}/{job_id}_{thread_id}.parquet`. Trả về GCS path (để lưu vào checkpoint).
- **Restore:** Nhận `tmp_parquet_path` → đọc Parquet từ GCS → trả về `pa.Table` để inject trở lại vào buffer. Sau khi inject xong, caller gọi `delete` để xóa tmp file.
- **Delete:** Xóa tmp file sau khi restore thành công.

**Interface:**
- `flush(table: pa.Table, job_type: str, job_id: str, thread_id: int) -> str` (returns GCS path)
- `restore(tmp_parquet_path: str) -> pa.Table`
- `delete(tmp_parquet_path: str) -> None`

---

## 8.11 `src/datarift/rate_limit/scheduler.py`

**Purpose:** Schedule delayed re-execution của cùng 1 Cloud Run Job via Google Cloud Tasks.

**Behavior:**
- Nhận `job_name` (Cloud Run Job name), `delay_seconds` (`Retry-After + 2`), và `job_env_overrides` (env vars cần override, vd: `RESUME_MODE=true`, `JOB_ID={original_job_id}` để job mới biết cần load checkpoint của job cũ) → tạo Cloud Tasks task với `scheduleTime = now + delay_seconds` để trigger Cloud Run Job tương ứng.
- Job mới khi start sẽ đọc env var `RESUME_MODE` + `PARENT_JOB_ID`: nếu `RESUME_MODE=true` thì mỗi thread gọi `checkpoint.load(job_type, parent_job_id, thread_id)` → restore state → tiếp tục từ `remaining_items`, inject `tmp_buffer.restore(tmp_parquet_path)` vào buffer trước khi bắt đầu xử lý.

**Interface:**
- `schedule_retry(job_name: str, delay_seconds: int, parent_job_id: str, job_type: str) -> None`

---

## 8.12 `src/datarift/workers/puuid_fetcher.py`

**Purpose:** Implement fetch logic cho 1 PUUID (Job B's Section 4.3 logic) — pagination, early-stop dedup, hard cap, `startTime` handling. Raise `RateLimitError` khi nhận 429 response (để `threaded_queue` detect).

**Interface:**
- `fetch_match_ids(puuid: str, last_read: date | None, existing_match_ids: set[str]) -> list[str]`

## 8.13 `src/datarift/workers/match_fetcher.py`

**Purpose:** Implement fetch logic cho 1 `match_id` (Job C) — call Match-V5 detail endpoint, parse response, extract `game_start_timestamp` cho partition derivation. Raise `RateLimitError` khi nhận 429.

**Interface:**
- `fetch_match_detail(match_id: str, region: str) -> dict`
- `derive_partition_date(game_start_timestamp: int) -> tuple[str, str, str]` (returns year, month, date)

## 8.14 `src/datarift/iceberg/catalog.py`

**Purpose:** Setup pyiceberg catalog client (BigQuery REST Catalog backend).

**Interface:**
- `get_catalog() -> Catalog`
- `get_or_create_table(catalog: Catalog, table_name: str, schema, partition_spec) -> Table`

## 8.15 `src/datarift/iceberg/sync.py`

**Purpose:** Register new Parquet files into Iceberg table (Job D logic).

**Interface:**
- `find_new_files(table: Table, gcs_prefix: str) -> list[str]`
- `register_files(table: Table, file_paths: list[str]) -> Snapshot`

## 8.16 `src/datarift/config/loader.py`

**Purpose:** Load & validate `conf/*.yaml` into typed config objects (Pydantic models), merging `base.yaml` với job-specific overrides.

**Interface:**
- `load_config(job_name: str) -> JobConfig` (Pydantic model, fields depend on job)
