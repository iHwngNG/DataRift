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

## 8.6 `src/datarift/riot_client/client.py`

**Purpose:** Async HTTP client wrapper cho Riot API với rate limiting + retry.

**Behavior:**
- Async requests via `httpx`
- Retry on 429/5xx via `tenacity` (exponential backoff)
- Semaphore-based concurrency control (configurable per job via `conf/`)

## 8.7 `src/datarift/riot_client/regions.py`

**Purpose:** Static mapping `platform -> region` (cluster) — single source of truth, dùng bởi Job A/B/C để build URLs đúng cluster cho Match-V5 calls.

## 8.8 `src/datarift/workers/threaded_queue.py`

**Purpose:** Generic `queue.Queue` + `ThreadPoolExecutor` runner — dùng chung bởi Job B Worker và Job C Worker.

**Interface:**
- `run_threaded(items: list, worker_fn: Callable, pool_size: int) -> list[Result]`
- Worker function nhận 1 item, trả về kết quả (hoặc raises — caller xử lý lỗi per-item, không fail toàn batch)

## 8.9 `src/datarift/workers/puuid_fetcher.py`

**Purpose:** Implement fetch logic cho 1 PUUID (Job B's Section 4.3 logic) — pagination, early-stop dedup, hard cap, `startTime` handling.

**Interface:**
- `fetch_match_ids(puuid: str, last_read: date | None, existing_match_ids: set[str]) -> list[str]`

## 8.10 `src/datarift/workers/match_fetcher.py`

**Purpose:** Implement fetch logic cho 1 `match_id` (Job C) — call Match-V5 detail endpoint, parse response, extract `game_start_timestamp` cho partition derivation.

**Interface:**
- `fetch_match_detail(match_id: str, region: str) -> dict`
- `derive_partition_date(game_start_timestamp: int) -> tuple[str, str, str]` (returns year, month, date)

## 8.11 `src/datarift/iceberg/catalog.py`

**Purpose:** Setup pyiceberg catalog client (BigQuery REST Catalog backend).

**Interface:**
- `get_catalog() -> Catalog`
- `get_or_create_table(catalog: Catalog, table_name: str, schema, partition_spec) -> Table`

## 8.12 `src/datarift/iceberg/sync.py`

**Purpose:** Register new Parquet files into Iceberg table (Job D logic).

**Interface:**
- `find_new_files(table: Table, gcs_prefix: str) -> list[str]`
- `register_files(table: Table, file_paths: list[str]) -> Snapshot`

## 8.13 `src/datarift/config/loader.py`

**Purpose:** Load & validate `conf/*.yaml` into typed config objects (Pydantic models), merging `base.yaml` với job-specific overrides.

**Interface:**
- `load_config(job_name: str) -> JobConfig` (Pydantic model, fields depend on job)
