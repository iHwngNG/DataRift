# 09 — Sprint Breakdown

Mỗi sprint giả định ~1 tuần. Checkbox `[ ]` → đánh `[x]` khi hoàn thành.

---

## Sprint 1 — Foundation & Shared Core

**Mục tiêu:** Setup project skeleton, config system, shared low-level modules không phụ thuộc Riot API.

- [x] Tạo repo structure theo `07-project-structure.md` (folders: `src/`, `jobs/`, `infra/`, `conf/`)
- [x] Viết `pyproject.toml` (Python 3.14, deps cơ bản: pyarrow, pyyaml, pydantic, structlog, httpx, tenacity)
- [x] Setup `conf/base.yaml` + `conf/platforms.yaml` (region/platform/tier/division definitions)
- [x] Implement `src/datarift/config/loader.py` (load + validate YAML → Pydantic models)
- [x] Implement `src/datarift/hashing/string_to_int.py` (deterministic hash → int)
- [x] Implement `src/datarift/hashing/shard.py` (modulo shard assignment)
- [x] Viết unit tests cho `hashing/` — verify deterministic output, distribution balance check across 4 shards
- [x] Implement `src/datarift/gcs/paths.py` (path builders cho tất cả layers theo `02-gcs-layout.md`)
- [x] Viết unit tests cho `gcs/paths.py`
- [x] Implement `src/datarift/parquet/buffer.py` (single-buffer flush logic)
- [x] Viết unit tests cho `buffer.py` — verify flush trigger đúng threshold

---

## Sprint 2 — Parquet I/O & Riot Client Core

**Mục tiêu:** GCS read/write layer hoàn chỉnh + Riot API client với rate limiting.

- [ ] Implement `src/datarift/parquet/io.py` (`read_parquet_files`, `write_parquet`, `overwrite_parquet`)
- [ ] Viết integration test cho `parquet/io.py` (dùng GCS emulator hoặc test bucket)
- [ ] Extend `src/datarift/parquet/buffer.py` để support **partitioned/multi-buffer** mode (cho Job C)
- [ ] Viết unit tests cho multi-buffer mode
- [ ] Implement `src/datarift/riot_client/client.py` (async httpx client, semaphore concurrency limit, tenacity retry cho 429/5xx)
- [ ] Implement `src/datarift/riot_client/regions.py` (platform → region/cluster mapping)
- [ ] Implement `src/datarift/riot_client/league_api.py` (wrappers cho League-Entries-V4: regular tiers + apex tiers Challenger/GM/Master)
- [ ] Implement `src/datarift/riot_client/match_api.py` (wrappers cho Match-V5: match IDs by PUUID + match detail by ID, hỗ trợ `startTime` param)
- [ ] Viết unit tests cho `riot_client/` với mocked HTTP responses

---

## Sprint 3 — Job A (User Ingestion)

**Mục tiêu:** Job A hoàn chỉnh, deployable, ghi đúng `./league/` structure.

- [ ] Viết `conf/job_a.yaml` (buffer_flush_mb, platforms/tiers/divisions reference)
- [ ] Implement `jobs/job_a_user_ingestion/main.py`: loop platforms × tiers × divisions, call `league_api`, buffer via `parquet/buffer.py`, flush qua `parquet/io.py`
- [ ] Handle apex tiers (Challenger/GrandMaster/Master) qua nhánh logic riêng trong `league_api.py`
- [ ] Viết `jobs/job_a_user_ingestion/Dockerfile`
- [ ] Local test: run Job A against real Riot API (dev key), verify output Parquet schema đúng `02-gcs-layout.md`
- [ ] Verify path structure đúng `./league/{region}/{platform}/{tier}/{division}/`
- [ ] Viết integration test: mocked Riot responses → verify Parquet output content & partitioning

---

## Sprint 4 — Job B-Distributor + Job B Worker (Core Logic)

**Mục tiêu:** Sharding pipeline hoàn chỉnh cho Job B, end-to-end trên local/dev.

- [ ] Viết `conf/job_b.yaml` (shard_count, thread_pool_size, buffer_flush_mb, pagination_count, max_match_ids caps)
- [ ] Implement `jobs/job_b_distributor/main.py`: đọc `./league/**`, extract PUUIDs, merge với `last_read` hiện có (nếu có), shard via `hashing/`, write `workspace/puuid/{shard_id}/data.parquet`
- [ ] Implement Pub/Sub publish logic trong Job B-Distributor (4 messages, payload `{shard_id}`)
- [ ] Implement `src/datarift/workers/threaded_queue.py` (generic queue + thread pool runner)
- [ ] Viết unit tests cho `threaded_queue.py`
- [ ] Implement `src/datarift/workers/puuid_fetcher.py`:
  - [ ] Case A logic: `last_read IS NULL` → pagination từ 0, cap 1000
  - [ ] Case B logic: `last_read < today` → `startTime` param + early-stop dedup + cap 1000
  - [ ] Skip logic: `last_read == today`
- [ ] Viết unit tests cho `puuid_fetcher.py` (mock cả 2 cases + early-stop + cap)
- [ ] Implement `jobs/job_b_worker/main.py`: đọc `SHARD_ID` từ env, load shard file, filter, run threaded queue, buffer output, flush `./matchID/`, update `last_read`, overwrite shard file
- [ ] Viết `jobs/job_b_distributor/Dockerfile` và `jobs/job_b_worker/Dockerfile`

---

## Sprint 5 — Job B End-to-End Validation

**Mục tiêu:** Validate toàn bộ Job B flow trên dev environment.

- [ ] Deploy Job B-Distributor + 4 Job B Worker instances lên Cloud Run (dev)
- [ ] Setup Pub/Sub topic + subscriptions cho Job B trigger chain (Terraform dev — xem Sprint 8)
- [ ] Run end-to-end: Distributor → 4 Workers → verify `./matchID/` output đúng schema & path
- [ ] Verify `last_read` updates đúng trong `workspace/puuid/{shard_id}/data.parquet` sau mỗi run
- [ ] Run lần 2 trong cùng ngày: verify các PUUID có `last_read == today` bị skip đúng
- [ ] Run lần 3 (giả lập ngày khác): verify incremental fetch (`startTime` + early-stop) hoạt động đúng, không duplicate match IDs
- [ ] Load test: kiểm tra thread pool size phù hợp với Riot rate limit (không bị 429 storm)

---

## Sprint 6 — Job C-Distributor + Job C Worker (Core Logic)

**Mục tiêu:** Sharding pipeline hoàn chỉnh cho Job C.

- [ ] Viết `conf/job_c.yaml` (shard_count, thread_pool_size, buffer_flush_mb=32)
- [ ] Implement `jobs/job_c_distributor/main.py`: đọc `./matchID/**` (per-file để giữ `puuid`/path context), extract `(match_id, is_ingested, region, platform, puuid)`, shard via `hashing/`, write `workspace/matchid/{shard_id}/data.parquet`
- [ ] Implement Pub/Sub publish logic trong Job C-Distributor
- [ ] Implement `src/datarift/workers/match_fetcher.py`:
  - [ ] `fetch_match_detail(match_id, region)`
  - [ ] `derive_partition_date(game_start_timestamp)` → (year, month, date)
- [ ] Viết unit tests cho `match_fetcher.py` (mock Riot response, verify partition derivation correctness)
- [ ] Implement `jobs/job_c_worker/main.py`:
  - [ ] Đọc `SHARD_ID`, load shard file, filter `is_ingested=0`
  - [ ] Run threaded queue → `match_fetcher`
  - [ ] Multi-partition buffer (region/platform/year/month/date) flush tại 32MB
  - [ ] Write `./match/...`
  - [ ] Update `is_ingested=1` trong shard file
  - [ ] Propagate `is_ingested=1` ngược về source `./matchID/{region}/{platform}/{puuid}/` files (group by `puuid`, rewrite affected files)
- [ ] Viết `jobs/job_c_distributor/Dockerfile` và `jobs/job_c_worker/Dockerfile`

---

## Sprint 7 — Job C End-to-End Validation + Job D (Iceberg Sync)

**Mục tiêu:** Validate Job C flow + implement Iceberg sync.

- [ ] Deploy Job C-Distributor + 4 Job C Worker instances lên Cloud Run (dev)
- [ ] Setup Pub/Sub topic + subscriptions cho Job C trigger chain
- [ ] Run end-to-end: Distributor → 4 Workers → verify `./match/{region}/{platform}/{year}/{month}/{date}/` output
- [ ] Verify `is_ingested` updates đúng cả trong `workspace/matchid/` và `./matchID/` source files
- [ ] Run lần 2: verify các `match_id` đã `is_ingested=1` không bị re-fetch
- [ ] Viết `conf/job_d.yaml` (table definitions, partition specs, catalog config)
- [ ] Implement `src/datarift/iceberg/catalog.py` (BigQuery REST Catalog setup via pyiceberg)
- [ ] Implement `src/datarift/iceberg/sync.py` (`find_new_files`, `register_files`)
- [ ] Implement `jobs/job_d_iceberg_sync/main.py`: loop qua 3 tables (`league`, `match_id`, `match`), find new files, register via `add_files`
- [ ] Viết `jobs/job_d_iceberg_sync/Dockerfile`
- [ ] Run Job D on dev: verify Iceberg tables created trong BigQuery catalog với đúng partition spec
- [ ] Re-run Job D: verify idempotency (không duplicate file registration, snapshot không tăng nếu không có file mới)

---

## Sprint 8 — Infrastructure (Terraform) & Scheduling

**Mục tiêu:** Toàn bộ infra-as-code cho dev & prod.

- [ ] `infra/shared/modules/gcs/`: bucket `datarift-lakehouse` + folder structure (via placeholder objects nếu cần)
- [ ] `infra/shared/modules/pubsub/`: topics + subscriptions cho Job B & C trigger chains
- [ ] `infra/shared/modules/cloud_run_job/`: generic reusable module cho Cloud Run Job definition (accepts image, env vars, resource limits)
- [ ] `infra/shared/modules/scheduler/`: Cloud Scheduler jobs cho Job A, B-Dist, C-Dist, D (independent crons)
- [ ] `infra/shared/modules/bigquery/`: datasets cho Iceberg catalog
- [ ] `infra/shared/modules/iam/`: service account + roles (storage, bigquery, pubsub, run.invoker)
- [ ] `infra/dev/main.tf`: wire shared modules với dev values (smaller resources, separate bucket/project nếu cần)
- [ ] `infra/dev/terraform.tfvars` + `backend.tf`
- [ ] `infra/prod/main.tf`, `terraform.tfvars`, `backend.tf`
- [ ] `terraform plan` + `apply` cho dev — verify toàn bộ resources tạo đúng
- [ ] Document Terraform workflow trong README (`cd infra/dev && terraform apply`)

---

## Sprint 9 — Polish, Documentation, Production Readiness

**Mục tiêu:** Hardening, docs, sẵn sàng cho prod deploy.

- [ ] Code review toàn bộ `src/` — verify module independence (no circular/unintended cross-imports)
- [ ] Verify tất cả magic numbers đã move vào `conf/`
- [ ] Viết structured logging (`structlog`) cho tất cả 6 jobs — consistent log fields (job_name, shard_id, run_id, counts)
- [ ] Viết README.md: architecture diagram, setup instructions, how to run each job locally, Terraform deploy steps
- [ ] Run `ruff` + `mypy` toàn project, fix violations
- [ ] Viết `pytest` coverage report — đảm bảo core modules (`hashing`, `parquet`, `workers`) có test coverage
- [ ] Dry-run toàn bộ pipeline trên dev với real Riot API key, full cycle: A → B-Dist → B-Worker(x4) → C-Dist → C-Worker(x4) → D
- [ ] Review GCS storage cost/size sau full run — sanity check buffer sizes (4MB/1MB/32MB) phù hợp thực tế
- [ ] Deploy lên `infra/prod/`, run smoke test
