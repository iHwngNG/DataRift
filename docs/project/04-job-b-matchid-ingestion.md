# 04 — Job B: Match ID Ingestion

Job B gồm 2 thành phần: **Job B-Distributor** (single job, sharding) và **Job B Worker** (parameterized, 4 shard instances).

---

## 4.1 Job B-Distributor

### Purpose

Đọc toàn bộ PUUIDs từ `./league/`, shard hóa theo hash, ghi shard files vào `workspace/puuid/{shard_id}/`, trigger Job B Workers.

### Trigger

Cloud Scheduler — independent cron, chạy trước Job B Workers theo schedule cố định (e.g. mỗi 6h).

### Process

1. List & read tất cả Parquet files dưới `./league/**`.
2. Extract distinct `(puuid, region, platform)` tuples.
3. **First-run consideration:** Nếu `puuid` đã tồn tại trong `workspace/puuid/{any_shard}/` từ lần chạy trước, giữ lại `last_read` hiện có (merge, không reset về NULL). PUUID mới → `last_read = NULL`.
4. Convert mỗi `puuid` → integer via `src/hashing/string_to_int.py`.
5. Apply `src/hashing/shard.py`: `shard_id = hash(puuid_int) % 4`.
6. Group records theo `shard_id`, ghi mỗi group → `workspace/puuid/{shard_id}/data.parquet` (schema: `puuid, last_read, region, platform`).
7. Publish 4 Pub/Sub messages (1 per shard) tới topic trigger Job B Worker, payload chứa `shard_id`.

### Output

- `workspace/puuid/{0..3}/data.parquet`

---

## 4.2 Job B Worker

### Purpose

Với mỗi shard, fetch match ID list cho các PUUID cần update (`last_read = NULL` hoặc `last_read < today`), ghi kết quả vào `./matchID/`, update `last_read` trong shard file.

### Trigger

Pub/Sub message từ Job B-Distributor, payload: `{"shard_id": <int>}`. Cloud Run Job đọc `SHARD_ID` từ env var (set bởi Pub/Sub-triggered execution).

### Input

- `workspace/puuid/{shard_id}/data.parquet`

### Process

1. Đọc `workspace/puuid/{shard_id}/data.parquet` vào memory.
2. Filter records: `last_read IS NULL OR last_read < today`. Skip records với `last_read == today` (đã xử lý trong ngày).
3. Push filtered PUUID records vào `queue.Queue`.
4. Spawn thread pool (size từ `conf/`), mỗi thread consume từ queue và thực hiện **fetch logic** (xem 4.3).
5. Mỗi thread output: list of `(match_id, puuid, region, platform)` → buffer vào PyArrow Table qua `src/parquet/buffer.py`.
6. Khi buffer đạt **1MB**, flush → write Parquet → `./matchID/{region}/{platform}/{puuid}/*.parquet` (schema includes `is_ingested=0` default).
7. Sau khi 1 PUUID hoàn tất xử lý: update `last_read = today` cho record đó trong in-memory shard table.
8. Cuối job: flush buffer còn lại; ghi đè `workspace/puuid/{shard_id}/data.parquet` với updated `last_read` values.

### 4.3 Fetch Logic per PUUID

**Case A — `last_read IS NULL`** (chưa từng fetch):
- Pagination từ `start=0`, `count=100`, tăng dần `start` (0, 100, 200, ...)
- Dừng khi: (a) response trả về 0 match IDs, hoặc (b) tổng số match IDs đã lấy đạt **1000**.

**Case B — `last_read < today`** (incremental fetch):
- Dùng Riot API param `startTime = last_read` (converted to unix timestamp) để narrow range — chỉ lấy match IDs từ sau `last_read`.
- Vẫn pagination với `count=100`, tăng `start` dần.
- **Early-stop dedup:** trước khi append match_id vào output, check xem match_id đã tồn tại trong `./matchID/{region}/{platform}/{puuid}/` chưa (load existing match_ids cho puuid này vào set trước khi bắt đầu fetch). Nếu gặp match_id đã tồn tại → dừng pagination cho puuid này (matches đã sorted theo thời gian giảm dần, nên gặp 1 cái cũ = phần còn lại cũng cũ).
- **Hard cap:** dừng nếu đã lấy đủ **1000 match IDs mới** trong run này, dù chưa early-stop.

### Output

- `./matchID/{region}/{platform}/{puuid}/*.parquet` (new match IDs, `is_ingested=0`)
- `workspace/puuid/{shard_id}/data.parquet` (updated `last_read`)

### Config Keys

```yaml
job_b:
  shard_count: 4
  thread_pool_size: 8
  buffer_flush_mb: 1
  pagination_count: 100
  max_match_ids_per_puuid_first_run: 1000
  max_match_ids_per_puuid_incremental: 1000
```
