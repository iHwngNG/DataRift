# 05 — Job C: Match Data Ingestion

Job C gồm 2 thành phần: **Job C-Distributor** và **Job C Worker** (4 shard instances), workflow đối xứng với Job B.

---

## 5.1 Job C-Distributor

### Purpose

Đọc toàn bộ match IDs từ `./matchID/`, lọc những `is_ingested = 0`, shard hóa, ghi vào `workspace/matchid/{shard_id}/`, trigger Job C Workers.

### Trigger

Cloud Scheduler — independent cron (e.g. mỗi 6h, lệch giờ với Job B-Distributor để tránh chồng load).

### Process

1. List & read tất cả Parquet files dưới `./matchID/**`.
2. Extract `(match_id, is_ingested, region, platform)` records.
3. Convert mỗi `match_id` → integer via `src/hashing/string_to_int.py`.
4. Apply `src/hashing/shard.py`: `shard_id = hash(match_id_int) % 4`.
5. Group theo `shard_id`, ghi mỗi group → `workspace/matchid/{shard_id}/data.parquet`.
6. Publish 4 Pub/Sub messages tới topic trigger Job C Worker, payload chứa `shard_id`.

> **Note:** Distributor ghi **toàn bộ** records (cả `is_ingested=0` và `=1`) vào shard file để giữ shard file là full mirror; Worker tự filter `is_ingested=0` khi xử lý. Điều này giúp lần distribute tiếp theo không cần re-merge state.

### Output

- `workspace/matchid/{0..3}/data.parquet`

---

## 5.2 Job C Worker

### Purpose

Với mỗi shard, fetch match detail data cho các `match_id` có `is_ingested=0`, ghi kết quả vào `./match/`, update `is_ingested=1` trong shard file và trong file gốc `./matchID/`.

### Trigger

Pub/Sub message từ Job C-Distributor, payload: `{"shard_id": <int>}`.

### Input

- `workspace/matchid/{shard_id}/data.parquet`

### Process

1. Đọc `workspace/matchid/{shard_id}/data.parquet` vào memory.
2. Filter records: `is_ingested == 0`.
3. Push filtered match_id records vào `queue.Queue`.
4. Spawn thread pool (size từ `conf/`), mỗi thread consume từ queue, gọi Match-V5 detail endpoint cho `match_id`.
5. Parse response → extract `game_start_timestamp` (unix ms) → derive `year/month/date`.
6. Buffer record vào PyArrow Table qua `src/parquet/buffer.py`, **partitioned by `(region, platform, year, month, date)`** — buffer riêng cho mỗi partition combination xuất hiện trong batch.
7. Khi 1 partition buffer đạt **32MB**, flush → write Parquet → `./match/{region}/{platform}/{year}/{month}/{date}/*.parquet`.
8. Sau khi 1 match_id hoàn tất: mark `is_ingested = 1` cho record đó trong in-memory shard table.
9. Cuối job:
   - Flush toàn bộ partition buffers còn lại.
   - Ghi đè `workspace/matchid/{shard_id}/data.parquet` với updated `is_ingested` values.
   - Update tương ứng `is_ingested` trong file gốc `./matchID/{region}/{platform}/{puuid}/*.parquet` (xem 5.3).

### 5.3 Updating `is_ingested` in Source `matchID/` Files

Vì shard file (`workspace/matchid/{shard_id}/`) là derived copy, sau khi Worker hoàn tất cần propagate `is_ingested=1` ngược lại file gốc trong `./matchID/{region}/{platform}/{puuid}/`.

**Approach:** Worker giữ một in-memory mapping `match_id → (region, platform, puuid, source_file_path)` được build lúc đọc shard (shard file cần chứa thêm cột `puuid` và `source_file_path` để traceability — xem note dưới). Cuối job, group updated match_ids theo `source_file_path`, rewrite từng file gốc tương ứng (overwrite Parquet với `is_ingested` updated).

> **Implementation note:** Để Job C-Distributor có thể populate `source_file_path` (hoặc tối thiểu `puuid`) khi build shard file, Distributor cần đọc `matchID/` theo từng file riêng (không gộp toàn bộ thành 1 table mất context path). Shard file schema do đó nên bao gồm thêm cột `puuid` (đủ để reconstruct path `./matchID/{region}/{platform}/{puuid}/`).

### Output

- `./match/{region}/{platform}/{year}/{month}/{date}/*.parquet`
- `workspace/matchid/{shard_id}/data.parquet` (updated `is_ingested`)
- `./matchID/{region}/{platform}/{puuid}/*.parquet` (updated `is_ingested`)

### Config Keys

```yaml
job_c:
  shard_count: 4
  thread_pool_size: 8
  buffer_flush_mb: 32
```
