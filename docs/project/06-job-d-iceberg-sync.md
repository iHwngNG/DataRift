# 06 — Job D: Iceberg Metadata Sync

## 6.1 Purpose

Sync Iceberg table metadata + catalog (BigQuery) từ toàn bộ GCS lakehouse data folders (`league/`, `matchID/`, `match/`). Đây là job duy nhất chịu trách nhiệm về Iceberg table state — các job A/B/C chỉ viết raw Parquet, không tự touch Iceberg metadata.

## 6.2 Trigger

Cloud Scheduler — daily cron, chạy sau khi các job A/B/C của ngày đã hoàn tất (e.g. cuối ngày UTC).

## 6.3 Iceberg Tables

| Iceberg Table | Source GCS Path | Partition Spec |
|---------------|------------------|-----------------|
| `datarift.league` | `./league/**` | `region, platform, tier, division` |
| `datarift.match_id` | `./matchID/**` | `region, platform` |
| `datarift.match` | `./match/**` | `region, platform, year, month` |

## 6.4 Catalog Configuration

- **Catalog backend:** BigQuery (Iceberg REST Catalog via BigQuery Metastore)
- **Metadata storage:** GCS, dưới `gs://datarift-lakehouse/iceberg/{table_name}/metadata/`
- **Data files:** Iceberg tables reference existing Parquet files đã ghi bởi Job A/B/C qua `add_files` (không rewrite data, chỉ register).

## 6.5 Process

1. Cho mỗi table trong 6.3:
   - Load (hoặc create nếu chưa tồn tại) Iceberg table qua `src/iceberg/catalog.py`.
   - Scan GCS path tương ứng để tìm các Parquet files mới (chưa có trong table's current snapshot) — so sánh file list với manifest hiện có.
   - Dùng `pyiceberg`'s `add_files` API để register các file mới vào table, tạo snapshot mới.
2. Update BigQuery catalog tables (External Tables hoặc BigLake tables) để reflect snapshot mới nhất — cho phép query trực tiếp từ BigQuery.
3. Log run summary: số file mới register per table, snapshot ID mới.

## 6.6 Idempotency

Job D phải idempotent — nếu chạy lại trong cùng ngày (hoặc retry sau failure), không được register duplicate files. Implementation dùng Iceberg's existing file-tracking (manifest list) để skip files đã registered.

## 6.7 Config Keys

```yaml
job_d:
  tables:
    - name: league
      gcs_path: league/
      partition_by: [region, platform, tier, division]
    - name: match_id
      gcs_path: matchID/
      partition_by: [region, platform]
    - name: match
      gcs_path: match/
      partition_by: [region, platform, year, month]
  catalog:
    type: bigquery
    warehouse: gs://datarift-lakehouse/iceberg
```
