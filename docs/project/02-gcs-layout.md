# 02 — GCS Bucket Layout & Schema

**Bucket name:** `datarift-lakehouse`

## 2.1 Folder Structure

```
gs://datarift-lakehouse/
├── league/
│   └── {region}/{platform}/{tier}/{division}/*.parquet
│
├── matchID/
│   └── {region}/{platform}/{puuid}/*.parquet
│
├── match/
│   └── {region}/{platform}/{year}/{month}/{date}/*.parquet
│
├── workspace/
│   ├── puuid/
│   │   └── {shard_id}/*.parquet          # Job B shard files
│   └── matchid/
│       └── {shard_id}/*.parquet          # Job C shard files
│
└── iceberg/
    └── {table_name}/
        ├── data/
        └── metadata/
```

## 2.2 Path Variables

| Variable | Values | Source |
|----------|--------|--------|
| `region` | `asia`, `sea`, `europe` | Mapped from `platform` (cluster-level grouping for Match API) |
| `platform` | `vn2`, `kr`, `euw1`, `eun1` | Riot platform routing values |
| `tier` | `iron`, `bronze`, `silver`, `gold`, `platinum`, `emerald`, `diamond` | From League API response |
| `division` | `I`, `II`, `III`, `IV` | From League API response |
| `year`, `month`, `date` | Derived from `gameStartTimestamp` (unix ms) in match data | Parsed at write-time in Job C |

## 2.3 Region ↔ Platform Mapping

| Platform | Region (cluster) |
|----------|-------------------|
| `kr` | `asia` |
| `vn2` | `sea` |
| `euw1` | `europe` |
| `eun1` | `europe` |

## 2.4 Schema per Layer

### `league/` (Job A output)

| Column | Type | Description |
|--------|------|-------------|
| `puuid` | STRING | Player UUID |
| `summoner_id` | STRING | Encrypted summoner ID |
| `summoner_name` | STRING | Display name |
| `tier` | STRING | Rank tier |
| `division` | STRING | Rank division |
| `league_points` | INT | LP |
| `wins` | INT | |
| `losses` | INT | |
| `region` | STRING | |
| `platform` | STRING | |
| `_ingested_at` | TIMESTAMP | |

### `matchID/` (Job B output)

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | STRING | Riot match ID |
| `puuid` | STRING | Owning summoner PUUID |
| `is_ingested` | INT | `0` = chưa fetch match detail, `1` = đã fetch (updated by Job C) |
| `region` | STRING | |
| `platform` | STRING | |
| `_ingested_at` | TIMESTAMP | When match ID was discovered |

### `match/` (Job C output)

Match detail data — schema follows Riot Match-V5 response structure, flattened to columnar Parquet. Key fields:

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | STRING | |
| `game_start_timestamp` | BIGINT | Unix ms, used for partitioning |
| `game_duration` | LONG | |
| `game_version` | STRING | |
| `queue_id` | INT | |
| `participants` | STRING (JSON) | Raw participant array (flattening deferred to Iceberg/Silver stage) |
| `teams` | STRING (JSON) | Raw teams array |
| `region` | STRING | |
| `platform` | STRING | |
| `_ingested_at` | TIMESTAMP | |

### `workspace/puuid/{shard_id}/*` (Job B shard / state file)

| Column | Type | Description |
|--------|------|-------------|
| `puuid` | STRING | |
| `last_read` | DATE \| NULL | `NULL` = never fetched; otherwise date of last successful fetch |
| `region` | STRING | |
| `platform` | STRING | |

### `workspace/matchid/{shard_id}/*` (Job C shard / state file)

| Column | Type | Description |
|--------|------|-------------|
| `match_id` | STRING | |
| `is_ingested` | INT | `0` or `1` |
| `region` | STRING | |
| `platform` | STRING | |

## 2.5 Parquet Buffer Sizes (per job)

| Job | Buffer Flush Threshold |
|-----|------------------------|
| Job A | 4 MB |
| Job B | 1 MB |
| Job C | 32 MB |

All buffering done in-memory via PyArrow `Table` accumulation; flush writes a new Parquet file to the appropriate GCS path when threshold reached or job completes.
