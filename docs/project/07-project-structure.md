# 07 — Project Structure

## 7.1 Repository Layout

```
datarift/
├── pyproject.toml                  # project-wide config (deps, tooling, ruff/mypy etc.)
├── README.md
│
├── conf/                           # all tunable settings
│   ├── base.yaml                   # shared defaults
│   ├── job_a.yaml
│   ├── job_b.yaml
│   ├── job_c.yaml
│   ├── job_d.yaml
│   └── platforms.yaml              # region/platform/tier/division definitions
│
├── src/                            # ALL reusable, independent functions/modules
│   └── datarift/
│       ├── __init__.py
│       ├── config/
│       │   ├── __init__.py
│       │   └── loader.py           # loads conf/*.yaml into typed config objects
│       │
│       ├── riot_client/
│       │   ├── __init__.py
│       │   ├── client.py           # async HTTP client, rate limiting, retry
│       │   ├── league_api.py       # League-Entries-V4 wrappers
│       │   ├── match_api.py        # Match-V5 (ids + detail) wrappers
│       │   └── regions.py          # platform <-> region (cluster) mapping
│       │
│       ├── hashing/
│       │   ├── __init__.py
│       │   ├── string_to_int.py    # deterministic string -> int conversion (for puuid/match_id)
│       │   └── shard.py            # hash function + shard assignment (mod N)
│       │
│       ├── parquet/
│       │   ├── __init__.py
│       │   ├── buffer.py           # in-memory PyArrow buffer with size-based flush
│       │   └── io.py               # GCS read/write helpers for Parquet
│       │
│       ├── gcs/
│       │   ├── __init__.py
│       │   └── paths.py            # path builders for all GCS paths (league/matchID/match/workspace/tmp/checkpoint)
│       │
│       ├── rate_limit/
│       │   ├── __init__.py
│       │   ├── checkpoint.py       # serialize/deserialize checkpoint state → GCS state/checkpoint/*
│       │   ├── tmp_buffer.py       # flush in-flight RAM buffer → GCS workspace/tmp/* ; restore on resume
│       │   └── scheduler.py        # schedule delayed Cloud Run Job re-execution via Cloud Tasks
│       │
│       ├── iceberg/
│       │   ├── __init__.py
│       │   ├── catalog.py          # BigQuery Iceberg catalog setup
│       │   └── sync.py             # add_files / snapshot registration logic
│       │
│       └── workers/
│           ├── __init__.py
│           ├── threaded_queue.py   # generic queue.Queue + thread pool runner với rate-limit detection
│           ├── puuid_fetcher.py    # Job B fetch logic (pagination, early-stop, cap)
│           └── match_fetcher.py    # Job C fetch logic
│
├── jobs/                           # deployable entrypoints — each uses src/ functions
│   ├── job_a_user_ingestion/
│   │   ├── Dockerfile
│   │   └── main.py
│   ├── job_b_distributor/
│   │   ├── Dockerfile
│   │   └── main.py
│   ├── job_b_worker/
│   │   ├── Dockerfile
│   │   └── main.py
│   ├── job_c_distributor/
│   │   ├── Dockerfile
│   │   └── main.py
│   ├── job_c_worker/
│   │   ├── Dockerfile
│   │   └── main.py
│   └── job_d_iceberg_sync/
│       ├── Dockerfile
│       └── main.py
│
└── infra/                           # Terraform IaC
    ├── shared/                      # common modules used by both dev & prod
    │   ├── modules/
    │   │   ├── gcs/
    │   │   ├── pubsub/
    │   │   ├── cloud_run_job/
    │   │   ├── cloud_tasks/         # Cloud Tasks queue cho delayed job re-execution
    │   │   ├── scheduler/
    │   │   ├── bigquery/
    │   │   └── iam/
    │   └── variables.tf
    │
    ├── dev/
    │   ├── main.tf                  # wires shared modules with dev-specific values
    │   ├── terraform.tfvars
    │   └── backend.tf
    │
    └── prod/
        ├── main.tf
        ├── terraform.tfvars
        └── backend.tf
```

## 7.2 Design Rules

1. **All reusable logic lives in `src/datarift/`.** No business logic in `jobs/*/main.py` beyond: load config → call `src/` functions → handle Pub/Sub payload / env vars.
2. **Each `jobs/*/main.py` is a thin entrypoint.** It imports from `src.datarift`, wires together the workflow for that specific job, and is independently containerized.
3. **Modules in `src/` are independent and composable** — e.g. `src/datarift/hashing/` has no dependency on `src/datarift/riot_client/`; `src/datarift/parquet/buffer.py` doesn't know about Riot-specific schemas.
4. **`conf/` is the single source of truth for tunables** (shard count, buffer sizes, thread pool sizes, platform/tier/division lists, pagination params, caps, Cloud Tasks queue name). No magic numbers hardcoded in `src/` or `jobs/`.
5. **`infra/shared/` contains all Terraform modules reused by dev & prod.** `infra/dev/` and `infra/prod/` only contain environment-specific wiring (variable values, backend state config) — no duplicated module code.
6. **Naming is descriptive** — e.g. `puuid_fetcher.py` not `fetcher.py`, `string_to_int.py` not `convert.py`, `tmp_buffer.py` not `temp.py`.
7. **Rate-limit handling is fully encapsulated in `src/datarift/rate_limit/`.** Jobs A/B/C không tự implement checkpoint hay scheduling logic — chúng chỉ gọi `rate_limit.*` functions. `workers/threaded_queue.py` detect khi tất cả threads bị 429 và delegate sang `rate_limit/` để xử lý.

## 7.3 `pyproject.toml` Responsibilities

- Project metadata (name: `datarift`, version, Python `>=3.14`)
- Dependencies (pyiceberg, pyarrow, google-cloud-storage, google-cloud-pubsub, google-cloud-bigquery, google-cloud-tasks, httpx, pydantic, tenacity, pyyaml, structlog)
- Dev dependencies (pytest, ruff, mypy)
- Tool configs (ruff, mypy, pytest) — single place for all project-wide tooling settings
- Each `jobs/*/` Dockerfile installs from root `pyproject.toml` (shared lockfile) — no per-job dependency duplication
