# parquet_writer Cloud Run Job
# ─────────────────────────────
# Consumes Riot league-entry records from Pub/Sub and writes them to GCS as
# snappy-compressed Parquet files.
#
# Built from the repository root context so that the shared config/ package
# and fetch_users pipeline modules (buffer.py, uploader.py) can be copied in.

FROM python:3.12-slim

# Prevent .pyc files and enable unbuffered stdout (important for Cloud Logging)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first to leverage Docker layer caching
COPY jobs/parquet_writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared config package
COPY config/ ./config/

# Reuse buffer.py and uploader.py from fetch_users (avoid code duplication)
COPY jobs/fetch_users/pipeline/buffer.py   ./pipeline/buffer.py
COPY jobs/fetch_users/pipeline/uploader.py ./pipeline/uploader.py

# Copy the writer entry point
COPY jobs/parquet_writer/main.py .

# Cloud Run Jobs invoke the container as a one-shot process
ENTRYPOINT ["python", "main.py"]
