# fetch_users Cloud Run Job
# ─────────────────────────
# Single-stage build using a slim Python 3.12 base image.
# Python 3.12 is used for broad library compatibility (pyarrow, google-cloud-storage).
# The job is invoked by Cloud Run with environment variables injected at runtime.

FROM python:3.12-slim

# Prevent .pyc files and enable unbuffered stdout (important for Cloud Logging)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first to leverage Docker layer caching
COPY jobs/fetch_users/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source from the project root context
COPY config/ ./config/
COPY jobs/fetch_users/pipeline/ ./pipeline/
COPY jobs/fetch_users/main.py .

# Cloud Run Jobs invoke the container as a one-shot process
ENTRYPOINT ["python", "main.py"]
