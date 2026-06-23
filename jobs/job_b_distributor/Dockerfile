# ============================================================
# Stage 1: Builder - compile and install dependencies
# ============================================================
FROM python:3.14-slim AS builder

# Install build dependencies for compiling native extensions (pyarrow)
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Create minimal README.md required by pyproject.toml readme field
RUN echo "# DataRift" > README.md

# Copy dependency manifests first for better layer caching
COPY pyproject.toml ./

# Install dependencies (build tools not needed at runtime)
RUN pip install --no-cache-dir --user .

# Copy source code
COPY src/ ./src/

# Install the datarift package in editable mode
RUN pip install --no-cache-dir --user -e .

# ============================================================
# Stage 2: Runtime - minimal production image
# ============================================================
FROM python:3.14-slim AS runtime

# Create non-root user for security
RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/false --create-home appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /root/.local /home/appuser/.local

# Copy application code and config
COPY --chown=appuser:appgroup jobs/ ./jobs/
COPY --chown=appuser:appgroup conf/ ./conf/

# Packages are already installed from builder stage via /root/.local
# No need to reinstall - just ensure PYTHONPATH is set correctly

# Set environment variables for Cloud Run
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/usr/local/lib/python3.14/site-packages

# Switch to non-root user
USER appuser

# Health check for Cloud Run
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import datarift; print('ok')" || exit 1

# Entry point - Cloud Run Jobs will invoke this
ENTRYPOINT ["python", "jobs/job_b_distributor/main.py"]
