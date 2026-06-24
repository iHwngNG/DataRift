# ============================================================
# Stage 1: Builder - compile and install dependencies
# ============================================================
FROM python:3.14-slim AS builder

# Install build dependencies for compiling native extensions
RUN apt-get update && apt-get install --no-install-recommends -y build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy dependency manifests first for better layer caching
COPY pyproject.toml ./

# Create minimal README.md required by pyproject.toml readme field
RUN echo "# DataRift" > README.md

# Copy source code - hatchling expects packages = ["src/datarift"]
COPY src/ ./src/

# Build the datarift package as a wheel - install build (PEP 517 wrapper) and hatchling (build-backend)
RUN pip install --no-cache-dir build hatchling && python -m build --wheel --outdir /dist .

# ============================================================
# Stage 2: Runtime - minimal production image
# ============================================================
FROM python:3.14-slim AS runtime

# Install runtime dependencies including build tools for pyiceberg C extension
RUN apt-get update && apt-get install --no-install-recommends -y build-essential && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd --gid 1000 appgroup && useradd --uid 1000 --gid appgroup --shell /bin/false --create-home appuser

WORKDIR /app

# Copy the wheel from builder and install it
COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

# Copy application code and config
COPY --chown=appuser:appgroup jobs/ ./jobs/
COPY --chown=appuser:appgroup conf/ ./conf/

# Set environment variables for Cloud Run
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Switch to non-root user
USER appuser

# Health check for Cloud Run
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import datarift; print('ok')" || exit 1

# Entry point - Cloud Run Jobs will invoke this
ENTRYPOINT ["python", "jobs/job_c_worker/main.py"]
