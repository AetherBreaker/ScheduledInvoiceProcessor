# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.14.2
FROM python:${PYTHON_VERSION}-slim AS base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

ENV PYTHONOPTIMIZE=1

WORKDIR /app

# Create a non-privileged user that the app will run under.
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Install gosu for dropping privileges in entrypoint
RUN apt-get update && apt-get install -y --no-install-recommends gosu && \
    rm -rf /var/lib/apt/lists/*

# Create directories for runtime data with proper permissions
RUN mkdir -p /app/src/logs /app/src/queue_backups && \
    chown -R appuser:appuser /app

# Download dependencies as a separate step to take advantage of Docker's caching.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Copy the source code into the container.
COPY --chown=appuser:appuser ./src ./src

# Copy entrypoint script (runs as root to fix volume permissions, then drops to appuser)
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set PYTHONPATH so imports work correctly
ENV PYTHONPATH=/app/src

# Health check - verifies heartbeat file was updated within last 3 minutes
HEALTHCHECK --interval=60s --timeout=10s --retries=3 --start-period=120s \
    CMD python3 -c "from pathlib import Path; from datetime import datetime, timedelta; hb = Path('/app/src/logs/heartbeat.txt'); exit(0 if hb.exists() and datetime.fromisoformat(hb.read_text().strip()) > (datetime.now() - timedelta(minutes=3)) else 1)"

# Run the application.
WORKDIR /app/src
ENTRYPOINT ["/app/entrypoint.sh"]
