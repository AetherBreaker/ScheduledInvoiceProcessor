#!/bin/sh
set -e

# Fix ownership of mounted volumes (bind mounts inherit host permissions)
chown -R appuser:appuser /app/src/logs /app/src/queue_backups 2>/dev/null || true
chown -R appuser:appuser /app/secrets 2>/dev/null || true

# Run the app
exec python3 __main__.py "$@"
