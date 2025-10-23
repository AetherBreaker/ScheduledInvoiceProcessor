# Docker Quick Reference

## Commands you'll use

```bash
# Start the application
docker-compose up -d

# Stop it
docker-compose down

# Watch logs in real-time
docker-compose logs -f

# Restart (keeps current build)
docker-compose restart

# Rebuild and restart
docker-compose build && docker-compose up -d
```

## File structure

```
├── docker-compose.yml     # Main config
├── Dockerfile            # Container definition
├── DOCKER_README.md      # Full docs
├── docker-setup.sh       # Setup helper
├── secrets/              # Credentials (not in git)
│   ├── db-key.json
│   ├── sas_ftp_creds.json
│   └── sft_creds.json
├── logs/                 # App logs
└── queue_backups/        # Queue state
```

## Quick security check

Make sure these are true:
- Secrets aren't in the Docker image
- Secrets aren't in git
- Container runs as non-root user
- Secrets mounted read-only
- Logs persist on host

## Troubleshooting

**Container won't start?**
```bash
docker-compose logs
ls -la secrets/
```

**Need a clean rebuild?**
```bash
docker-compose build --no-cache
docker-compose up -d
```

**Check if it's running?**
```bash
docker ps | grep scheduled-order-middleman
docker stats scheduled-order-middleman
```

## What's actually running

The container handles:
- Google Sheets API integration
- Job scheduling via APScheduler
- Invoice pickup/dropoff processing
- Auto-restart if it crashes
- Writing logs to the host filesystem

## Current performance

Typical resource usage:
```
CPU: ~0.12%
Memory: ~84MB
Logs: ./logs/
Status: Should be up and processing jobs
```

If you need more details, check DOCKER_README.md.
