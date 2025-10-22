# Docker Deployment Guide

## How it works

The architecture is pretty straightforward:

- Secrets live in `./secrets/` on your machine and get mounted into the container at runtime
- Logs write to `./logs/` so they persist even if you rebuild the container
- Queue backups go to `./queue_backups/` for the same reason
- No credentials ever get baked into the Docker image

## Requirements

- Docker Engine 20.10 or newer
- Docker Compose 1.27 or newer

## Getting started

### Setting up secrets

You need three credential files in a `secrets/` directory:

```bash
mkdir -p secrets
```

Then add these files:

- `db-key.json` - Your Google Service Account credentials
- `sas_ftp_creds.json` - SAS FTP login info
- `sft_creds.json` - SFT website FTP credentials

There's a helper script that does this automatically if you already have the files in your project root:

```bash
chmod +x docker-setup.sh
./docker-setup.sh
```

### Running the application

Build and start:

```bash
docker-compose build
docker-compose up -d
```

Watch the logs:

```bash
docker-compose logs -f
```

### Verifying everything works

Check the container status:

```bash
docker-compose ps
```

The logs should show the scheduler starting up and jobs being added. You can also check the log files directly:

```bash
tail -f logs/ScheduledOrderMiddleman.log
tail -f logs/ScheduledOrderMiddleman_debug.log
```

## Day-to-day usage

### Starting and stopping

```bash
# Start everything
docker-compose up -d

# Stop everything
docker-compose down

# Restart without rebuilding
docker-compose restart
```

### Checking logs

```bash
# All logs
docker-compose logs

# Follow live output
docker-compose logs -f

# Just the last 100 lines
docker-compose logs --tail=100
```

### Updating code

When you pull new changes:

```bash
git pull
docker-compose up -d --build
```

## Project layout

```
.
├── docker-compose.yml       # Main config file
├── Dockerfile               # Defines the container
├── secrets/                 # Your credentials (never committed)
│   ├── db-key.json
│   ├── sas_ftp_creds.json
│   └── sft_creds.json
├── logs/                    # App writes logs here
├── queue_backups/           # Queue state saves here
└── src/                     # Application code
```

## Security stuff

### What's protected

The setup keeps your secrets safe in a few ways:

- Secrets stay in `./secrets/` on your machine, not in the image
- `.dockerignore` prevents them from being copied during builds
- `.gitignore` keeps them out of version control
- Secrets get mounted read-only at `/run/secrets/` in the container
- The container runs as a non-root user called `appuser`

### Important notes

Don't commit anything from `secrets/` to git. It's already in `.gitignore`, but worth repeating.

Lock down the secrets directory:

```bash
chmod 700 secrets/
```

Make sure you have backups of these files somewhere secure outside the repo.

## Common issues

### "Google API key file not found"

The app can't find your secrets. Check that they exist:

```bash
ls -la secrets/
```

You should see all three JSON files. If not, run `./docker-setup.sh` or create them manually.

### Permission errors

Sometimes Docker has issues with file permissions:

```bash
chmod 700 secrets/
chmod -R 777 logs/
chmod -R 777 queue_backups/
```

### Container exits immediately

Check what went wrong:

```bash
docker-compose logs scheduler
```

Usually it's either missing secrets or a bad config value.

### Changing secrets

If you need to update credential files while the app is running:

```bash
# Update the file in secrets/
docker-compose restart scheduler
```

The new secrets will be picked up on restart.

## Configuration

Environment variables get set in `docker-compose.yml`:

```yaml
environment:
  - TZ=America/New_York # Your timezone
  - GOOGLE_API_KEY_FILE=/run/secrets/db_key # Google API creds
  - SAS_FTP_CREDS_FILE=/run/secrets/sas_ftp_creds # SAS FTP login
  - SFT_WEBSITE_CREDS_FILE=/run/secrets/sft_creds # SFT FTP login
```

You can override database settings in the same file if needed (lines 28-33 in docker-compose.yml).

## Production considerations

If you're deploying this for real, consider:

1. **Secret management** - Look into Docker Swarm secrets, AWS Secrets Manager, or HashiCorp Vault instead of plain files
2. **Monitoring** - Add proper healthchecks and hook it up to Prometheus/Grafana
3. **Logging** - Ship logs to something centralized like ELK or Splunk
4. **Backups** - Set up automated backups of the `queue_backups/` directory
5. **Resource limits** - Prevent the container from eating all your memory

Example resource limits:

```yaml
services:
  scheduler:
    # ...
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.5'
          memory: 256M
```

That should cover most use cases. Check `docker-compose.yml` and `Dockerfile` if you need to tweak anything else.
