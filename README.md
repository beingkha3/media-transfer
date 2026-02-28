# Media Transfer

Web UI + worker pipeline to browse a Zurg mount and copy selected media into destination storage (Local or GDrive), with Redis-backed job tracking.

## Features

- Browse Zurg files/folders from the web UI.
- Client-side search, clear button, result filters, and sorting.
- Enqueue copy jobs with media type:
  - `auto` (detect from `movies/` or `shows/` path)
  - forced `movie` or `show`
- Per-job destination selection:
  - `local`
  - `gdrive`
- Job controls: cancel, retry, delete.
- Progress details in Jobs:
  - percent
  - total size / downloaded size
  - speed and ETA
- Safe copy flow using `.partial` file + final rename.
- Optional Jellyfin library refresh after successful copy.

---

## Architecture

- `app` (FastAPI): UI + API + Redis job enqueue/management.
- `worker` (Python): consumes jobs from Redis and runs `rsync`.
- `redis`: queue + job state storage.

Main routes:

- `GET /` UI
- `GET /api/browse?path=...`
- `POST /api/enqueue`
- `POST /api/cancel/{job_id}`
- `POST /api/retry/{job_id}`
- `POST /api/delete/{job_id}`
- `GET /api/jobs`

---

## Requirements

- Docker + Docker Compose
- Host mounts available:
  - Zurg source (read)
  - local destination paths
  - gdrive destination paths (optional)

---

## Configuration

Current setup is in [docker-compose.yml](docker-compose.yml).

### Important environment variables

- `ZURG_ROOT` source mount root inside container (default `/zurg`)
- `MOVIES_DEST` local movies destination
- `SHOWS_DEST` local shows destination
- `GDRIVE_MOVIES_DEST` gdrive movies destination
- `GDRIVE_SHOWS_DEST` gdrive shows destination
- `REDIS_URL` Redis connection string
- `BASIC_AUTH_USER` / `BASIC_AUTH_PASS` UI/API basic auth
- `PROGRESS_MODE` `rsync`, `poll`, or `both`
- `JELLYFIN_REFRESH_ENABLED` `true|false`
- `JELLYFIN_URL`
- `JELLYFIN_API_KEY`

### Security note

Do not keep plaintext secrets in compose for production. Prefer `.env` and variable substitution.

---

## Run

From project root:

```bash
docker compose up -d --build
```

Check status/logs:

```bash
docker compose ps
docker compose logs -f app
docker compose logs -f worker
```

Stop:

```bash
docker compose down
```

---

## How to use

1. Open the UI (service behind your reverse proxy / mapped port).
2. Browse `movies` or `shows` under Zurg.
3. Pick destination (`Local` or `GDrive`).
4. Click **Download** on a file.
5. Track progress in **Jobs**.
6. Use **Retry** for failed/cancelled jobs.

---

## Job status meanings

- `queued`: waiting for worker.
- `running`: worker picked job.
- `copying`: rsync actively copying.
- `finalizing`: bytes copied; destination filesystem still flushing/closing/renaming.
- `done`: completed.
- `cancel_requested` / `cancelling` / `cancelled`.
- `error`: copy failed.

---

## Troubleshooting

### Stuck at 99% / finalizing on GDrive

This can happen when the destination mount is slow and rsync waits in kernel I/O state.

What to do safely:

1. Wait a bit (mount may still flush).
2. If it remains stuck for a long time:
   - stop worker: `docker compose stop worker`
   - fix/remount destination on host
   - start worker: `docker compose up -d worker`
   - click **Retry** in UI

Why this is safe:

- Transfers write to `*.partial` first.
- Final filename is only created after successful finalize/rename.
- Retry resumes and verifies with rsync (`--append-verify`).

### Basic auth issues

- Ensure `BASIC_AUTH_USER` and `BASIC_AUTH_PASS` are set consistently.
- If empty, auth check is effectively bypassed.

### No jobs moving

- Check Redis and worker:
  - `docker compose ps`
  - `docker compose logs -f worker`

---

## Project structure

- [docker-compose.yml](docker-compose.yml)
- [app/Dockerfile](app/Dockerfile)
- [app/requirements.txt](app/requirements.txt)
- [app/app/main.py](app/app/main.py)
- [app/app/worker.py](app/app/worker.py)
- [data/](data/)

---

## Notes

- Worker currently processes jobs sequentially (single queue consumer).
- Hidden files/folders are skipped in browse listing.
- Path traversal protections are enforced for source browsing/enqueue.
