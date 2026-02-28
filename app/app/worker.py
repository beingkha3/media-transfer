import os
import time
import subprocess
import signal
import re
from urllib.parse import urljoin

import redis

ZURG_ROOT = os.environ.get("ZURG_ROOT", "/zurg")
MOVIES_DEST = os.environ.get("MOVIES_DEST", "/dest/movies")
SHOWS_DEST = os.environ.get("SHOWS_DEST", "/dest/series")
GDRIVE_MOVIES_DEST = os.environ.get("GDRIVE_MOVIES_DEST", "/gdrive/movies")
GDRIVE_SHOWS_DEST = os.environ.get("GDRIVE_SHOWS_DEST", "/gdrive/series")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

JELLYFIN_REFRESH_ENABLED = os.environ.get("JELLYFIN_REFRESH_ENABLED", "false").lower() == "true"
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "http://jellyfin:8096")
JELLYFIN_API_KEY = os.environ.get("JELLYFIN_API_KEY", "")

PROGRESS_MODE = os.environ.get("PROGRESS_MODE", "rsync").lower()  # rsync | poll | both

r = redis.from_url(REDIS_URL, decode_responses=True)

PCT_RE = re.compile(r"(\d{1,3})%")  # from rsync progress2

def safe_abs_under(root: str, rel: str) -> str:
    rel = rel.strip().lstrip("/")
    norm = os.path.normpath(rel)
    if norm.startswith("..") or "/.." in norm:
        raise ValueError("Path traversal")
    abs_path = os.path.normpath(os.path.join(root, norm))
    root_norm = os.path.normpath(root)
    if os.path.commonpath([abs_path, root_norm]) != root_norm:
        raise ValueError("Escapes root")
    return abs_path

def set_job(job_id: str, **fields):
    fields["updated_at"] = int(time.time())
    r.hset(f"job:{job_id}", mapping=fields)

def get_job(job_id: str):
    return r.hgetall(f"job:{job_id}")

def cancel_requested(job_id: str) -> bool:
    v = r.hget(f"job:{job_id}", "cancel_requested")
    return str(v) == "1"

def dest_for(job):
    media_type = job.get("media_type", "show")
    destination = (job.get("destination") or "local").lower()

    if destination == "gdrive":
        return GDRIVE_MOVIES_DEST if media_type == "movie" else GDRIVE_SHOWS_DEST

    return MOVIES_DEST if media_type == "movie" else SHOWS_DEST

def file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return 0


def progress_fields(downloaded: int, total: int, started_at: int):
    downloaded = max(0, downloaded)
    total = max(0, total)

    # Keep copying state below 100 until rename/finalize completes.
    raw_pct = int((downloaded / total) * 100) if total > 0 else 0
    progress_pct = min(99, max(0, raw_pct)) if total > 0 else 0

    elapsed = max(1, int(time.time()) - int(started_at or int(time.time())))
    speed_bps = int(downloaded / elapsed) if downloaded > 0 else 0
    remaining = max(0, total - downloaded)
    eta_seconds = int(remaining / speed_bps) if speed_bps > 0 else -1

    return {
        "downloaded_bytes": min(downloaded, total) if total > 0 else downloaded,
        "source_size_bytes": total,
        "progress_pct": progress_pct,
        "speed_bps": speed_bps,
        "eta_seconds": eta_seconds,
    }


def process_state(pid: int) -> str:
    if not pid:
        return ""
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("State:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return parts[1]
                    return ""
    except Exception:
        return ""
    return ""

def refresh_jellyfin():
    if not (JELLYFIN_REFRESH_ENABLED and JELLYFIN_API_KEY):
        return
    try:
        import urllib.request
        req = urllib.request.Request(
            urljoin(JELLYFIN_URL, f"/Library/Refresh?api_key={JELLYFIN_API_KEY}"),
            method="POST",
        )
        urllib.request.urlopen(req, timeout=15).read()
    except Exception:
        # don't fail the job just because refresh failed
        pass

def rsync_with_progress(job_id: str, src_abs: str, tmp_abs: str, total: int):
    """
    Runs rsync and parses --info=progress2 stdout for percentage.
    Also optionally file-polls for comparison/fallback.
    """
    cmd = [
        "rsync",
        "-a",
        "--partial",
        "--append-verify",
        "--no-perms",
        "--no-owner",
        "--no-group",
        "--info=progress2",
        src_abs,
        tmp_abs,
    ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )
    set_job(job_id, pid=str(proc.pid))
    job_state = get_job(job_id)
    started_at = int(job_state.get("started_at") or int(time.time()))

    last_poll = 0
    last_pct = 0
    full_copy_since = 0

    try:
        while True:
            if cancel_requested(job_id):
                copied_now = file_size(tmp_abs)
                set_job(job_id, status="cancelling", message="Stopping rsync...")
                try:
                    proc.send_signal(signal.SIGTERM)
                    time.sleep(1)
                    if proc.poll() is None:
                        proc.kill()
                except Exception:
                    pass
                set_job(
                    job_id,
                    status="cancelled",
                    message="Cancelled by user",
                    **progress_fields(copied_now, total, started_at),
                )
                return False

            line = proc.stdout.readline() if proc.stdout else ""
            if line:
                m = PCT_RE.search(line)
                if m:
                    pct = min(100, int(m.group(1)))
                    last_pct = max(last_pct, pct)
                    est_copied = int((last_pct / 100) * total) if total > 0 else 0
                    if total > 0 and est_copied >= total:
                        full_copy_since = full_copy_since or int(time.time())
                        proc_state = process_state(proc.pid)
                        wait_msg = "Finalizing destination writes..."
                        if full_copy_since and (int(time.time()) - full_copy_since) >= 60 and proc_state == "D":
                            wait_msg = "Finalizing on destination (I/O wait on mount)..."
                        set_job(
                            job_id,
                            status="finalizing",
                            **progress_fields(est_copied, total, started_at),
                            message=wait_msg,
                        )
                        continue
                    set_job(
                        job_id,
                        status="copying",
                        **progress_fields(est_copied, total, started_at),
                        message=f"rsync progress: {last_pct}%",
                    )

            # optional polling mode
            now = time.time()
            if PROGRESS_MODE in ("poll", "both") and (now - last_poll) >= 2:
                copied = file_size(tmp_abs)
                poll_pct = int((copied / total) * 100) if total > 0 else 0
                poll_pct = min(100, poll_pct)
                last_poll = now
                if total > 0 and copied >= total:
                    full_copy_since = full_copy_since or int(now)
                    proc_state = process_state(proc.pid)
                    wait_msg = "Finalizing destination writes..."
                    if full_copy_since and (int(now) - full_copy_since) >= 60 and proc_state == "D":
                        wait_msg = "Finalizing on destination (I/O wait on mount)..."
                    set_job(
                        job_id,
                        status="finalizing",
                        **progress_fields(copied, total, started_at),
                        message=wait_msg,
                    )
                    continue
                if PROGRESS_MODE == "poll":
                    last_pct = max(last_pct, poll_pct)
                    set_job(
                        job_id,
                        status="copying",
                        **progress_fields(copied, total, started_at),
                        message=f"poll progress: {last_pct}%",
                    )
                elif PROGRESS_MODE == "both":
                    # only update pct if rsync parsing is not moving
                    if poll_pct > last_pct:
                        last_pct = poll_pct
                        set_job(
                            job_id,
                            status="copying",
                            **progress_fields(copied, total, started_at),
                            message=f"poll fallback: {last_pct}%",
                        )
                    else:
                        set_job(job_id, **progress_fields(copied, total, started_at))

            ret = proc.poll()
            if ret is not None:
                out, err = proc.communicate(timeout=2)
                if ret != 0:
                    copied_now = file_size(tmp_abs)
                    set_job(
                        job_id,
                        status="error",
                        **progress_fields(copied_now, total, started_at),
                        message=f"rsync failed: {(err or out or '')[-300:]}",
                    )
                    return False
                return True

    finally:
        # clear pid
        set_job(job_id, pid="")

def run_copy(job_id: str):
    job = get_job(job_id)
    if not job:
        return

    src_rel = job["source_rel_path"]
    src_abs = safe_abs_under(ZURG_ROOT, src_rel)

    if not os.path.isfile(src_abs):
        set_job(job_id, status="error", message="Source file missing")
        return

    if cancel_requested(job_id):
        set_job(job_id, status="cancelled", message="Cancelled before start", progress_pct=0)
        return

    dest_root = dest_for(job)

    # Keep same relative structure under movies/shows
    parts = src_rel.split("/", 1)
    dest_rel = parts[1] if len(parts) == 2 else os.path.basename(src_rel)

    dest_abs = os.path.normpath(os.path.join(dest_root, dest_rel))
    os.makedirs(os.path.dirname(dest_abs), exist_ok=True)

    total = file_size(src_abs)
    started_at = int(time.time())
    set_job(job_id, source_size_bytes=total, started_at=started_at, speed_bps=0, eta_seconds=-1)

    # If already exists & same size, skip
    if os.path.exists(dest_abs) and file_size(dest_abs) == total and total > 0:
        set_job(job_id, status="done", progress_pct=100, downloaded_bytes=total, speed_bps=0, eta_seconds=0, message="Already present (skipped)")
        refresh_jellyfin()
        return

    tmp_abs = dest_abs + ".partial"
    set_job(job_id, dest_abs=dest_abs, tmp_abs=tmp_abs)

    set_job(job_id, status="copying", progress_pct=0, downloaded_bytes=0, speed_bps=0, eta_seconds=-1, message="Starting rsync...")

    ok = rsync_with_progress(job_id, src_abs, tmp_abs, total)
    if not ok:
        return

    if cancel_requested(job_id):
        set_job(job_id, status="cancelled", message="Cancelled at end", **progress_fields(file_size(tmp_abs), total, started_at))
        return

    set_job(job_id, status="finalizing", progress_pct=99, downloaded_bytes=total, speed_bps=0, eta_seconds=0, message="Finalizing file...")

    # Rename to final
    os.replace(tmp_abs, dest_abs)

    set_job(job_id, status="done", progress_pct=100, downloaded_bytes=total, speed_bps=0, eta_seconds=0, message="Completed")
    refresh_jellyfin()

def main():
    print("Worker started. Waiting for jobs...")
    while True:
        job_id = r.rpop("queue:jobs")
        if not job_id:
            time.sleep(1)
            continue

        job = get_job(job_id)
        if not job:
            continue

        # If cancelled while queued
        if cancel_requested(job_id):
            set_job(job_id, status="cancelled", message="Cancelled while queued", progress_pct=0)
            continue

        try:
            set_job(job_id, status="running", message="Picked up by worker", progress_pct=0)
            run_copy(job_id)
        except Exception as e:
            set_job(job_id, status="error", message=str(e), pid="")

if __name__ == "__main__":
    main()
