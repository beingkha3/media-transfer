import os
import posixpath
import time
import uuid
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import redis

ZURG_ROOT = os.environ.get("ZURG_ROOT", "/zurg")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

BASIC_USER = os.environ.get("BASIC_AUTH_USER", "")
BASIC_PASS = os.environ.get("BASIC_AUTH_PASS", "")

r = redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="Media Transfer")


# -------------------------
# Basic Auth
# -------------------------
def _basic_auth_ok(request: Request) -> bool:
    if not BASIC_USER or not BASIC_PASS:
        return True
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("basic "):
        return False
    import base64
    try:
        raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        u, p = raw.split(":", 1)
        return u == BASIC_USER and p == BASIC_PASS
    except Exception:
        return False


async def require_auth(request: Request):
    if not _basic_auth_ok(request):
        return JSONResponse(
            {"error": "Unauthorized"},
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="media-transfer"'},
        )


# -------------------------
# Path safety helpers
# -------------------------
def safe_join_under_root(root: str, rel_path: str) -> str:
    rel_path = rel_path.strip().lstrip("/")
    norm = posixpath.normpath(rel_path)

    if norm.startswith("../") or norm == "..":
        raise ValueError("Path traversal blocked")

    abs_path = os.path.normpath(os.path.join(root, norm))
    root_norm = os.path.normpath(root)

    if os.path.commonpath([abs_path, root_norm]) != root_norm:
        raise ValueError("Path escapes root")

    return abs_path


def list_dir(root: str, rel_path: str):
    abs_path = safe_join_under_root(root, rel_path)

    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"Not found: {abs_path}")
    if not os.path.isdir(abs_path):
        raise NotADirectoryError(f"Not a directory: {abs_path}")

    entries = []
    with os.scandir(abs_path) as it:
        for e in it:
            if e.name.startswith("."):
                continue
            try:
                st = e.stat()
            except FileNotFoundError:
                continue
            entries.append(
                {
                    "name": e.name,
                    "is_dir": e.is_dir(),
                    "size": st.st_size if e.is_file() else None,
                    "mtime": int(st.st_mtime),
                }
            )

    entries.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
    return entries


def detect_media_type(source_rel_path: str) -> Optional[str]:
    p = source_rel_path.strip().lstrip("/")
    if p.startswith("shows/"):
        return "show"
    if p.startswith("movies/"):
        return "movie"
    return None


# -------------------------
# Models
# -------------------------
MediaType = Literal["movie", "show", "auto"]
DestinationType = Literal["local", "gdrive"]


class EnqueueBody(BaseModel):
    source_rel_path: str
    media_type: Optional[MediaType] = "auto"
    destination: Optional[DestinationType] = "local"


# -------------------------
# UI
# -------------------------
UI_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Media Transfer</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 18px; }
    .row { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
    input, select, button { padding:10px 12px; font-size:14px; }
    button { cursor:pointer; }
    .path { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; padding:8px 10px; background:#f4f4f4; border-radius:8px; }
    .grid { margin-top: 14px; border:1px solid #eee; border-radius:12px; overflow:hidden; }
    .item { display:flex; justify-content:space-between; gap:10px; padding:10px 12px; border-top:1px solid #eee; }
    .item:first-child { border-top:none; }
    .name { display:flex; gap:10px; align-items:center; }
    .badge { font-size:12px; padding:2px 8px; border:1px solid #ddd; border-radius:999px; }
    .muted { color:#666; font-size:12px; }
    .jobs { margin-top: 18px; }
    .job { border:1px solid #eee; border-radius:12px; padding:10px 12px; margin-top:10px; }
    .ok { color: #0a7; }
    .err { color: #c22; }
    .statusbar { margin-top:10px; padding:10px 12px; border:1px solid #eee; border-radius:12px; background:#fafafa; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  </style>
</head>
<body>
  <h2>Media Transfer</h2>

  <div class="row">
    <label>Browse:</label>
    <select id="rootSel">
      <option value="">/ (zurg root)</option>
      <option value="movies">movies</option>
      <option value="shows">shows</option>
    </select>

    <button onclick="goUp()">Up</button>

    <span class="path" id="pathView">/</span>
  </div>

  <div class="row" style="margin-top:12px;">
    <input id="search" placeholder="Search (client-side filter)" style="min-width:240px;" oninput="onSearchInput()" />
    <button id="clearSearchBtn" onclick="clearSearch()" title="Clear search" style="display:none;">x</button>
    <select id="resultFilterSel" onchange="render()">
      <option value="all">Results: All</option>
      <option value="files">Results: Files</option>
      <option value="dirs">Results: Folders</option>
    </select>
    <select id="resultSortSel" onchange="render()">
      <option value="name_asc">Sort: Name A→Z</option>
      <option value="name_desc">Sort: Name Z→A</option>
      <option value="size_asc">Sort: Size Small→Large</option>
      <option value="size_desc">Sort: Size Large→Small</option>
    </select>
    <select id="typeSel">
      <option value="auto">Auto-detect (recommended)</option>
      <option value="movie">Force Movie</option>
      <option value="show">Force Show</option>
    </select>
    <select id="destSel">
      <option value="local">Destination: Local</option>
      <option value="gdrive">Destination: GDrive</option>
    </select>
    <button onclick="refresh()">Refresh</button>
  </div>

  <div class="statusbar">
    <div><b>Status:</b> <span id="statusText">Loading...</span></div>
    <div class="muted">Current path: <span class="mono" id="statusPath">/</span></div>
  </div>

  <div class="grid" id="list"></div>

  <div class="jobs">
    <h3>Jobs</h3>
    <div class="row">
      <button onclick="loadJobs()">Reload Jobs</button>
      <button onclick="clearStatus()">Clear Status</button>
    </div>
    <div id="jobs"></div>
  </div>

<script>
let cwd = "";
let items = [];

function setStatus(msg, isErr=false){
  const el = document.getElementById("statusText");
  el.textContent = msg;
  el.className = isErr ? "err" : "";
  document.getElementById("statusPath").textContent = "/" + (cwd || "");
}

function clearStatus(){ setStatus("OK"); }

function updateSearchClearVisibility(){
  const searchEl = document.getElementById("search");
  const clearBtn = document.getElementById("clearSearchBtn");
  const hasText = (searchEl.value || "").length > 0;
  clearBtn.style.display = hasText ? "inline-block" : "none";
}

function onSearchInput(){
  updateSearchClearVisibility();
  render();
}

function clearSearch(){
  const searchEl = document.getElementById("search");
  searchEl.value = "";
  updateSearchClearVisibility();
  render();
  searchEl.focus();
}

function joinPath(a,b){
  if(!a) return b;
  if(!b) return a;
  return (a.replace(/\/+$/,'') + "/" + b.replace(/^\/+/,''));
}

function setRoot(){
  const root = document.getElementById("rootSel").value;
  cwd = root || "";
  document.getElementById("typeSel").value = "auto";
  refresh();
}
document.getElementById("rootSel").addEventListener("change", setRoot);

function pathLabel(){ return "/" + (cwd || ""); }

function goUp(){
  if(!cwd) return;
  const parts = cwd.split("/").filter(Boolean);
  parts.pop();
  cwd = parts.join("/");
  refresh();
}

async function refresh(){
  document.getElementById("pathView").textContent = pathLabel();
  setStatus("Browsing...");
  try{
    const res = await fetch(`/api/browse?path=${encodeURIComponent(cwd)}`);
    const data = await res.json().catch(()=>null);
    if(!res.ok){
      setStatus(`Browse failed (${res.status}): ` + (data?.detail || data?.error || "Unknown"), true);
      items = [];
      render();
      return;
    }
    items = data || [];
    setStatus("OK");
    render();
  }catch(e){
    setStatus("Browse exception: " + (e?.message || String(e)), true);
    items = [];
    render();
  }
}

function fmtBytes(n){
  if(n === null || n === undefined) return "";
  const u = ["B","KB","MB","GB","TB"];
  let i=0; let x=n;
  while(x>=1024 && i<u.length-1){ x/=1024; i++; }
  return `${x.toFixed(i?1:0)} ${u[i]}`;
}

function render(){
  const q = (document.getElementById("search").value || "").toLowerCase();
  const resultFilter = document.getElementById("resultFilterSel").value;
  const resultSort = document.getElementById("resultSortSel").value;
  const el = document.getElementById("list");
  el.innerHTML = "";
  updateSearchClearVisibility();

  const filtered = items
    .filter(it => !q || it.name.toLowerCase().includes(q))
    .filter(it => {
      if(resultFilter === "files") return !it.is_dir;
      if(resultFilter === "dirs") return it.is_dir;
      return true;
    })
    .slice()
    .sort((a, b) => {
      if(resultSort === "name_asc") return a.name.localeCompare(b.name);
      if(resultSort === "name_desc") return b.name.localeCompare(a.name);

      const sizeA = a.is_dir ? -1 : Number(a.size || 0);
      const sizeB = b.is_dir ? -1 : Number(b.size || 0);

      if(resultSort === "size_asc") return sizeA - sizeB || a.name.localeCompare(b.name);
      if(resultSort === "size_desc") return sizeB - sizeA || a.name.localeCompare(b.name);
      return 0;
    });

  if(filtered.length === 0){
    el.innerHTML = `<div class="item"><div class="muted">No items</div></div>`;
    return;
  }

  for(const it of filtered){
    const row = document.createElement("div");
    row.className = "item";

    const left = document.createElement("div");
    left.className = "name";

    const badge = document.createElement("span");
    badge.className = "badge";
    badge.textContent = it.is_dir ? "DIR" : "FILE";

    const name = document.createElement("span");
    name.textContent = it.name;

    left.appendChild(badge);
    left.appendChild(name);

    if(it.is_dir){
      row.style.cursor = "pointer";
      row.onclick = () => { cwd = joinPath(cwd, it.name); refresh(); };
    }

    const right = document.createElement("div");
    right.className = "row";

    const size = document.createElement("span");
    size.className = "muted";
    size.textContent = it.is_dir ? "" : fmtBytes(it.size);
    right.appendChild(size);

    if(!it.is_dir){
      const btn = document.createElement("button");
      btn.textContent = "Download";
      btn.onclick = async (ev) => {
        ev.stopPropagation();
        const type = document.getElementById("typeSel").value;
        const destination = document.getElementById("destSel").value;
        const rel = joinPath(cwd, it.name);

        setStatus("Enqueueing...");
        try{
          const res = await fetch("/api/enqueue", {
            method:"POST",
            headers:{ "Content-Type":"application/json" },
            body: JSON.stringify({ source_rel_path: rel, media_type: type, destination })
          });
          const data = await res.json().catch(()=>null);
          if(!res.ok){
            setStatus("Enqueue failed: " + (data?.detail || data?.error || "Unknown"), true);
            return;
          }
          setStatus("Enqueued job: " + data.job_id);
          loadJobs();
        }catch(e){
          setStatus("Enqueue exception: " + (e?.message || String(e)), true);
        }
      };
      right.appendChild(btn);
    }

    row.appendChild(left);
    row.appendChild(right);
    el.appendChild(row);
  }
}

async function postJson(path){
  const res = await fetch(path, {method:"POST"});
  const data = await res.json().catch(()=>null);
  if(!res.ok){
    setStatus(`Request failed (${res.status}): ` + (data?.detail || data?.error || "Unknown"), true);
    return null;
  }
  return data;
}

async function cancelJob(jobId){
  setStatus("Cancel requested...");
  await postJson(`/api/cancel/${encodeURIComponent(jobId)}`);
  loadJobs();
}

async function retryJob(jobId){
  setStatus("Retry queued...");
  await postJson(`/api/retry/${encodeURIComponent(jobId)}`);
  loadJobs();
}

async function deleteJob(jobId){
  const ok = confirm("Delete this job? (Will try to remove .partial temp file)");
  if(!ok) return;
  setStatus("Deleting job...");
  await postJson(`/api/delete/${encodeURIComponent(jobId)}`);
  loadJobs();
}

async function loadJobs(){
  try{
    const res = await fetch("/api/jobs");
    const jobs = await res.json().catch(()=>[]);
    const el = document.getElementById("jobs");
    el.innerHTML = "";

    if(!res.ok){
      el.innerHTML = `<div class="job err">Failed to load jobs</div>`;
      return;
    }

    if(!jobs || jobs.length === 0){
      el.innerHTML = `<div class="job"><div class="muted">No jobs yet</div></div>`;
      return;
    }

    for(const j of jobs){
      const box = document.createElement("div");
      box.className = "job";

      const st = j.status || "";
      const cls = st === "done" ? "ok" : (st === "error" ? "err" : "");

      const canCancel = ["queued","running","copying","finalizing","cancel_requested","cancelling"].includes(st);
      const canRetry  = ["error","cancelled","done"].includes(st);
      const canDelete = ["error","cancelled","done"].includes(st);

      const totalBytes = Number(j.source_size_bytes || 0);
      let downloadedBytes = Number(j.downloaded_bytes || 0);
      const speedBps = Number(j.speed_bps || 0);
      const etaSeconds = Number(j.eta_seconds ?? -1);
      if((!downloadedBytes || downloadedBytes < 0) && totalBytes > 0){
        downloadedBytes = Math.floor(((Number(j.progress_pct || 0) || 0) / 100) * totalBytes);
      }
      if(st === "done" && totalBytes > 0){
        downloadedBytes = totalBytes;
      }
      if(totalBytes > 0){
        downloadedBytes = Math.min(downloadedBytes, totalBytes);
      }
      const sizeInfo = totalBytes > 0
        ? `Size: ${fmtBytes(totalBytes)} • Downloaded: ${fmtBytes(downloadedBytes)}`
        : "";
      const speedInfo = speedBps > 0 ? `Speed: ${fmtBytes(speedBps)}/s` : "";
      const etaInfo = etaSeconds >= 0 ? `ETA: ${Math.floor(etaSeconds / 60)}m ${etaSeconds % 60}s` : "";

      box.innerHTML = `
        <div class="row" style="justify-content:space-between;">
          <div>
            <b>${(j.media_type || "auto").toUpperCase()}</b>
            <span class="badge">${(j.destination || "local").toUpperCase()}</span>
            <span class="muted">${j.source_rel_path || ""}</span>
          </div>
          <div class="row">
            ${canCancel ? `<button data-act="cancel">Cancel</button>` : ``}
            ${canRetry ? `<button data-act="retry">Retry</button>` : ``}
            ${canDelete ? `<button data-act="delete">Delete</button>` : ``}
          </div>
        </div>
        <div>Status: <span class="${cls}">${st}</span> • ${(j.progress_pct ?? 0)}%</div>
        ${sizeInfo ? `<div class="muted">${sizeInfo}</div>` : ``}
        ${(speedInfo || etaInfo) ? `<div class="muted">${speedInfo}${speedInfo && etaInfo ? " • " : ""}${etaInfo}</div>` : ``}
        <div class="muted">${j.message ?? ""}</div>
      `;

      // Wire buttons safely
      const btnCancel = box.querySelector('button[data-act="cancel"]');
      if(btnCancel) btnCancel.onclick = () => cancelJob(j.job_id);

      const btnRetry = box.querySelector('button[data-act="retry"]');
      if(btnRetry) btnRetry.onclick = () => retryJob(j.job_id);

      const btnDelete = box.querySelector('button[data-act="delete"]');
      if(btnDelete) btnDelete.onclick = () => deleteJob(j.job_id);

      el.appendChild(box);
    }
  }catch(e){
    setStatus("Jobs exception: " + (e?.message || String(e)), true);
  }
}

refresh();
loadJobs();
setInterval(loadJobs, 5000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def ui(auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth
    return HTMLResponse(UI_HTML)


# -------------------------
# API
# -------------------------
@app.get("/api/browse")
async def api_browse(path: str = "", auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth
    try:
        return list_dir(ZURG_ROOT, path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/enqueue")
async def api_enqueue(body: EnqueueBody, auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth

    try:
        src_abs = safe_join_under_root(ZURG_ROOT, body.source_rel_path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not os.path.exists(src_abs) or not os.path.isfile(src_abs):
        raise HTTPException(status_code=404, detail="Source file not found")

    source_size_bytes = os.path.getsize(src_abs)

    mt = body.media_type or "auto"
    destination = body.destination or "local"
    if mt == "auto":
        detected = detect_media_type(body.source_rel_path)
        if not detected:
            raise HTTPException(
                status_code=400,
                detail="Cannot auto-detect media type. Browse via movies/ or shows/ or force Movie/Show.",
            )
        mt = detected

    job_id = str(uuid.uuid4())
    now = int(time.time())

    job = {
        "job_id": job_id,
        "source_rel_path": body.source_rel_path.strip().lstrip("/"),
        "media_type": mt,
        "destination": destination,
        "status": "queued",
        "progress_pct": 0,
        "source_size_bytes": source_size_bytes,
        "downloaded_bytes": 0,
      "speed_bps": 0,
      "eta_seconds": -1,
      "started_at": 0,
        "message": "",
        "created_at": now,
        "updated_at": now,
        "cancel_requested": "0",
        "pid": "",
        "dest_abs": "",
        "tmp_abs": "",
    }

    r.hset(f"job:{job_id}", mapping=job)
    r.lpush("queue:jobs", job_id)

    return {"job_id": job_id}


@app.post("/api/cancel/{job_id}")
async def api_cancel(job_id: str, auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth

    key = f"job:{job_id}"
    job = r.hgetall(key)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    r.hset(
        key,
        mapping={
            "cancel_requested": "1",
            "status": "cancel_requested",
            "message": "Cancel requested",
            "updated_at": int(time.time()),
        },
    )
    r.lrem("queue:jobs", 0, job_id)
    return {"ok": True, "job_id": job_id}


@app.post("/api/retry/{job_id}")
async def api_retry(job_id: str, auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth

    key = f"job:{job_id}"
    job = r.hgetall(key)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") not in ("error", "cancelled", "done"):
        raise HTTPException(status_code=400, detail="Job is active. Cancel it first.")

    now = int(time.time())
    r.hset(
        key,
        mapping={
            "status": "queued",
            "progress_pct": 0,
            "downloaded_bytes": 0,
            "speed_bps": 0,
            "eta_seconds": -1,
            "started_at": 0,
            "message": "Retry queued",
            "cancel_requested": "0",
            "pid": "",
            "updated_at": now,
        },
    )
    r.lpush("queue:jobs", job_id)
    return {"ok": True, "job_id": job_id}


@app.post("/api/delete/{job_id}")
async def api_delete(job_id: str, auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth

    key = f"job:{job_id}"
    job = r.hgetall(key)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Don't delete active jobs
    if job.get("status") in ("queued", "running", "copying", "cancel_requested", "cancelling", "finalizing"):
        raise HTTPException(status_code=400, detail="Job is active. Cancel it first.")

    # Remove from queue (safety)
    r.lrem("queue:jobs", 0, job_id)

    # Cleanup temp partial if worker stored it
    tmp_abs = job.get("tmp_abs") or ""
    if tmp_abs:
        try:
            if os.path.exists(tmp_abs) and os.path.isfile(tmp_abs):
                os.remove(tmp_abs)
        except Exception:
            pass

    r.delete(key)
    return {"ok": True, "job_id": job_id}


@app.get("/api/jobs")
async def api_jobs(auth=Depends(require_auth)):
    if isinstance(auth, JSONResponse):
        return auth

    numeric_fields = [
      "created_at",
      "updated_at",
      "progress_pct",
      "source_size_bytes",
      "downloaded_bytes",
      "speed_bps",
      "eta_seconds",
      "started_at",
    ]

    keys = r.keys("job:*")
    jobs = []
    for k in keys:
        j = r.hgetall(k)
        if not j:
            continue
        j = {
            key: (int(value) if key in numeric_fields and str(value).isdigit() else value)
            for key, value in j.items()
        }
        jobs.append(j)

    jobs.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return jobs[:50]
