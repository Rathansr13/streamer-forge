"""
StreamForge VOD Origin Server
──────────────────────────────
Standalone VOD platform — upload video, get asset_id, play via ABR.

REST API:
  POST   /api/upload              → upload video, get asset_id instantly
  GET    /api/assets              → list all assets
  GET    /api/assets/<asset_id>   → poll status + delivery URLs
  DELETE /api/assets/<asset_id>   → delete asset + files
  GET    /health                  → server health check

HLS delivery:
  GET /vod/<asset_id>/master.m3u8          → master manifest
  GET /vod/<asset_id>/<variant>/<file>     → segments + variant manifests
  GET /vod/<asset_id>/thumb.jpg            → thumbnail

Web UI:
  GET /                           → full upload + ABR player SPA
  GET /player/<asset_id>          → direct player for an asset
"""

import json
import logging
import os
import shutil
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from aiohttp import web
import asyncio

# Import the VOD transcoder
import sys
sys.path.insert(0, str(Path(__file__).parent))
from transcoder import encode_vod

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ORIGIN] %(message)s")
log = logging.getLogger("origin")

BASE_DIR     = Path(__file__).parent.parent
HLS_DIR      = BASE_DIR / "hls_output"
UPLOAD_DIR   = BASE_DIR / "uploads"
ASSETS_FILE  = BASE_DIR / "assets.json"
ORIGIN_HOST  = "0.0.0.0"
ORIGIN_PORT  = 8080

HLS_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

CORS = {
    "Access-Control-Allow-Origin":   "*",
    "Access-Control-Allow-Methods":  "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers":  "Content-Type, Range",
    "Access-Control-Expose-Headers": "Content-Length, Content-Range",
}


# ── Asset data model ───────────────────────────────────────────────────────
@dataclass
class Asset:
    asset_id:          str
    name:              str
    original_filename: str
    file_size:         int
    status:            str        # queued | encoding | packaging | ready | error
    created_at:        float
    updated_at:        float
    duration:          Optional[float] = None
    width:             Optional[int]   = None
    height:            Optional[int]   = None
    error_msg:         Optional[str]   = None
    encode_progress:   int = 0
    variants:          List[str] = field(default_factory=list)

    def to_dict(self, base_url: str = "") -> dict:
        d = asdict(self)
        d["file_size_mb"]   = round(self.file_size / 1024 / 1024, 2)
        d["created_at_fmt"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(self.created_at))
        if base_url and self.status == "ready":
            d["urls"] = {
                "hls_master": f"{base_url}/vod/{self.asset_id}/master.m3u8",
                "thumbnail":  f"{base_url}/vod/{self.asset_id}/thumb.jpg",
                **{f"hls_{v}": f"{base_url}/vod/{self.asset_id}/{v}/{v}.m3u8"
                   for v in self.variants},
            }
        return d


# ── In-memory registry ─────────────────────────────────────────────────────
_assets: Dict[str, Asset] = {}
_lock   = threading.Lock()


def _save():
    with _lock:
        data = {aid: asdict(a) for aid, a in _assets.items()}
    with open(ASSETS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _load():
    if not ASSETS_FILE.exists():
        return
    try:
        raw = json.loads(ASSETS_FILE.read_text())
        for aid, d in raw.items():
            d.setdefault("variants", [])
            _assets[aid] = Asset(**d)
        log.info(f"Loaded {len(_assets)} assets from disk")
    except Exception as e:
        log.warning(f"Could not load assets.json: {e}")


# ── Background encode worker ───────────────────────────────────────────────
def _run_encode(asset: Asset, source_path: str):
    """Called in a daemon thread; updates asset in place."""

    def on_progress(status: str, pct: int):
        asset.status           = status if status not in ("ready", "error") else status
        asset.encode_progress  = pct
        asset.updated_at       = time.time()
        _save()

    result = encode_vod(asset.asset_id, source_path, on_progress=on_progress)

    asset.status           = result["status"]
    asset.duration         = result.get("duration")
    asset.width            = result.get("width")
    asset.height           = result.get("height")
    asset.variants         = result.get("variants", [])
    asset.error_msg        = result.get("error_msg")
    asset.encode_progress  = 100 if result["status"] == "ready" else 0
    asset.updated_at       = time.time()
    _save()


# ── CORS preflight ─────────────────────────────────────────────────────────
async def handle_options(request):
    return web.Response(headers=CORS)


# ── REST API ───────────────────────────────────────────────────────────────
async def api_upload(request: web.Request) -> web.Response:
    reader = await request.multipart()
    file_field = None
    name_val   = ""

    async for part in reader:
        if part.name == "file":
            file_field = part
            orig_name  = part.filename or "upload.mp4"
            asset_id   = "vod-" + uuid.uuid4().hex[:12].upper()
            dest_dir   = UPLOAD_DIR / asset_id
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path  = dest_dir / orig_name
            # Stream to disk
            with open(dest_path, "wb") as f:
                while True:
                    chunk = await part.read_chunk(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        elif part.name == "name":
            name_val = (await part.read()).decode("utf-8", errors="replace").strip()

    if file_field is None:
        return web.Response(status=400, text="No file uploaded", headers=CORS)

    file_size = dest_path.stat().st_size
    name      = name_val or Path(orig_name).stem
    now       = time.time()

    asset = Asset(
        asset_id=asset_id, name=name, original_filename=orig_name,
        file_size=file_size, status="queued",
        created_at=now, updated_at=now,
    )
    with _lock:
        _assets[asset_id] = asset
    _save()

    # Start encoding in background thread
    t = threading.Thread(target=_run_encode, args=(asset, str(dest_path)), daemon=True)
    t.start()

    log.info(f"Upload accepted: {orig_name} → {asset_id} ({file_size/1024/1024:.1f} MB)")
    base = str(request.url.origin())
    return web.Response(
        status=202,
        text=json.dumps({**asset.to_dict(base), "message": "Encoding started"}),
        content_type="application/json",
        headers=CORS,
    )


async def api_list_assets(request: web.Request) -> web.Response:
    base = str(request.url.origin())
    with _lock:
        items = [a.to_dict(base) for a in sorted(
            _assets.values(), key=lambda x: x.created_at, reverse=True
        )]
    return web.Response(
        text=json.dumps({"assets": items, "total": len(items)}),
        content_type="application/json", headers=CORS,
    )


async def api_get_asset(request: web.Request) -> web.Response:
    asset_id = request.match_info["asset_id"]
    with _lock:
        asset = _assets.get(asset_id)
    if not asset:
        return web.Response(status=404, text=json.dumps({"error": "Not found"}),
                            content_type="application/json", headers=CORS)
    base = str(request.url.origin())
    return web.Response(text=json.dumps(asset.to_dict(base)),
                        content_type="application/json", headers=CORS)


async def api_delete_asset(request: web.Request) -> web.Response:
    asset_id = request.match_info["asset_id"]
    with _lock:
        asset = _assets.pop(asset_id, None)
    if not asset:
        return web.Response(status=404, text=json.dumps({"error": "Not found"}),
                            content_type="application/json", headers=CORS)
    shutil.rmtree(HLS_DIR    / asset_id, ignore_errors=True)
    shutil.rmtree(UPLOAD_DIR / asset_id, ignore_errors=True)
    _save()
    return web.Response(text=json.dumps({"deleted": asset_id}),
                        content_type="application/json", headers=CORS)


async def api_health(request: web.Request) -> web.Response:
    return web.Response(
        text=json.dumps({"status": "ok", "assets": len(_assets)}),
        content_type="application/json", headers=CORS,
    )


# ── HLS / VOD file serving ─────────────────────────────────────────────────
async def serve_vod_master(request: web.Request) -> web.Response:
    asset_id = request.match_info["asset_id"]
    path     = HLS_DIR / asset_id / "master.m3u8"
    if not path.exists():
        with _lock:
            a = _assets.get(asset_id)
        if a and a.status in ("queued", "encoding", "packaging"):
            return web.Response(status=503, text="Encoding in progress",
                                headers={**CORS, "Retry-After": "3"})
        return web.Response(status=404, text="Asset not found", headers=CORS)
    headers = {**CORS, "Content-Type": "application/vnd.apple.mpegurl",
               "Cache-Control": "public, max-age=300"}
    return web.Response(body=path.read_bytes(), headers=headers)


async def serve_vod_file(request: web.Request) -> web.Response:
    asset_id = request.match_info["asset_id"]
    variant  = request.match_info["variant"]
    filename = request.match_info["filename"]
    path     = HLS_DIR / asset_id / variant / filename
    if not path.exists():
        return web.Response(status=404, text="Not found", headers=CORS)
    if filename.endswith(".m3u8"):
        ct  = "application/vnd.apple.mpegurl"
        cc  = "public, max-age=300"
    elif filename.endswith(".ts"):
        ct  = "video/mp2t"
        cc  = "public, max-age=86400, immutable"
    else:
        return web.Response(status=400, headers=CORS)
    return web.Response(body=path.read_bytes(),
                        headers={**CORS, "Content-Type": ct, "Cache-Control": cc})


async def serve_thumb(request: web.Request) -> web.Response:
    asset_id = request.match_info["asset_id"]
    path     = HLS_DIR / asset_id / "thumb.jpg"
    if not path.exists():
        return web.Response(status=404, headers=CORS)
    return web.Response(body=path.read_bytes(),
                        headers={**CORS, "Content-Type": "image/jpeg",
                                 "Cache-Control": "public, max-age=86400"})


# ── Web UI (single-page app) ───────────────────────────────────────────────
def _build_ui_html(asset_id_focus: str = "") -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>StreamForge VOD</title>
<script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=Fragment+Mono:ital@0;1&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
:root{{
  --bg:#08090e;--s1:#0f1118;--s2:#161b26;--s3:#1e2535;
  --border:#252d3e;--border2:#2d3a50;
  --text:#d8e0f0;--muted:#4b5568;--muted2:#6b7d9a;
  --accent:#4f8ef7;--green:#22d67a;--red:#f05060;--amber:#f0a030;
  --head:'Outfit',sans-serif;--mono:'Fragment Mono',monospace;
}}
html{{font-size:14px;}}
body{{background:var(--bg);color:var(--text);font-family:var(--head);-webkit-font-smoothing:antialiased;min-height:100vh;
  background-image:radial-gradient(ellipse 80% 50% at 15% 10%,#4f8ef712 0%,transparent 60%),
    radial-gradient(ellipse 60% 40% at 85% 90%,#22d67a0a 0%,transparent 60%);}}
::-webkit-scrollbar{{width:4px;}}::-webkit-scrollbar-track{{background:var(--bg);}}::-webkit-scrollbar-thumb{{background:var(--s3);border-radius:2px;}}
@keyframes fadeUp{{from{{opacity:0;transform:translateY(8px);}}to{{opacity:1;transform:translateY(0);}}}}
@keyframes spin{{to{{transform:rotate(360deg);}}}}
@keyframes pulse{{0%,100%{{opacity:1;}}50%{{opacity:.3;}}}}
@keyframes shimmer{{0%,100%{{opacity:.5;}}50%{{opacity:1;}}}}

/* Header */
.hdr{{display:flex;align-items:center;justify-content:space-between;padding:18px 28px;
  border-bottom:1px solid var(--border);background:#0f111888;backdrop-filter:blur(12px);
  position:sticky;top:0;z-index:100;}}
.logo{{display:flex;align-items:center;gap:10px;}}
.logo-mark{{width:34px;height:34px;border-radius:8px;background:linear-gradient(135deg,#4f8ef7,#22d67a);
  display:flex;align-items:center;justify-content:center;font-weight:800;font-size:14px;color:#fff;
  box-shadow:0 0 18px #4f8ef730;}}
.logo-name{{font-weight:800;font-size:17px;letter-spacing:-.03em;color:#fff;}}
.logo-tag{{font-size:10px;color:var(--muted2);letter-spacing:.1em;font-family:var(--mono);}}
.hdr-pill{{display:flex;align-items:center;gap:6px;font-size:11px;font-family:var(--mono);
  color:var(--muted2);background:var(--s2);border:1px solid var(--border);padding:5px 12px;border-radius:99px;}}
.hdr-dot{{width:6px;height:6px;border-radius:50%;animation:pulse 2s ease-in-out infinite;}}

/* Layout */
.layout{{display:grid;grid-template-columns:380px 1fr;min-height:calc(100vh - 61px);}}
.sidebar{{border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden;}}
.main{{display:flex;flex-direction:column;overflow:hidden;}}

/* Section label */
.sec-lbl{{font-size:10px;font-family:var(--mono);letter-spacing:.15em;color:var(--muted);
  text-transform:uppercase;padding:14px 20px 10px;border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;}}

/* Upload */
.upl-area{{padding:18px;}}
.dropzone{{border:2px dashed var(--border2);border-radius:11px;padding:28px 16px;text-align:center;
  cursor:pointer;transition:all .2s;background:var(--s1);position:relative;overflow:hidden;}}
.dropzone::before{{content:'';position:absolute;inset:0;background:linear-gradient(135deg,#4f8ef70a,#22d67a08);opacity:0;transition:opacity .2s;}}
.dropzone:hover,.dropzone.drag{{border-color:var(--accent);}}
.dropzone:hover::before,.dropzone.drag::before{{opacity:1;}}
.dz-icon{{font-size:36px;margin-bottom:8px;display:block;}}
.dz-title{{font-weight:700;font-size:14px;color:#fff;margin-bottom:5px;}}
.dz-sub{{font-size:11px;color:var(--muted2);line-height:1.7;font-family:var(--mono);}}
.file-pill{{display:flex;align-items:center;gap:10px;background:var(--s2);border:1px solid var(--border2);
  border-radius:8px;padding:9px 13px;margin-top:11px;}}
.fp-icon{{width:36px;height:28px;background:var(--s3);border-radius:4px;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0;}}
.fp-name{{font-size:12px;color:#fff;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;}}
.fp-size{{font-size:10px;color:var(--muted2);font-family:var(--mono);flex-shrink:0;}}
.fp-rm{{background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;padding:2px 4px;}}
.fp-rm:hover{{color:var(--red);}}
.fld{{margin-top:13px;}}
.fld-lbl{{font-size:10px;color:var(--muted2);letter-spacing:.1em;text-transform:uppercase;margin-bottom:5px;font-family:var(--mono);}}
.fld-inp{{width:100%;background:var(--s2);border:1px solid var(--border2);color:var(--text);
  border-radius:6px;padding:9px 12px;font-family:var(--head);font-size:13px;outline:none;transition:border-color .15s;}}
.fld-inp:focus{{border-color:var(--accent);box-shadow:0 0 0 2px #4f8ef715;}}
.upl-btn{{width:100%;margin-top:15px;padding:12px;background:linear-gradient(135deg,#4f8ef7,#22d67a);
  border:none;border-radius:9px;color:#fff;font-family:var(--head);font-weight:700;font-size:13px;
  letter-spacing:.04em;cursor:pointer;transition:all .2s;}}
.upl-btn:hover{{transform:translateY(-1px);box-shadow:0 6px 20px #4f8ef730;}}
.upl-btn:disabled{{background:var(--s3);color:var(--muted);cursor:not-allowed;transform:none;box-shadow:none;}}

/* Progress */
.prog-wrap{{margin-top:15px;animation:fadeUp .3s ease;}}
.prog-head{{display:flex;justify-content:space-between;font-size:11px;color:var(--muted2);margin-bottom:7px;font-family:var(--mono);}}
.prog-head span{{color:var(--accent);}}
.prog-track{{height:3px;background:var(--s3);border-radius:99px;overflow:hidden;}}
.prog-fill{{height:100%;background:linear-gradient(90deg,var(--accent),var(--green));border-radius:99px;transition:width .4s ease;}}
.pipeline{{display:flex;background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:11px 13px;margin-top:14px;}}
.ps{{flex:1;text-align:center;position:relative;}}
.ps::after{{content:'';position:absolute;top:9px;left:55%;width:90%;height:1px;background:var(--border);z-index:0;}}
.ps:last-child::after{{display:none;}}
.ps-dot{{width:20px;height:20px;border-radius:50%;margin:0 auto 4px;display:flex;align-items:center;justify-content:center;
  font-size:8px;position:relative;z-index:1;border:1.5px solid var(--border2);background:var(--s1);color:var(--muted);transition:all .3s;}}
.ps.act .ps-dot{{border-color:var(--accent);background:#4f8ef720;color:var(--accent);box-shadow:0 0 8px #4f8ef750;animation:shimmer 1.2s ease-in-out infinite;}}
.ps.done .ps-dot{{border-color:var(--green);background:#22d67a20;color:var(--green);}}
.ps-lbl{{font-size:9px;color:var(--muted);letter-spacing:.04em;font-family:var(--mono);}}
.ps.act .ps-lbl{{color:var(--accent);}} .ps.done .ps-lbl{{color:var(--green);}}

/* Asset list */
.asset-scroll{{flex:1;overflow-y:auto;padding:10px 14px;}}
.empty{{text-align:center;padding:36px 16px;color:var(--muted);font-size:13px;}}
.empty-icon{{font-size:28px;margin-bottom:8px;opacity:.3;}}
.ac{{display:flex;align-items:center;gap:11px;padding:10px 12px;border-radius:8px;cursor:pointer;
  border:1px solid transparent;margin-bottom:5px;transition:all .15s;background:var(--s1);}}
.ac:hover{{border-color:var(--border2);background:var(--s2);}}
.ac.sel{{border-color:var(--accent);background:#4f8ef710;}}
.ac.play{{border-color:var(--green);background:#22d67a08;}}
.ac-thumb{{width:52px;height:36px;border-radius:4px;overflow:hidden;background:var(--s3);flex-shrink:0;
  display:flex;align-items:center;justify-content:center;font-size:16px;position:relative;}}
.ac-thumb img{{width:100%;height:100%;object-fit:cover;}}
.ac-ov{{position:absolute;inset:0;background:#00000060;display:flex;align-items:center;justify-content:center;
  opacity:0;transition:opacity .15s;font-size:13px;}}
.ac:hover .ac-ov{{opacity:1;}}
.ac-info{{flex:1;overflow:hidden;}}
.ac-name{{font-size:12px;color:#fff;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.ac-meta{{font-size:10px;color:var(--muted2);margin-top:2px;font-family:var(--mono);}}
.badge{{font-size:9px;padding:2px 7px;border-radius:4px;flex-shrink:0;font-weight:600;letter-spacing:.08em;font-family:var(--mono);}}
.b-ready{{background:#22d67a18;color:var(--green);border:1px solid #22d67a30;}}
.b-enc{{background:#4f8ef718;color:var(--accent);border:1px solid #4f8ef730;animation:pulse 1.5s ease-in-out infinite;}}
.b-q{{background:#f0a03018;color:var(--amber);border:1px solid #f0a03030;}}
.b-err{{background:#f0506018;color:var(--red);border:1px solid #f0506030;}}

/* Detail panel */
.detail{{flex:1;display:flex;flex-direction:column;overflow-y:auto;}}
.detail-empty{{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;color:var(--muted);}}
.de-icon{{font-size:52px;opacity:.12;}}
.de-text{{font-size:14px;font-weight:500;}}
.de-sub{{font-size:12px;font-family:var(--mono);}}

/* Asset ID hero */
.id-hero{{padding:16px 22px;background:linear-gradient(135deg,#4f8ef710,#22d67a08);
  border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px;}}
.id-lbl{{font-size:10px;color:var(--green);letter-spacing:.15em;font-family:var(--mono);text-transform:uppercase;margin-bottom:3px;}}
.id-val{{font-size:19px;font-weight:800;color:#fff;font-family:var(--mono);letter-spacing:.03em;}}
.id-copy{{background:var(--s2);border:1px solid #22d67a40;color:var(--green);border-radius:7px;
  padding:7px 14px;font-size:11px;font-family:var(--mono);cursor:pointer;transition:all .15s;flex-shrink:0;}}
.id-copy:hover{{background:#22d67a18;}}

/* Encode progress in detail */
.enc-prog{{padding:14px 22px;background:var(--s1);border-bottom:1px solid var(--border);}}
.enc-prog-head{{display:flex;justify-content:space-between;font-size:11px;font-family:var(--mono);color:var(--muted2);margin-bottom:7px;}}
.enc-prog-head span{{color:var(--accent);}}

/* Info grid */
.info-grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:16px 22px;
  background:var(--s1);border-bottom:1px solid var(--border);}}
.info-lbl{{font-size:9px;color:var(--muted2);letter-spacing:.12em;font-family:var(--mono);text-transform:uppercase;margin-bottom:4px;}}
.info-val{{font-size:12px;color:var(--text);font-family:var(--mono);}}

/* URL rows */
.urls{{padding:0 22px 16px;background:var(--s1);border-bottom:1px solid var(--border);}}
.urls-title{{font-size:10px;color:var(--muted2);letter-spacing:.12em;font-family:var(--mono);text-transform:uppercase;padding:13px 0 9px;}}
.url-row{{display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid var(--border);}}
.url-row:last-child{{border-bottom:none;}}
.url-tag{{font-size:9px;padding:2px 7px;border-radius:4px;flex-shrink:0;font-weight:700;letter-spacing:.08em;font-family:var(--mono);}}
.t-hls{{background:#22d67a18;color:var(--green);border:1px solid #22d67a30;}}
.t-thumb{{background:#f0a03018;color:var(--amber);border:1px solid #f0a03030;}}
.url-val{{flex:1;font-size:10px;color:var(--muted2);font-family:var(--mono);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}}
.copy-btn{{background:var(--s2);border:1px solid var(--border2);color:var(--muted2);border-radius:5px;
  padding:3px 10px;font-size:10px;font-family:var(--mono);cursor:pointer;transition:all .15s;flex-shrink:0;}}
.copy-btn:hover{{border-color:var(--green);color:var(--green);}}
.copy-btn.ok{{border-color:var(--green);color:var(--green);background:#22d67a15;}}

/* Play button */
.play-section{{padding:14px 22px;background:var(--s1);border-bottom:1px solid var(--border);display:flex;gap:10px;}}
.play-btn{{flex:1;padding:12px;background:linear-gradient(135deg,#4f8ef7,#22d67a);
  border:none;border-radius:9px;color:#fff;font-family:var(--head);font-weight:700;font-size:13px;cursor:pointer;transition:all .2s;}}
.play-btn:hover{{transform:translateY(-1px);box-shadow:0 5px 18px #4f8ef730;}}
.del-btn{{background:transparent;border:1px solid var(--border2);color:var(--muted2);
  border-radius:9px;padding:0 14px;cursor:pointer;font-size:14px;transition:all .15s;}}
.del-btn:hover{{border-color:var(--red);color:var(--red);}}

/* Player */
.player-wrap{{display:none;flex-direction:column;animation:fadeUp .3s ease;border-bottom:1px solid var(--border);}}
.player-topbar{{padding:12px 20px;display:flex;align-items:center;justify-content:space-between;
  background:var(--s1);border-bottom:1px solid var(--border);}}
.pt-name{{font-size:13px;font-weight:700;color:#fff;}}
.pt-id{{font-size:10px;color:var(--muted2);font-family:var(--mono);margin-top:2px;}}
.pt-right{{display:flex;gap:8px;}}
.vid-wrap{{position:relative;background:#000;aspect-ratio:16/9;}}
.vid-wrap video{{width:100%;height:100%;display:block;}}
.vid-ov{{position:absolute;inset:0;background:#000b;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:11px;transition:opacity .3s;}}
.vid-ov.gone{{opacity:0;pointer-events:none;}}
.spinner{{width:40px;height:40px;border:3px solid #ffffff15;border-top-color:var(--accent);border-radius:50%;animation:spin .85s linear infinite;}}
.ov-msg{{font-size:13px;color:rgba(255,255,255,.7);}}
.ov-sub{{font-size:10px;color:var(--muted2);font-family:var(--mono);}}
.q-badge{{position:absolute;bottom:10px;right:10px;background:#000a;border:1px solid #ffffff20;
  padding:3px 8px;border-radius:4px;font-size:10px;color:rgba(255,255,255,.8);font-family:var(--mono);}}
.p-ctrl{{display:flex;align-items:center;gap:12px;padding:11px 18px;background:var(--s2);
  border-top:1px solid var(--border);flex-wrap:wrap;}}
.ctrl-lbl{{font-size:10px;color:var(--muted);font-family:var(--mono);letter-spacing:.1em;}}
.q-sel{{background:var(--s1);border:1px solid var(--border2);color:var(--text);border-radius:5px;
  padding:5px 9px;font-family:var(--mono);font-size:11px;cursor:pointer;outline:none;}}
.q-sel:focus{{border-color:var(--accent);}}
.divider{{width:1px;height:18px;background:var(--border);}}
.abr-st{{font-size:11px;font-family:var(--mono);}}
.stats-bar{{display:grid;grid-template-columns:repeat(5,1fr);border-top:1px solid var(--border);background:var(--s1);}}
.sc{{padding:11px 15px;border-right:1px solid var(--border);display:flex;flex-direction:column;gap:3px;}}
.sc:last-child{{border-right:none;}}
.sc-lbl{{font-size:9px;color:var(--muted);letter-spacing:.12em;font-family:var(--mono);text-transform:uppercase;}}
.sc-val{{font-size:13px;color:#fff;font-weight:600;font-family:var(--mono);}}

/* Toast */
#toasts{{position:fixed;bottom:22px;right:22px;display:flex;flex-direction:column;gap:7px;z-index:9999;}}
.toast{{background:var(--s2);border-left:3px solid var(--green);border-radius:8px;padding:10px 15px;
  font-size:12px;color:#fff;min-width:230px;box-shadow:0 4px 20px #00000060;animation:fadeUp .25s ease;}}
.toast.err{{border-left-color:var(--red);}} .toast.info{{border-left-color:var(--accent);}}
</style>
</head>
<body>

<!-- Header -->
<header class="hdr">
  <div class="logo">
    <div class="logo-mark">SF</div>
    <div>
      <div class="logo-name">StreamForge</div>
      <div class="logo-tag">VOD PIPELINE</div>
    </div>
  </div>
  <div style="display:flex;gap:10px;align-items:center;">
    <div class="hdr-pill"><div class="hdr-dot" id="api-dot" style="background:var(--green)"></div><span id="api-status">READY</span></div>
    <div class="hdr-pill" style="color:var(--muted2)"><span id="asset-count">0</span> ASSETS</div>
  </div>
</header>

<div class="layout">
  <!-- Sidebar -->
  <div class="sidebar">

    <!-- Upload -->
    <div class="sec-lbl"><span>⬆ VOD INGEST</span></div>
    <div class="upl-area">
      <div class="dropzone" id="dz" onclick="document.getElementById('fi').click()">
        <span class="dz-icon">🎬</span>
        <div class="dz-title">Drop video here</div>
        <div class="dz-sub">or click to browse<br>MP4 · MOV · MKV · AVI · MXF</div>
      </div>
      <input type="file" id="fi" accept="video/*" style="display:none" onchange="onFilePick(this.files[0])">
      <div id="fp" style="display:none" class="file-pill">
        <div class="fp-icon">🎞️</div>
        <div class="fp-name" id="fp-name"></div>
        <div class="fp-size" id="fp-size"></div>
        <button class="fp-rm" onclick="clearFile()">✕</button>
      </div>
      <div class="fld"><div class="fld-lbl">Asset Name</div>
        <input class="fld-inp" id="asset-name" placeholder="e.g. Product Demo v2"></div>
      <div id="prog-wrap" style="display:none" class="prog-wrap">
        <div class="prog-head"><span id="prog-stage">Uploading...</span><span id="prog-pct">0%</span></div>
        <div class="prog-track"><div class="prog-fill" id="prog-fill" style="width:0%"></div></div>
        <div class="pipeline" id="pipeline">
          <div class="ps" id="ps0"><div class="ps-dot">1</div><div class="ps-lbl">Upload</div></div>
          <div class="ps" id="ps1"><div class="ps-dot">2</div><div class="ps-lbl">Probe</div></div>
          <div class="ps" id="ps2"><div class="ps-dot">3</div><div class="ps-lbl">Encode</div></div>
          <div class="ps" id="ps3"><div class="ps-dot">4</div><div class="ps-lbl">Package</div></div>
          <div class="ps" id="ps4"><div class="ps-dot">✓</div><div class="ps-lbl">Ready</div></div>
        </div>
      </div>
      <button class="upl-btn" id="upl-btn" disabled onclick="startUpload()">▲  Upload & Ingest</button>
    </div>

    <!-- Asset Library -->
    <div class="sec-lbl"><span>◈ ASSET LIBRARY</span><span style="color:var(--muted2);font-size:10px;" id="lib-count">0 total</span></div>
    <div class="asset-scroll" id="asset-list">
      <div class="empty"><div class="empty-icon">📦</div>Upload a video to begin</div>
    </div>
  </div>

  <!-- Main area -->
  <div class="main">

    <!-- Player (shown when playing) -->
    <div class="player-wrap" id="player-wrap">
      <div class="player-topbar">
        <div><div class="pt-name" id="pt-name">—</div><div class="pt-id" id="pt-id">—</div></div>
        <div class="pt-right">
          <button class="copy-btn" id="copy-hls-btn">COPY HLS URL</button>
          <button class="copy-btn" onclick="closePlayer()">✕ Close</button>
        </div>
      </div>
      <div class="vid-wrap">
        <video id="video" controls playsinline></video>
        <div class="vid-ov" id="vid-ov">
          <div class="spinner"></div>
          <div class="ov-msg" id="ov-msg">Loading...</div>
          <div class="ov-sub" id="ov-sub"></div>
        </div>
        <div class="q-badge" id="q-badge" style="display:none">AUTO</div>
      </div>
      <div class="p-ctrl">
        <div class="ctrl-lbl">QUALITY</div>
        <select class="q-sel" id="q-sel" onchange="setQuality(this.value)">
          <option value="-1">Auto (ABR)</option>
        </select>
        <div class="divider"></div>
        <div class="ctrl-lbl">ABR</div>
        <div class="abr-st" id="abr-st" style="color:var(--green)">Auto</div>
        <div style="margin-left:auto">
          <button class="copy-btn" id="copy-hls-btn2">COPY HLS URL</button>
        </div>
      </div>
      <div class="stats-bar">
        <div class="sc"><div class="sc-lbl">Quality</div><div class="sc-val" id="st-q" style="color:var(--green)">—</div></div>
        <div class="sc"><div class="sc-lbl">Bitrate</div><div class="sc-val" id="st-br" style="color:var(--accent)">—</div></div>
        <div class="sc"><div class="sc-lbl">Buffer</div><div class="sc-val" id="st-buf" style="color:var(--amber)">—</div></div>
        <div class="sc"><div class="sc-lbl">Segments</div><div class="sc-val" id="st-segs">0</div></div>
        <div class="sc"><div class="sc-lbl">Dropped</div><div class="sc-val" id="st-drop" style="color:var(--red)">0</div></div>
      </div>
    </div>

    <!-- Asset detail -->
    <div class="detail" id="detail">
      <div class="detail-empty" id="detail-empty">
        <div class="de-icon">⬡</div>
        <div class="de-text">Upload a video to begin</div>
        <div class="de-sub">Upload → Get Asset ID → Play in ABR Player</div>
      </div>
    </div>
  </div>
</div>

<div id="toasts"></div>

<script>
const API = '';   // same origin
let selectedId = '{asset_id_focus}';
let playingId  = '';
let hlsInst    = null;
let currentHlsUrl = '';
let segCount   = 0;
let selectedFile = null;
let uploadXhr  = null;

// ── File pick ──────────────────────────────────────────────────────────────
function onFilePick(f) {{
  if (!f || !f.type.startsWith('video/')) {{ toast('Please select a video file','err'); return; }}
  selectedFile = f;
  document.getElementById('dz').style.display = 'none';
  document.getElementById('fp').style.display = 'flex';
  document.getElementById('fp-name').textContent = f.name;
  document.getElementById('fp-size').textContent = fmtSize(f.size);
  if (!document.getElementById('asset-name').value)
    document.getElementById('asset-name').value = f.name.replace(/\\.[^.]+$/, '');
  document.getElementById('upl-btn').disabled = false;
}}

function clearFile() {{
  selectedFile = null;
  document.getElementById('fi').value = '';
  document.getElementById('fp').style.display = 'none';
  document.getElementById('dz').style.display = 'block';
  document.getElementById('asset-name').value = '';
  document.getElementById('upl-btn').disabled = true;
  document.getElementById('prog-wrap').style.display = 'none';
  setProg(0,'','');
}}

// ── Drag & drop ────────────────────────────────────────────────────────────
const dz = document.getElementById('dz');
dz.addEventListener('dragover',  e => {{ e.preventDefault(); dz.classList.add('drag'); }});
dz.addEventListener('dragleave', () => dz.classList.remove('drag'));
dz.addEventListener('drop', e => {{
  e.preventDefault(); dz.classList.remove('drag');
  const f = e.dataTransfer.files[0];
  if (f && f.type.startsWith('video/')) onFilePick(f);
  else toast('Please drop a video file','err');
}});

// ── Upload ─────────────────────────────────────────────────────────────────
function startUpload() {{
  if (!selectedFile) return;
  const name = document.getElementById('asset-name').value.trim() || selectedFile.name;
  document.getElementById('upl-btn').disabled = true;
  document.getElementById('prog-wrap').style.display = 'block';
  setPipeStep(-1);

  const fd = new FormData();
  fd.append('file', selectedFile);
  fd.append('name', name);

  const xhr = new XMLHttpRequest();
  uploadXhr = xhr;
  xhr.open('POST', API + '/api/upload');

  xhr.upload.onprogress = e => {{
    if (e.lengthComputable) {{
      const pct = Math.round(e.loaded / e.total * 30);
      setProg(pct, 'Uploading...', pct + '%');
      setPipeStep(pct > 2 ? 0 : -1);
    }}
  }};

  xhr.onload = () => {{
    if (xhr.status === 202) {{
      const data = JSON.parse(xhr.responseText);
      setProg(35, 'Queued for encoding...', '35%'); setPipeStep(1);
      setTimeout(() => {{ setProg(55,'Encoding...','55%'); setPipeStep(2); }}, 500);
      selectedId = data.asset_id;
      loadAssets();
      toast('✓ Asset ' + data.asset_id + ' — encoding started');
      setTimeout(() => {{
        clearFile();
        document.getElementById('upl-btn').disabled = false;
      }}, 1800);
    }} else {{
      toast('Upload failed: ' + xhr.responseText, 'err');
      document.getElementById('upl-btn').disabled = false;
    }}
  }};
  xhr.onerror = () => {{ toast('Network error','err'); document.getElementById('upl-btn').disabled = false; }};
  xhr.send(fd);
}}

function setProg(pct, stage, label) {{
  document.getElementById('prog-fill').style.width = pct + '%';
  if (stage) document.getElementById('prog-stage').textContent = stage;
  if (label) document.getElementById('prog-pct').textContent = label;
}}

function setPipeStep(active) {{
  for (let i = 0; i < 5; i++) {{
    const el = document.getElementById('ps' + i);
    el.className = 'ps' + (i < active ? ' done' : i === active ? ' act' : '');
  }}
}}

// ── Load assets ────────────────────────────────────────────────────────────
async function loadAssets() {{
  try {{
    const r  = await fetch(API + '/api/assets');
    const d  = await r.json();
    const assets = d.assets || [];
    document.getElementById('asset-count').textContent = assets.length;
    document.getElementById('lib-count').textContent = assets.length + ' total';
    renderList(assets);
    // Sync encode progress for encoding assets
    assets.forEach(a => {{
      if (a.asset_id === selectedId && (a.status === 'encoding' || a.status === 'packaging')) {{
        const pct = a.encode_progress || 0;
        setProg(pct, a.status + '...', pct + '%');
        const si = pct < 5 ? 0 : pct < 10 ? 1 : pct < 90 ? 2 : pct < 98 ? 3 : 4;
        setPipeStep(si);
      }}
      if (a.asset_id === selectedId && a.status === 'ready') {{
        document.getElementById('prog-wrap').style.display = 'none';
        setPipeStep(5);
      }}
    }});
    // Render detail for selected
    const sel = assets.find(a => a.asset_id === selectedId);
    if (sel) renderDetail(sel);
    else document.getElementById('detail-empty') && (document.getElementById('detail').innerHTML = detailEmptyHtml());
  }} catch(e) {{
    document.getElementById('api-dot').style.background = 'var(--red)';
    document.getElementById('api-status').textContent = 'OFFLINE';
  }}
}}

function renderList(assets) {{
  const el = document.getElementById('asset-list');
  if (!assets.length) {{
    el.innerHTML = '<div class="empty"><div class="empty-icon">📦</div>Upload a video to begin</div>';
    return;
  }}
  el.innerHTML = assets.map(a => {{
    const bc = a.status === 'ready' ? 'b-ready' : a.status === 'encoding' || a.status === 'packaging' ? 'b-enc' : a.status === 'queued' ? 'b-q' : 'b-err';
    const cls = 'ac' + (a.asset_id === selectedId ? ' sel' : '') + (a.asset_id === playingId ? ' play' : '');
    const thumb = a.status === 'ready' ? `<img src="/vod/${{a.asset_id}}/thumb.jpg" onerror="this.style.display='none'">` : '🎬';
    return `<div class="${{cls}}" onclick="selectAsset('${{a.asset_id}}')">
      <div class="ac-thumb">${{thumb}}<div class="ac-ov">▶</div></div>
      <div class="ac-info">
        <div class="ac-name">${{esc(a.name || a.original_filename)}}</div>
        <div class="ac-meta">${{a.asset_id.slice(0,16)}}… · ${{fmtSize(a.file_size)}} · ${{fmtAgo(a.created_at)}}</div>
      </div>
      <span class="badge ${{bc}}">${{a.status.toUpperCase()}}</span>
    </div>`;
  }}).join('');
}}

function selectAsset(id) {{
  selectedId = id;
  loadAssets();
}}

function renderDetail(a) {{
  const det = document.getElementById('detail');
  let html = '';

  // Asset ID hero
  html += `<div class="id-hero">
    <div><div class="id-lbl">Asset ID</div><div class="id-val">${{a.asset_id}}</div></div>
    <button class="id-copy" onclick="cp('${{a.asset_id}}',this)">COPY ID</button>
  </div>`;

  // Encode progress
  if (a.status !== 'ready' && a.status !== 'error') {{
    const pct = a.encode_progress || 0;
    const si  = pct < 5 ? 0 : pct < 10 ? 1 : pct < 90 ? 2 : pct < 98 ? 3 : 4;
    html += `<div class="enc-prog">
      <div class="enc-prog-head"><span>${{a.status}}</span><span>${{pct}}%</span></div>
      <div class="prog-track"><div class="prog-fill" style="width:${{pct}}%"></div></div>
      <div class="pipeline" style="margin-top:12px;">
        ${{[0,1,2,3,4].map(i => `<div class="ps${{i<si?' done':i===si?' act':''}}">
          <div class="ps-dot">${{i<si?'✓':i+1}}</div>
          <div class="ps-lbl">${{['Upload','Probe','Encode','Package','Ready'][i]}}</div>
        </div>`).join('')}}
      </div>
    </div>`;
  }}

  // Error
  if (a.status === 'error') {{
    html += `<div style="padding:13px 22px;background:#f0506010;border-bottom:1px solid var(--border);">
      <div style="font-size:11px;color:var(--red);font-family:var(--mono);">⚠ ${{esc(a.error_msg||'Encoding failed')}}</div>
    </div>`;
  }}

  // Play button
  if (a.status === 'ready') {{
    html += `<div class="play-section">
      <button class="play-btn" onclick="playAsset('${{a.asset_id}}')">
        ${{playingId === a.asset_id ? '▐▌ Now Playing' : '▶  Play in ABR Player'}}
      </button>
      <button class="del-btn" onclick="deleteAsset('${{a.asset_id}}','${{esc(a.name)}}')">🗑</button>
    </div>`;
  }}

  // Info grid
  html += `<div class="info-grid">
    <div><div class="info-lbl">Original File</div><div class="info-val">${{esc(a.original_filename)}}</div></div>
    <div><div class="info-lbl">File Size</div><div class="info-val">${{fmtSize(a.file_size)}}</div></div>
    <div><div class="info-lbl">Duration</div><div class="info-val">${{fmtDur(a.duration)}}</div></div>
    <div><div class="info-lbl">Resolution</div><div class="info-val">${{a.width ? a.width+'×'+a.height : '—'}}</div></div>
    <div><div class="info-lbl">Variants</div><div class="info-val">${{(a.variants||[]).join(', ')||'—'}}</div></div>
    <div><div class="info-lbl">Created</div><div class="info-val">${{a.created_at_fmt||'—'}}</div></div>
  </div>`;

  // URLs
  if (a.status === 'ready') {{
    const base = window.location.origin;
    const hlsUrl = base + '/vod/' + a.asset_id + '/master.m3u8';
    const rows = [
      ['HLS', 't-hls', hlsUrl],
      ['THUMB', 't-thumb', base + '/vod/' + a.asset_id + '/thumb.jpg'],
      ...(a.variants||[]).map(v => [v.toUpperCase(), 't-hls', base + '/vod/' + a.asset_id + '/' + v + '/' + v + '.m3u8']),
    ];
    html += `<div class="urls"><div class="urls-title">Delivery URLs</div>
      ${{rows.map(([tag,cls,url]) => `<div class="url-row">
        <span class="url-tag ${{cls}}">${{tag}}</span>
        <span class="url-val">${{url}}</span>
        <button class="copy-btn" onclick="cp('${{url}}',this)">COPY</button>
      </div>`).join('')}}
    </div>`;
  }}

  det.innerHTML = html;
}}

function detailEmptyHtml() {{
  return `<div class="detail-empty" id="detail-empty">
    <div class="de-icon">⬡</div>
    <div class="de-text">Upload a video to begin</div>
    <div class="de-sub">Upload → Get Asset ID → Play in ABR Player</div>
  </div>`;
}}

// ── Player ─────────────────────────────────────────────────────────────────
function playAsset(id) {{
  playingId = id;
  const assets_data = document.getElementById('asset-list').querySelectorAll('.ac');
  loadAssets();

  fetch(API + '/api/assets/' + id).then(r=>r.json()).then(a => {{
    const hlsUrl = window.location.origin + '/vod/' + id + '/master.m3u8';
    currentHlsUrl = hlsUrl;
    segCount = 0;

    document.getElementById('pt-name').textContent = a.name || a.original_filename;
    document.getElementById('pt-id').textContent   = id;
    document.getElementById('copy-hls-btn').onclick  = () => cp(hlsUrl);
    document.getElementById('copy-hls-btn2').onclick = () => cp(hlsUrl);

    const pw = document.getElementById('player-wrap');
    pw.style.display = 'flex';
    pw.style.flexDirection = 'column';

    // Reset stats
    ['st-q','st-br','st-buf'].forEach(i => document.getElementById(i).textContent = '—');
    ['st-segs','st-drop'].forEach(i => document.getElementById(i).textContent = '0');

    showOv('Loading stream...', 'Fetching master manifest');
    initHls(hlsUrl);
  }});
}}

function closePlayer() {{
  if (hlsInst) {{ hlsInst.destroy(); hlsInst = null; }}
  document.getElementById('video').src = '';
  document.getElementById('player-wrap').style.display = 'none';
  document.getElementById('q-badge').style.display = 'none';
  playingId = '';
  loadAssets();
}}

function showOv(msg, sub) {{
  const ov = document.getElementById('vid-ov');
  ov.classList.remove('gone');
  document.getElementById('ov-msg').textContent = msg||'';
  document.getElementById('ov-sub').textContent = sub||'';
}}
function hideOv() {{
  document.getElementById('vid-ov').classList.add('gone');
  document.getElementById('q-badge').style.display = 'block';
}}

function initHls(src) {{
  const video = document.getElementById('video');
  if (hlsInst) {{ hlsInst.destroy(); hlsInst = null; }}
  video.src = '';

  if (Hls.isSupported()) {{
    hlsInst = new Hls({{
      maxBufferLength:60, maxMaxBufferLength:120, maxBufferHole:0.5,
      manifestLoadingMaxRetry:20, manifestLoadingRetryDelay:2000,
      levelLoadingMaxRetry:10, fragLoadingMaxRetry:6,
      startLevel:-1, abrEwmaDefaultEstimate:1000000,
    }});
    hlsInst.loadSource(src);
    hlsInst.attachMedia(video);

    hlsInst.on(Hls.Events.MANIFEST_PARSED, (e,d) => {{
      const sel = document.getElementById('q-sel');
      sel.innerHTML = '<option value="-1">Auto (ABR)</option>' +
        d.levels.map((l,i)=>`<option value="${{i}}">${{l.height}}p — ${{Math.round(l.bitrate/1000)}} kbps</option>`).join('');
      showOv('Buffering...','Fetching first segment');
      video.play().catch(()=>{{}});
    }});
    hlsInst.on(Hls.Events.FRAG_BUFFERED, hideOv);
    hlsInst.on(Hls.Events.LEVEL_SWITCHED, (e,d) => {{
      const l = hlsInst.levels[d.level];
      document.getElementById('q-badge').textContent = l.height+'p';
      document.getElementById('st-q').textContent  = l.height+'p';
      document.getElementById('st-br').textContent = Math.round(l.bitrate/1000)+'kbps';
      const auto = hlsInst.autoLevelEnabled;
      document.getElementById('abr-st').textContent = auto ? 'Auto' : 'Manual';
      document.getElementById('abr-st').style.color = auto ? 'var(--green)' : 'var(--accent)';
    }});
    hlsInst.on(Hls.Events.FRAG_LOADED, () => {{
      segCount++;
      document.getElementById('st-segs').textContent = segCount;
    }});
    hlsInst.on(Hls.Events.ERROR, (e,d) => {{
      if (d.details==='manifestLoadError' && d.response?.code===503) {{
        showOv('Encoding in progress...','Retrying automatically'); return;
      }}
      if (d.fatal) {{
        if (d.type===Hls.ErrorTypes.NETWORK_ERROR) {{ showOv('Reconnecting...',d.details); hlsInst.startLoad(); }}
        else if (d.type===Hls.ErrorTypes.MEDIA_ERROR) {{ showOv('Recovering...'); hlsInst.recoverMediaError(); }}
        else {{ showOv('Error',d.details); setTimeout(()=>initHls(src),3000); }}
      }}
    }});
  }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
    video.src = src;
    video.addEventListener('loadedmetadata', hideOv, {{once:true}});
    video.play().catch(()=>{{}});
  }} else {{
    showOv('HLS Not Supported','Use Chrome, Firefox, or Safari');
  }}

  clearInterval(window._statsIv);
  window._statsIv = setInterval(() => {{
    if (video.buffered.length) {{
      const buf = Math.max(0, video.buffered.end(video.buffered.length-1) - video.currentTime);
      document.getElementById('st-buf').textContent = buf.toFixed(1)+'s';
    }}
    const q = video.getVideoPlaybackQuality?.();
    if (q) document.getElementById('st-drop').textContent = q.droppedVideoFrames;
  }}, 1000);
}}

function setQuality(val) {{
  if (!hlsInst) return;
  hlsInst.currentLevel = parseInt(val);
}}

// ── Delete ─────────────────────────────────────────────────────────────────
async function deleteAsset(id, name) {{
  if (!confirm('Delete "' + name + '"?')) return;
  if (playingId === id) closePlayer();
  await fetch(API + '/api/assets/' + id, {{method:'DELETE'}});
  if (selectedId === id) selectedId = '';
  loadAssets();
  toast('Asset deleted');
}}

// ── Utilities ──────────────────────────────────────────────────────────────
function fmtSize(b) {{
  if (!b) return '—';
  if (b>1e9) return (b/1e9).toFixed(2)+' GB';
  if (b>1e6) return (b/1e6).toFixed(1)+' MB';
  return (b/1024).toFixed(0)+' KB';
}}
function fmtDur(s) {{
  if (!s) return '—';
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sec=Math.floor(s%60);
  return h>0?`${{h}}:${{String(m).padStart(2,'0')}}:${{String(sec).padStart(2,'0')}}`:
             `${{m}}:${{String(sec).padStart(2,'0')}}`;
}}
function fmtAgo(ts) {{
  const d=Date.now()/1000-ts;
  if(d<60) return 'just now';
  if(d<3600) return Math.floor(d/60)+'m ago';
  if(d<86400) return Math.floor(d/3600)+'h ago';
  return Math.floor(d/86400)+'d ago';
}}
function esc(s) {{ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }}
function cp(text, btn) {{
  navigator.clipboard?.writeText(text).catch(()=>{{}});
  if (btn) {{ btn.textContent='✓ COPIED'; btn.classList.add('ok');
    setTimeout(()=>{{btn.textContent='COPY';btn.classList.remove('ok');}},2000); }}
  toast('Copied to clipboard','info');
}}
let _tid=0;
function toast(msg,type='') {{
  const id=++_tid, el=document.createElement('div');
  el.className='toast'+(type?' '+type:''); el.textContent=msg;
  document.getElementById('toasts').appendChild(el);
  setTimeout(()=>el.remove(),3500);
}}

// ── Boot ───────────────────────────────────────────────────────────────────
loadAssets();
setInterval(loadAssets, 2500);
if (selectedId) setTimeout(()=>{{
  fetch(API+'/api/assets/'+selectedId).then(r=>r.json()).then(renderDetail).catch(()=>{{}});
}}, 300);
</script>
</body></html>"""


async def serve_ui(request: web.Request) -> web.Response:
    asset_id = request.match_info.get("asset_id", "")
    return web.Response(text=_build_ui_html(asset_id), content_type="text/html")


# ── App factory ────────────────────────────────────────────────────────────
def create_app() -> web.Application:
    app = web.Application(client_max_size=2 * 1024 ** 3)   # 2 GB upload limit

    app.router.add_route("OPTIONS", "/{path_info:.*}", handle_options)

    # REST API
    app.router.add_post  ("/api/upload",               api_upload)
    app.router.add_get   ("/api/assets",               api_list_assets)
    app.router.add_get   ("/api/assets/{asset_id}",    api_get_asset)
    app.router.add_delete("/api/assets/{asset_id}",    api_delete_asset)
    app.router.add_get   ("/health",                   api_health)

    # VOD HLS delivery
    app.router.add_get("/vod/{asset_id}/master.m3u8",         serve_vod_master)
    app.router.add_get("/vod/{asset_id}/{variant}/{filename}", serve_vod_file)
    app.router.add_get("/vod/{asset_id}/thumb.jpg",           serve_thumb)

    # Web UI
    app.router.add_get("/",                    serve_ui)
    app.router.add_get("/player/{asset_id}",   serve_ui)

    return app


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=int(os.environ.get("SF_PORT", ORIGIN_PORT)))
    parser.add_argument("--host", default=os.environ.get("SF_HOST", ORIGIN_HOST))
    cli = parser.parse_args()

    _load()
    app = create_app()
    log.info(f"StreamForge VOD Server starting on http://{cli.host}:{cli.port}")
    log.info(f"  UI:      http://localhost:{cli.port}/")
    log.info(f"  Upload:  POST http://localhost:{cli.port}/api/upload")
    log.info(f"  Assets:  GET  http://localhost:{cli.port}/api/assets")
    web.run_app(app, host=cli.host, port=cli.port, access_log=None)