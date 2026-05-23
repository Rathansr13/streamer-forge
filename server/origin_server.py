"""
StreamForge HLS Origin Server
──────────────────────────────
Serves HLS manifests and MPEG-TS segments to players
(or CDN edge nodes pulling from origin).

Endpoints:
  GET /live/{stream_id}/master.m3u8           → master manifest
  GET /live/{stream_id}/{variant}/{file}      → variant manifest or segment
  GET /vod/{asset_id}/master.m3u8            → VOD master manifest
  GET /status                                 → JSON stream status
  GET /                                       → Web player page
  WS  /ws/status                             → Real-time status WebSocket

CDN cache headers:
  .m3u8  → Cache-Control: no-cache (live) / max-age=300 (VOD)
  .ts    → Cache-Control: max-age=86400 (segments are immutable)
"""

import asyncio
import json
import logging
import mimetypes
import os
import time
from pathlib import Path

from aiohttp import web
import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ORIGIN] %(message)s")
log = logging.getLogger("origin")

HLS_OUTPUT_DIR = Path(__file__).parent.parent / "hls_output"
TEMPLATES_DIR  = Path(__file__).parent.parent / "templates"
ORIGIN_HOST    = "0.0.0.0"
ORIGIN_PORT    = 8080

# CORS headers for CDN pull and browser players
CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Range",
    "Access-Control-Expose-Headers": "Content-Length, Content-Range",
}


def build_cors_response(body, content_type, cache_control, status=200):
    headers = {**CORS_HEADERS, "Cache-Control": cache_control, "Content-Type": content_type}
    return web.Response(body=body, status=status, headers=headers)


# ── Live HLS endpoints ─────────────────────────────────────────────────────
async def serve_live_master(request: web.Request) -> web.Response:
    stream_id = request.match_info["stream_id"]
    manifest_path = HLS_OUTPUT_DIR / stream_id / "master.m3u8"

    if not manifest_path.exists():
        return web.Response(status=404, text=f"Stream {stream_id} not found or not ready")

    # Check at least one variant has segments before serving master
    # This prevents the player from starting before FFmpeg has output anything
    has_segments = False
    for variant in ["360p", "480p", "720p", "1080p"]:
        vdir = HLS_OUTPUT_DIR / stream_id / variant
        if vdir.exists() and any(vdir.glob("seg_*.ts")):
            has_segments = True
            break

    if not has_segments:
        # Return 503 — player/CDN will retry
        headers = {**CORS_HEADERS, "Retry-After": "2", "Content-Type": "text/plain"}
        return web.Response(status=503, text="Stream starting, please wait...", headers=headers)

    content = manifest_path.read_bytes()
    log.info(f"[PLAY] master.m3u8 for stream {stream_id}")
    return build_cors_response(content, "application/vnd.apple.mpegurl", "no-cache, no-store")


async def serve_live_variant(request: web.Request) -> web.Response:
    stream_id = request.match_info["stream_id"]
    variant   = request.match_info["variant"]
    filename  = request.match_info["filename"]

    file_path = HLS_OUTPUT_DIR / stream_id / variant / filename

    if not file_path.exists():
        return web.Response(status=404, text=f"Segment not found: {filename}")

    content = file_path.read_bytes()

    if filename.endswith(".m3u8"):
        # Variant manifest — no cache (live sliding window)
        return build_cors_response(content, "application/vnd.apple.mpegurl", "no-cache, no-store")
    elif filename.endswith(".ts"):
        # Segment — immutable, cache forever at CDN
        return build_cors_response(content, "video/mp2t", "public, max-age=86400, immutable")
    else:
        return web.Response(status=400, text="Unknown file type")


# ── Status API ─────────────────────────────────────────────────────────────
async def serve_status(request: web.Request) -> web.Response:
    state_path = HLS_OUTPUT_DIR / "stream_state.json"
    if state_path.exists():
        state = json.loads(state_path.read_text())
    else:
        state = {}

    # Augment with manifest availability
    for stream_id, info in state.items():
        master = HLS_OUTPUT_DIR / stream_id / "master.m3u8"
        info["hls_ready"] = master.exists()
        info["play_url"]  = f"http://localhost:{ORIGIN_PORT}/live/{stream_id}/master.m3u8"

    headers = {**CORS_HEADERS, "Content-Type": "application/json"}
    return web.Response(
        text=json.dumps(state, indent=2),
        headers=headers
    )


async def serve_stream_status(request: web.Request) -> web.Response:
    stream_id = request.match_info["stream_id"]
    state_path = HLS_OUTPUT_DIR / "stream_state.json"

    data = {}
    if state_path.exists():
        all_state = json.loads(state_path.read_text())
        data = all_state.get(stream_id, {"error": f"Stream {stream_id} not found"})
        master = HLS_OUTPUT_DIR / stream_id / "master.m3u8"
        data["hls_ready"] = master.exists()
        data["play_url"]  = f"http://localhost:{ORIGIN_PORT}/live/{stream_id}/master.m3u8"

        # Count segments per variant
        seg_counts = {}
        for v in ["1080p", "720p", "480p", "360p"]:
            vdir = HLS_OUTPUT_DIR / stream_id / v
            if vdir.exists():
                seg_counts[v] = len(list(vdir.glob("seg_*.ts")))
        data["segment_counts"] = seg_counts

    headers = {**CORS_HEADERS, "Content-Type": "application/json"}
    return web.Response(text=json.dumps(data, indent=2), headers=headers)


# ── WebSocket real-time status push ──────────────────────────────────────
async def ws_status(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    log.info("WebSocket client connected for status")

    try:
        while not ws.closed:
            state_path = HLS_OUTPUT_DIR / "stream_state.json"
            state = {}
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text())
                except json.JSONDecodeError:
                    pass

            for sid, info in state.items():
                master = HLS_OUTPUT_DIR / sid / "master.m3u8"
                info["hls_ready"] = master.exists()
                info["play_url"]  = f"http://localhost:{ORIGIN_PORT}/live/{sid}/master.m3u8"

            await ws.send_str(json.dumps({
                "type": "status_update",
                "timestamp": time.time(),
                "streams": state,
            }))
            await asyncio.sleep(2)
    except Exception as e:
        log.info(f"WebSocket closed: {e}")

    return ws


# ── Web player ─────────────────────────────────────────────────────────────
async def serve_player(request: web.Request) -> web.Response:
    stream_id = request.match_info.get("stream_id", "ls-001")
    play_url  = f"http://localhost:{ORIGIN_PORT}/live/{stream_id}/master.m3u8"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>StreamForge Player — {stream_id}</title>
  <script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: #0a0d12; color: #e2e8f0; font-family: 'DM Mono', monospace; padding: 20px; }}
    h1 {{ font-size: 20px; margin-bottom: 16px; color: #00ff88; }}
    #wrap {{ position: relative; max-width: 960px; background: #000; border-radius: 8px; overflow: hidden; }}
    video {{ width: 100%; display: block; min-height: 200px; }}
    #overlay {{ position: absolute; inset: 0; background: #000c; display: flex; flex-direction: column;
                align-items: center; justify-content: center; gap: 14px; }}
    #overlay.hidden {{ display: none; }}
    .spinner {{ width: 40px; height: 40px; border: 3px solid #ffffff20;
                border-top-color: #00ff88; border-radius: 50%;
                animation: spin 0.8s linear infinite; }}
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    .overlay-msg {{ font-size: 14px; color: #aaa; }}
    .overlay-sub {{ font-size: 11px; color: #555; }}
    .stats {{ margin-top: 14px; font-size: 12px; color: #6b7280; display: flex; gap: 10px; flex-wrap: wrap; }}
    .stat {{ background: #111621; border: 1px solid #1e2535; border-radius: 6px; padding: 8px 14px; }}
    .stat span {{ color: #00c8ff; font-weight: bold; }}
    .url {{ margin-top: 12px; font-size: 11px; color: #4b5563; word-break: break-all; }}
    .badge {{ display: inline-block; padding: 2px 8px; font-size: 10px; border-radius: 4px;
              margin-left: 10px; border: 1px solid; }}
    .badge-live    {{ background:#00ff8820; border-color:#00ff88; color:#00ff88; }}
    .badge-loading {{ background:#ffaa0020; border-color:#ffaa00; color:#ffaa00; }}
    .badge-error   {{ background:#ff003320; border-color:#ff0033; color:#ff6b6b; }}
    select {{ background:#111621; border:1px solid #1e2535; color:#e2e8f0; padding:6px 10px;
              border-radius:6px; font-size:12px; margin-top:12px; cursor:pointer; }}
    #errbox {{ margin-top:12px; background:#ff003315; border:1px solid #ff003340;
               border-radius:8px; padding:12px 16px; font-size:12px; color:#ff9999; display:none; }}
  </style>
</head>
<body>
  <h1>⬡ StreamForge Player <span id="badge" class="badge badge-loading">LOADING</span></h1>

  <div id="wrap">
    <video id="video" controls autoplay muted playsinline></video>
    <div id="overlay">
      <div class="spinner"></div>
      <div class="overlay-msg" id="overlay-msg">Waiting for stream segments...</div>
      <div class="overlay-sub" id="overlay-sub">FFmpeg is encoding your video</div>
    </div>
  </div>

  <div>
    <select id="quality" onchange="switchQuality(this.value)">
      <option value="">Auto (ABR)</option>
      <option value="1080">1080p — 4.5 Mbps</option>
      <option value="720">720p — 2.5 Mbps</option>
      <option value="480">480p — 1.2 Mbps</option>
      <option value="360">360p — 600 Kbps</option>
    </select>
  </div>

  <div class="stats">
    <div class="stat">Quality: <span id="level">—</span></div>
    <div class="stat">Bitrate: <span id="bitrate">—</span></div>
    <div class="stat">Buffer: <span id="buffer">—</span>s</div>
    <div class="stat">Dropped: <span id="dropped">0</span></div>
    <div class="stat">Segments: <span id="segs">0</span></div>
    <div class="stat">Retries: <span id="retries">0</span></div>
  </div>
  <div id="errbox"></div>
  <div class="url">HLS URL: {play_url}</div>

  <script>
    const video   = document.getElementById('video');
    const overlay = document.getElementById('overlay');
    const badge   = document.getElementById('badge');
    const src     = "{play_url}";
    let hls, segCount = 0, retries = 0;

    function fmt(n) {{
      return n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(0)+'K' : String(n);
    }}

    function setLive() {{
      overlay.classList.add('hidden');
      badge.textContent = 'LIVE';
      badge.className = 'badge badge-live';
    }}

    function setLoading(msg, sub) {{
      overlay.classList.remove('hidden');
      badge.textContent = 'LOADING';
      badge.className = 'badge badge-loading';
      if (msg) document.getElementById('overlay-msg').textContent = msg;
      if (sub) document.getElementById('overlay-sub').textContent = sub;
    }}

    function setError(msg) {{
      badge.textContent = 'ERROR';
      badge.className = 'badge badge-error';
      document.getElementById('errbox').style.display = 'block';
      document.getElementById('errbox').textContent = '⚠ ' + msg;
    }}

    function switchQuality(h) {{
      if (!hls) return;
      if (!h) {{ hls.currentLevel = -1; return; }}
      const idx = hls.levels.findIndex(l => l.height == parseInt(h));
      if (idx >= 0) hls.currentLevel = idx;
    }}

    function initHls() {{
      if (hls) {{ hls.destroy(); }}

      hls = new Hls({{
        // Buffer settings — give the player plenty of runway
        maxBufferLength:             60,
        maxMaxBufferLength:          120,
        maxBufferHole:               0.5,
        liveSyncDurationCount:       4,      // stay 4 segments behind live edge
        liveMaxLatencyDurationCount: 12,
        // Recovery settings
        manifestLoadingMaxRetry:      20,
        manifestLoadingRetryDelay:    1500,
        levelLoadingMaxRetry:         10,
        fragLoadingMaxRetry:          6,
        fragLoadingRetryDelay:        500,
        // ABR
        startLevel:                   -1,    // auto pick lowest first
        abrEwmaDefaultEstimate:       500000,
      }});

      hls.loadSource(src);
      hls.attachMedia(video);

      hls.on(Hls.Events.MANIFEST_PARSED, (e, d) => {{
        console.log('Manifest parsed, levels:', d.levels.length);
        setLoading('Buffering...', 'Fetching first segments');
        video.play().catch(() => {{}});
      }});

      hls.on(Hls.Events.FRAG_BUFFERED, () => {{
        // As soon as first fragment is buffered, hide overlay
        setLive();
      }});

      hls.on(Hls.Events.LEVEL_SWITCHED, (e, d) => {{
        const l = hls.levels[d.level];
        document.getElementById('level').textContent = l.height + 'p';
        document.getElementById('bitrate').textContent = fmt(l.bitrate) + 'bps';
      }});

      hls.on(Hls.Events.FRAG_LOADED, () => {{
        segCount++;
        document.getElementById('segs').textContent = segCount;
      }});

      hls.on(Hls.Events.ERROR, (e, d) => {{
        console.warn('HLS error', d.type, d.details, d.fatal);

        if (d.details === 'manifestLoadError' && d.response && d.response.code === 503) {{
          // Stream not ready yet — keep retrying silently
          retries++;
          document.getElementById('retries').textContent = retries;
          setLoading('Stream is starting...', 'Retry ' + retries + ' — waiting for segments');
          return;  // HLS.js will retry automatically per manifestLoadingMaxRetry
        }}

        if (d.fatal) {{
          if (d.type === Hls.ErrorTypes.NETWORK_ERROR) {{
            retries++;
            document.getElementById('retries').textContent = retries;
            setLoading('Reconnecting...', 'Network error — retry ' + retries);
            setTimeout(() => initHls(), 2000);
          }} else if (d.type === Hls.ErrorTypes.MEDIA_ERROR) {{
            setLoading('Recovering...', 'Media error');
            hls.recoverMediaError();
          }} else {{
            setError(d.details + ' — reloading in 3s');
            setTimeout(() => initHls(), 3000);
          }}
        }}
      }});
    }}

    // Stats update loop
    setInterval(() => {{
      if (video.buffered.length) {{
        const buf = Math.max(0, video.buffered.end(video.buffered.length - 1) - video.currentTime);
        document.getElementById('buffer').textContent = buf.toFixed(1);
      }}
      const q = video.getVideoPlaybackQuality ? video.getVideoPlaybackQuality() : null;
      if (q) document.getElementById('dropped').textContent = q.droppedVideoFrames;
    }}, 1000);

    // Start
    if (Hls.isSupported()) {{
      initHls();
    }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
      video.src = src;
      video.addEventListener('loadedmetadata', setLive);
    }} else {{
      setError('HLS not supported in this browser');
    }}
  </script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def serve_index(request: web.Request) -> web.Response:
    """List all active streams."""
    state_path = HLS_OUTPUT_DIR / "stream_state.json"
    streams = {}
    if state_path.exists():
        try:
            streams = json.loads(state_path.read_text())
        except:
            pass

    items = ""
    for sid, info in streams.items():
        status_color = "#00ff88" if info.get("status") == "live" else "#6b7280"
        items += f"""
        <div style="background:#111621;border:1px solid #1e2535;border-radius:10px;padding:18px;margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div>
              <div style="font-size:15px;color:#fff">{info.get('name', sid)}</div>
              <div style="font-size:11px;color:#4b5563;margin-top:4px">
                {sid} · {info.get('region','—')} · Uptime: {info.get('uptime_fmt','—')} · {info.get('bitrate_mbps','—')} Mbps
              </div>
            </div>
            <div style="display:flex;align-items:center;gap:12px">
              <span style="color:{status_color};font-size:11px;border:1px solid {status_color};
                    padding:2px 8px;border-radius:4px">{info.get('status','—').upper()}</span>
              <a href="/player/{sid}" style="background:linear-gradient(135deg,#00ff88,#00c8ff);
                 color:#0a0d12;padding:8px 16px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:bold">
                ▶ WATCH
              </a>
            </div>
          </div>
          <div style="margin-top:10px;font-size:11px;color:#00c8ff;background:#0a0d12;padding:8px 12px;border-radius:6px">
            HLS: http://localhost:{ORIGIN_PORT}/live/{sid}/master.m3u8
          </div>
        </div>"""

    if not items:
        items = '<div style="color:#4b5563;text-align:center;padding:40px">No active streams. Start the broadcaster!</div>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>StreamForge — Live Streams</title>
  <meta http-equiv="refresh" content="5">
  <style>
    * {{ box-sizing:border-box;margin:0;padding:0; }}
    body {{ background:#0a0d12;color:#e2e8f0;font-family:'DM Mono',monospace;padding:28px; }}
    h1 {{ font-size:22px;color:#fff;margin-bottom:6px; }}
    .sub {{ color:#4b5563;font-size:12px;margin-bottom:24px; }}
    a {{ color:inherit; }}
  </style>
</head>
<body>
  <h1>⬡ StreamForge <span style="color:#00ff88">Origin Server</span></h1>
  <div class="sub">Live streams · Auto-refresh every 5s · Port {ORIGIN_PORT}</div>
  {items}
  <div style="margin-top:20px;font-size:11px;color:#1e2535">
    API: <a href="/status" style="color:#4b5563">/status</a> &nbsp;|&nbsp;
    Docs: HLS endpoints at /live/{{stream_id}}/master.m3u8
  </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ── App factory ────────────────────────────────────────────────────────────
def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",                                     serve_index)
    app.router.add_get("/player/{stream_id}",                   serve_player)
    app.router.add_get("/live/{stream_id}/master.m3u8",         serve_live_master)
    app.router.add_get("/live/{stream_id}/{variant}/{filename}", serve_live_variant)
    app.router.add_get("/status",                               serve_status)
    app.router.add_get("/status/{stream_id}",                   serve_stream_status)
    app.router.add_get("/ws/status",                            ws_status)
    return app


if __name__ == "__main__":
    HLS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    app = create_app()
    log.info(f"StreamForge Origin Server starting on http://{ORIGIN_HOST}:{ORIGIN_PORT}")
    log.info(f"  Player:    http://localhost:{ORIGIN_PORT}/player/{{stream_id}}")
    log.info(f"  Status:    http://localhost:{ORIGIN_PORT}/status")
    log.info(f"  HLS:       http://localhost:{ORIGIN_PORT}/live/{{stream_id}}/master.m3u8")
    web.run_app(app, host=ORIGIN_HOST, port=ORIGIN_PORT, access_log=None)