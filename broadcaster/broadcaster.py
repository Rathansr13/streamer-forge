"""
StreamForge VOD Upload Client (CLI)
─────────────────────────────────────
Command-line tool to upload a video file to the StreamForge
VOD API and get back an asset_id.

Previously this was an RTMP broadcaster for live streaming.
Now repurposed as a VOD ingest client — no live stream, no TCP
handshake, just a clean multipart HTTP upload with progress.

Usage:
  python broadcaster.py --file my_video.mp4
  python broadcaster.py --file clip.mp4 --name "Product Demo v3"
  python broadcaster.py --file clip.mp4 --server http://localhost:8080
  python broadcaster.py --list                      # list all assets
  python broadcaster.py --play <asset_id>           # print play URL
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path


API_BASE = "http://localhost:8080"
CHUNK    = 65536   # 64 KB read chunks for upload progress


def fmt_size(b: int) -> str:
    if b > 1e9: return f"{b/1e9:.2f} GB"
    if b > 1e6: return f"{b/1e6:.1f} MB"
    return f"{b/1024:.0f} KB"


def fmt_dur(s) -> str:
    if not s: return "—"
    s = int(s)
    h, m, sec = s // 3600, (s % 3600) // 60, s % 60
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def fmt_ago(ts: float) -> str:
    d = time.time() - ts
    if d < 60:    return "just now"
    if d < 3600:  return f"{int(d/60)}m ago"
    if d < 86400: return f"{int(d/3600)}h ago"
    return f"{int(d/86400)}d ago"


# ── Upload with progress bar ───────────────────────────────────────────────
def upload_file(file_path: str, name: str, server: str) -> dict:
    path = Path(file_path)
    if not path.exists():
        print(f"✗ File not found: {file_path}")
        sys.exit(1)

    file_size = path.stat().st_size
    name      = name or path.stem

    print(f"""
\033[96mStreamForge VOD Upload\033[0m
  File:   {path.name}
  Size:   {fmt_size(file_size)}
  Name:   {name}
  Server: {server}
""")

    # Build multipart body manually for progress tracking
    boundary = "----SF_BOUNDARY_" + str(int(time.time()))
    b = boundary.encode()

    # Header parts
    header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="name"\r\n\r\n'
        f"{name}\r\n"
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{path.name}"\r\n'
        f"Content-Type: video/mp4\r\n\r\n"
    ).encode()
    footer = f"\r\n--{boundary}--\r\n".encode()

    total_size = len(header) + file_size + len(footer)

    print(f"  \033[90mUploading {fmt_size(file_size)}...\033[0m")
    t0 = time.time()
    uploaded = 0

    try:
        import http.client, urllib.parse
        parsed = urllib.parse.urlparse(server)
        conn   = http.client.HTTPConnection(parsed.netloc, timeout=300)
        conn.connect()
        conn.putrequest("POST", "/api/upload")
        conn.putheader("Content-Type",   f"multipart/form-data; boundary={boundary}")
        conn.putheader("Content-Length", str(total_size))
        conn.endheaders()

        # Send header part
        conn.send(header)
        uploaded += len(header)

        # Send file with progress
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK)
                if not chunk:
                    break
                conn.send(chunk)
                uploaded += len(chunk)
                pct      = int((uploaded / total_size) * 100)
                elapsed  = time.time() - t0
                speed    = uploaded / max(elapsed, 0.1)
                bar_w    = 30
                filled   = int(bar_w * pct / 100)
                bar      = "█" * filled + "░" * (bar_w - filled)
                print(f"\r  [{bar}] {pct:3d}%  {fmt_size(int(speed))}/s  ", end="", flush=True)

        # Send footer
        conn.send(footer)

        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        conn.close()

        elapsed = time.time() - t0
        print(f"\r  \033[92m✓ Upload complete in {elapsed:.1f}s ({fmt_size(int(file_size/elapsed))}/s)\033[0m            ")

        if resp.status != 202:
            print(f"\033[91m✗ Server returned {resp.status}: {body}\033[0m")
            sys.exit(1)

        return json.loads(body)

    except ConnectionRefusedError:
        print(f"\033[91m✗ Cannot connect to {server}. Is the server running?\033[0m")
        print("  Start it with: python run_all.py")
        sys.exit(1)
    except Exception as e:
        print(f"\033[91m✗ Upload error: {e}\033[0m")
        sys.exit(1)


# ── Poll until ready ───────────────────────────────────────────────────────
def poll_asset(asset_id: str, server: str) -> dict:
    url = f"{server}/api/assets/{asset_id}"
    print(f"\n  \033[90mPolling encoding status...\033[0m")

    while True:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                data = json.loads(r.read())
        except Exception as e:
            print(f"  Poll error: {e}")
            time.sleep(3)
            continue

        status   = data.get("status", "?")
        progress = data.get("encode_progress", 0)
        bar_w    = 24
        filled   = int(bar_w * progress / 100)
        bar      = "█" * filled + "░" * (bar_w - filled)
        print(f"\r  [{bar}] {progress:3d}%  {status:12s}", end="", flush=True)

        if status == "ready":
            print()
            return data
        if status == "error":
            print(f"\n\033[91m✗ Encoding failed: {data.get('error_msg','unknown')}\033[0m")
            sys.exit(1)
        time.sleep(2)


# ── List assets ────────────────────────────────────────────────────────────
def list_assets(server: str):
    try:
        with urllib.request.urlopen(f"{server}/api/assets", timeout=10) as r:
            data = json.loads(r.read())
    except ConnectionRefusedError:
        print(f"\033[91m✗ Cannot connect to {server}\033[0m")
        sys.exit(1)

    assets = data.get("assets", [])
    if not assets:
        print("No assets found.")
        return

    print(f"\n\033[96mStreamForge Asset Library ({len(assets)} assets)\033[0m\n")
    print(f"  {'ASSET ID':<24}  {'NAME':<28}  {'STATUS':<10}  {'SIZE':>8}  {'DUR':>7}  {'CREATED'}")
    print("  " + "─" * 95)
    for a in assets:
        status  = a.get("status","?")
        color   = "\033[92m" if status == "ready" else "\033[93m" if status in ("encoding","packaging") else "\033[91m"
        reset   = "\033[0m"
        print(
            f"  {a['asset_id']:<24}  {a.get('name','—')[:28]:<28}  "
            f"{color}{status:<10}{reset}  "
            f"{fmt_size(a.get('file_size',0)):>8}  "
            f"{fmt_dur(a.get('duration')):>7}  "
            f"{fmt_ago(a.get('created_at',0))}"
        )
    print()


# ── Print play info ────────────────────────────────────────────────────────
def print_play_info(asset_id: str, server: str):
    try:
        with urllib.request.urlopen(f"{server}/api/assets/{asset_id}", timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"✗ {e}")
        sys.exit(1)

    status = data.get("status", "?")
    if status != "ready":
        print(f"Asset {asset_id} is not ready yet (status: {status})")
        sys.exit(1)

    print(f"""
\033[96mAsset: {data.get('name','—')}\033[0m
  Asset ID:   {asset_id}
  Status:     {status}
  Duration:   {fmt_dur(data.get('duration'))}
  Resolution: {data.get('width','?')}×{data.get('height','?')}
  Variants:   {', '.join(data.get('variants',[]))}

\033[93mDelivery URLs:\033[0m
  Browser Player:  {server}/player/{asset_id}
  HLS Master:      {server}/vod/{asset_id}/master.m3u8
  Thumbnail:       {server}/vod/{asset_id}/thumb.jpg
""")


# ── CLI ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="StreamForge VOD Upload Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python broadcaster.py --file video.mp4
  python broadcaster.py --file video.mp4 --name "My Video" --wait
  python broadcaster.py --list
  python broadcaster.py --play vod-ABC123
        """
    )
    parser.add_argument("--file",   help="Video file to upload")
    parser.add_argument("--name",   default="", help="Asset name (default: filename)")
    parser.add_argument("--server", default=API_BASE, help="Server URL")
    parser.add_argument("--wait",   action="store_true", help="Wait for encoding to finish")
    parser.add_argument("--list",   action="store_true", help="List all assets")
    parser.add_argument("--play",   metavar="ASSET_ID",  help="Print play URLs for an asset")
    args = parser.parse_args()

    if args.list:
        list_assets(args.server)
        return

    if args.play:
        print_play_info(args.play, args.server)
        return

    if not args.file:
        parser.print_help()
        sys.exit(1)

    # Upload
    result = upload_file(args.file, args.name, args.server)
    asset_id = result["asset_id"]

    print(f"""
\033[92m✓ Asset created!\033[0m
  Asset ID:    \033[97m{asset_id}\033[0m
  Name:        {result.get('name','—')}
  Status:      {result.get('status','queued')}
  Poll URL:    {args.server}/api/assets/{asset_id}
  Player URL:  {args.server}/player/{asset_id}
""")

    if args.wait:
        data = poll_asset(asset_id, args.server)
        print(f"""
\033[92m✓ Encoding complete!\033[0m
  Duration:   {fmt_dur(data.get('duration'))}
  Resolution: {data.get('width','?')}×{data.get('height','?')}
  Variants:   {', '.join(data.get('variants',[]))}

\033[93mPlay it:\033[0m
  {args.server}/player/{asset_id}
  {args.server}/vod/{asset_id}/master.m3u8
""")


if __name__ == "__main__":
    main()