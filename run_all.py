"""
StreamForge VOD — Orchestrator
───────────────────────────────
Starts the VOD origin server only.
No ingest server. No broadcaster. No live stream.

Usage:
  python run_all.py
  python run_all.py --port 8080
  python run_all.py --host 0.0.0.0

Then open: http://localhost:8080
  - Upload a video
  - Get your asset_id
  - Play via the built-in ABR player
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# Load .env before anything else
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / "server" / ".env")
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("orchestrator")

BASE_DIR   = Path(__file__).parent
SERVER_DIR = BASE_DIR / "server"


def print_banner():
    print("""\033[92m
  ██████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗
 ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║
 ╚█████╗    ██║   ██████╔╝█████╗  ███████║██╔████╔██║
  ╚═══██╗   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║
 ██████╔╝   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║
 ╚═════╝    ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝
\033[96m         V O D  ·  P I P E L I N E\033[0m
""")


def check_deps():
    """Check Python packages, FFmpeg, and MongoDB before starting."""
    # ── Python packages ────────────────────────────────────────────────────
    missing = []
    for pkg in ["aiohttp", "pymongo", "dotenv"]:
        try:
            __import__(pkg if pkg != "dotenv" else "dotenv")
        except ImportError:
            missing.append(pkg if pkg != "dotenv" else "python-dotenv")
    if missing:
        print(f"\033[91mMissing packages: {', '.join(missing)}\033[0m")
        print(f"Run: pip install {' '.join(missing)}")
        sys.exit(1)

    # ── FFmpeg ─────────────────────────────────────────────────────────────
    import shutil
    found = (
        os.environ.get("FFMPEG_PATH") or
        shutil.which("ffmpeg") or
        _imageio_ffmpeg()
    )
    if not found:
        print("""\033[91m
═══ FFmpeg not found ═══
  pip install imageio-ffmpeg    (quickest fix)
  winget install ffmpeg         (Windows)
  brew install ffmpeg           (macOS)
  sudo apt install ffmpeg       (Linux)
\033[0m""")
        sys.exit(1)
    log.info(f"  FFmpeg:  {found}")

    # ── MongoDB ────────────────────────────────────────────────────────────
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db  = os.environ.get("MONGO_DB",  "streamforge")
    try:
        from pymongo import MongoClient
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")
        count = client[mongo_db]["assets"].count_documents({})
        client.close()
        log.info(f"  MongoDB: {mongo_uri} / {mongo_db}  ({count} assets)")
    except Exception as e:
        print(f"""\033[91m
═══ MongoDB not reachable ═══
  URI: {mongo_uri}
  DB:  {mongo_db}
  Error: {e}

  Options:
    Local:  Install MongoDB Community Edition and start mongod
            https://www.mongodb.com/docs/manual/installation/
    Cloud:  Use MongoDB Atlas free tier (512 MB free)
            https://www.mongodb.com/atlas/database
            Then set MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
            in server/.env
\033[0m""")
        sys.exit(1)


def _imageio_ffmpeg():
    try:
        import imageio_ffmpeg
        p = imageio_ffmpeg.get_ffmpeg_exe()
        return p if p and Path(p).exists() else None
    except ImportError:
        return None


class ComponentProcess:
    def __init__(self, name: str, cmd: list):
        self.name = name
        self.cmd  = cmd
        self.proc = None

    def start(self):
        log.info(f"  Starting {self.name}...")
        self.proc = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
            text=True,
        )
        threading.Thread(target=self._stream_logs, daemon=True).start()
        log.info(f"  ✓ {self.name} PID={self.proc.pid}")

    def _stream_logs(self):
        colors = {"VOD Server": "\033[94m"}
        color  = colors.get(self.name, "\033[96m")
        reset  = "\033[0m"
        label  = self.name[:12].ljust(12)
        for line in self.proc.stdout:
            line = line.rstrip()
            if line:
                print(f"{color}[{label}]{reset} {line}")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            log.info(f"  Stopping {self.name}...")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def is_running(self):
        return self.proc is not None and self.proc.poll() is None


def main():
    parser = argparse.ArgumentParser(
        description="StreamForge VOD — Upload, Encode, Play",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all.py              Start on default port 8080
  python run_all.py --port 9000  Start on custom port
        """
    )
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    print_banner()
    print("\033[92m═══ StreamForge VOD Starting ═══\033[0m")
    check_deps()

    # Create output dirs
    for d in ["hls_output", "uploads"]:
        (BASE_DIR / d).mkdir(parents=True, exist_ok=True)
    log.info(f"  HLS dir: {BASE_DIR / 'hls_output'}")
    log.info(f"  Uploads: {BASE_DIR / 'uploads'}")

    # Migrate assets.json → MongoDB if it still exists
    assets_json = BASE_DIR / "assets.json"
    if assets_json.exists():
        _migrate_assets_json(assets_json)

    # Start VOD origin server
    env = os.environ.copy()
    env["SF_PORT"] = str(args.port)
    env["SF_HOST"] = args.host

    server = ComponentProcess(
        "VOD Server",
        [sys.executable, str(SERVER_DIR / "origin_server.py"),
         "--port", str(args.port), "--host", args.host]
    )
    server.proc = subprocess.Popen(
        server.cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env, text=True,
    )
    threading.Thread(target=server._stream_logs, daemon=True).start()
    time.sleep(1.5)
    log.info(f"  ✓ VOD Server PID={server.proc.pid}")

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db  = os.environ.get("MONGO_DB",  "streamforge")

    print(f"""
\033[92m═══ StreamForge VOD RUNNING ═══\033[0m

  \033[96m🌐 Web UI:\033[0m        http://localhost:{args.port}/
  \033[96m📤 Upload API:\033[0m    POST http://localhost:{args.port}/api/upload
  \033[96m📋 Asset List:\033[0m    GET  http://localhost:{args.port}/api/assets
  \033[96m▶  Player:\033[0m        http://localhost:{args.port}/player/<asset_id>
  \033[96m📡 HLS URL:\033[0m       http://localhost:{args.port}/vod/<asset_id>/master.m3u8
  \033[96m🍃 MongoDB:\033[0m       {mongo_uri} / {mongo_db}
  \033[96m❤  Health:\033[0m        http://localhost:{args.port}/health

  \033[93mWorkflow:\033[0m
    1. Open http://localhost:{args.port}/
    2. Drop a video file and click Upload & Ingest
    3. Copy your asset_id once encoding completes
    4. Hit ▶ Play in ABR Player

  \033[90mPress Ctrl+C to stop\033[0m
""")

    # Signal handling
    shutdown = threading.Event()

    def handle_signal(sig, frame):
        print("\n")
        server.stop()
        shutdown.set()

    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while not shutdown.is_set():
            time.sleep(3)
            if not server.is_running():
                log.warning(f"  ⚠ VOD Server stopped (exit code: {server.proc.returncode})")
                break
    except KeyboardInterrupt:
        server.stop()


def _migrate_assets_json(json_path: Path):
    """
    One-time migration: move assets.json records into MongoDB.
    Renames the file to assets.json.migrated when done.
    """
    import json
    try:
        from pymongo import MongoClient
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        mongo_db  = os.environ.get("MONGO_DB",  "streamforge")
        client    = MongoClient(mongo_uri, serverSelectionTimeoutMS=4000)
        col       = client[mongo_db]["assets"]

        raw = json.loads(json_path.read_text())
        migrated = 0
        for aid, d in raw.items():
            d.setdefault("variants",    [])
            d.setdefault("r2_uploaded", False)
            d.setdefault("r2_base_url", "")
            d["_id"] = aid
            try:
                col.insert_one(d)
                migrated += 1
            except Exception:
                pass  # already exists — skip

        client.close()
        json_path.rename(json_path.with_suffix(".json.migrated"))
        log.info(f"  ✓ Migrated {migrated} assets from assets.json → MongoDB")
    except Exception as e:
        log.warning(f"  ⚠ assets.json migration failed: {e} — continuing anyway")


if __name__ == "__main__":
    main()