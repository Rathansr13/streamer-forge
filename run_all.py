"""
StreamForge — Full Stack Orchestrator
──────────────────────────────────────
Starts all components in the correct order:
  1. HLS Origin Server       (aiohttp, port 8080)
  2. Ingest Server           (raw TCP, port 9935)
  3. Transcoder Pipeline     (FFmpeg workers)
  4. Broadcaster Client      (sends test stream)

Usage:
  python run_all.py                          # Full demo with test pattern
  python run_all.py --source video.mp4       # Use a real video file
  python run_all.py --no-broadcaster         # Server only (connect OBS manually)
  python run_all.py --stream-key sk-xxx      # Use specific stream key
"""

import argparse
import asyncio
import logging
import os
import signal
import subprocess
import sys
import time
import threading
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("orchestrator")

BASE_DIR      = Path(__file__).parent
HLS_OUTPUT    = BASE_DIR / "hls_output"
SERVER_DIR    = BASE_DIR / "server"
BROADCASTER_DIR = BASE_DIR / "broadcaster"

DEFAULT_STREAM_KEY = "sk-xK92mNpQvR4TgH7sLmWe"
DEFAULT_STREAM_ID  = "ls-001"


def print_banner():
    print("""
\033[92m
  ██████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗
 ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║
 ╚█████╗    ██║   ██████╔╝█████╗  ███████║██╔████╔██║
  ╚═══██╗   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║
 ██████╔╝   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║
 ╚═════╝    ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝
\033[96m              F O R G E  ·  M E D I A  ·  P L A T F O R M
\033[0m
""")


class ComponentProcess:
    def __init__(self, name: str, cmd: list, env=None):
        self.name = name
        self.cmd  = cmd
        self.env  = env or os.environ.copy()
        self.proc: subprocess.Popen = None
        self._log_thread = None

    def start(self):
        log.info(f"  Starting {self.name}...")
        self.proc = subprocess.Popen(
            self.cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
            text=True,
        )
        self._log_thread = threading.Thread(
            target=self._stream_logs, daemon=True
        )
        self._log_thread.start()
        log.info(f"  ✓ {self.name} PID={self.proc.pid}")
        return self

    def _stream_logs(self):
        prefix_colors = {
            "Origin Server":  "\033[94m",   # blue
            "Ingest Server":  "\033[93m",   # yellow
            "Broadcaster":    "\033[95m",   # magenta
            "Transcoder":     "\033[96m",   # cyan
        }
        reset = "\033[0m"
        color = prefix_colors.get(self.name, "")
        short = self.name[:12].ljust(12)

        for line in self.proc.stdout:
            line = line.rstrip()
            if line:
                print(f"{color}[{short}]{reset} {line}")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            log.info(f"  Stopping {self.name}...")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None


class StreamForgeOrchestrator:
    def __init__(self, args):
        self.args       = args
        self.components = []
        self._shutdown  = threading.Event()

    def setup_dirs(self):
        HLS_OUTPUT.mkdir(parents=True, exist_ok=True)
        log.info(f"  HLS output dir: {HLS_OUTPUT}")

    def check_ffmpeg(self) -> str:
        """Verify FFmpeg is available before starting any component."""
        import shutil
        # Check env override
        env_path = os.environ.get("FFMPEG_PATH")
        if env_path and Path(env_path).exists():
            log.info(f"  FFmpeg: {env_path}  (FFMPEG_PATH env)")
            return env_path
        # System PATH
        system = shutil.which("ffmpeg")
        if system:
            log.info(f"  FFmpeg: {system}")
            return system
        # imageio-ffmpeg bundled binary
        try:
            import imageio_ffmpeg
            bundled = imageio_ffmpeg.get_ffmpeg_exe()
            if bundled and Path(bundled).exists():
                log.info(f"  FFmpeg: {bundled}  (imageio-ffmpeg)")
                return bundled
        except ImportError:
            pass
        # Not found
        print("""
\033[91m═══ ERROR: FFmpeg not found ═══\033[0m

StreamForge requires FFmpeg for video transcoding.

\033[93mQuickest fix (no install required):\033[0m
  pip install imageio-ffmpeg

\033[93mFull install options:\033[0m
  Windows:  winget install ffmpeg
            choco install ffmpeg
            scoop install ffmpeg
            Download: https://ffmpeg.org/download.html
              → Extract zip, e.g. C:\\ffmpeg\\
              → Add C:\\ffmpeg\\bin to your PATH
              OR set env variable: set FFMPEG_PATH=C:\\ffmpeg\\bin\\ffmpeg.exe

  macOS:    brew install ffmpeg
  Linux:    sudo apt install ffmpeg

\033[93mVerify install:\033[0m
  ffmpeg -version
\033[91m═══════════════════════════════\033[0m
""")
        sys.exit(1)

    def start_origin_server(self):
        comp = ComponentProcess(
            "Origin Server",
            [sys.executable, str(SERVER_DIR / "origin_server.py")]
        )
        comp.start()
        self.components.append(comp)
        time.sleep(1.5)   # Give server time to bind port
        return comp

    def start_ingest_server(self):
        comp = ComponentProcess(
            "Ingest Server",
            [sys.executable, str(SERVER_DIR / "ingest_server.py")]
        )
        comp.start()
        self.components.append(comp)
        time.sleep(1.0)
        return comp

    def start_transcoder(self, stream_id: str, source: str):
        comp = ComponentProcess(
            "Transcoder",
            [
                sys.executable,
                str(SERVER_DIR / "transcoder.py"),
                stream_id,
                source,
            ]
        )
        comp.start()
        self.components.append(comp)
        return comp

    def start_broadcaster(self, stream_key: str, source: str):
        comp = ComponentProcess(
            "Broadcaster",
            [
                sys.executable,
                str(BROADCASTER_DIR / "broadcaster.py"),
                "--key",    stream_key,
                "--source", source,
            ]
        )
        comp.start()
        self.components.append(comp)
        return comp

    def stop_all(self):
        log.info("\n  Shutting down all components...")
        for comp in reversed(self.components):
            comp.stop()
        log.info("  All components stopped.")

    def run(self):
        print_banner()
        source = self.args.source

        print("\033[92m═══ StreamForge Starting ═══\033[0m")
        self.setup_dirs()
        self.check_ffmpeg()

        # 1. Origin server (HTTP for HLS delivery)
        self.start_origin_server()

        # 2. Ingest server (TCP for broadcaster)
        self.start_ingest_server()

        # 3. Transcoder (FFmpeg pipeline for the stream)
        self.start_transcoder(DEFAULT_STREAM_ID, source)
        time.sleep(2)

        # 4. Broadcaster (simulates OBS sending stream)
        if not self.args.no_broadcaster:
            self.start_broadcaster(
                self.args.stream_key or DEFAULT_STREAM_KEY,
                source
            )

        print(f"""
\033[92m═══ StreamForge RUNNING ═══\033[0m

  \033[96m🌐 Web Interface:\033[0m   http://localhost:8080
  \033[96m▶  Player:\033[0m          http://localhost:8080/player/{DEFAULT_STREAM_ID}
  \033[96m📡 HLS Master:\033[0m      http://localhost:8080/live/{DEFAULT_STREAM_ID}/master.m3u8
  \033[96m📊 Status API:\033[0m      http://localhost:8080/status
  \033[96m🔌 Ingest TCP:\033[0m      localhost:9935

  \033[93mTo connect OBS / Broadcaster manually:\033[0m
    RTMP URL:   tcp://localhost:9935 (custom protocol)
    Stream Key: {DEFAULT_STREAM_KEY}

  \033[90mPress Ctrl+C to stop all components\033[0m
""")

        # ── Signal handler ────────────────────────────────────────────────────
        def handle_signal(sig, frame):
            print("\n")
            self.stop_all()
            self._shutdown.set()

        signal.signal(signal.SIGINT,  handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        # ── Health monitor loop ───────────────────────────────────────────────
        try:
            while not self._shutdown.is_set():
                time.sleep(5)
                # Check if any component crashed
                for comp in self.components:
                    if not comp.is_running():
                        log.warning(f"  ⚠ {comp.name} stopped (exit code: {comp.proc.returncode})")
        except KeyboardInterrupt:
            self.stop_all()


def main():
    parser = argparse.ArgumentParser(
        description="StreamForge — Full Stack Live Streaming Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all.py                          Run with FFmpeg test pattern
  python run_all.py --source my_video.mp4   Run with a real video file
  python run_all.py --no-broadcaster        Server-only mode (connect OBS manually)
  python run_all.py --stream-key sk-xxx     Use a specific stream key
        """
    )
    parser.add_argument("--source",         default="testsrc",     help="Video source: file path or 'testsrc'")
    parser.add_argument("--stream-key",     default=None,          help="Stream key to use")
    parser.add_argument("--no-broadcaster", action="store_true",   help="Don't start broadcaster (server-only)")
    args = parser.parse_args()

    orch = StreamForgeOrchestrator(args)
    orch.run()


if __name__ == "__main__":
    main()