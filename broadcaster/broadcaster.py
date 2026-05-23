"""
StreamForge Broadcaster Client
───────────────────────────────
Simulates an OBS/encoder client:
  1. Opens a video source (file or test pattern)
  2. Reads it frame-by-frame via FFmpeg
  3. Wraps frames in StreamForge wire protocol
  4. Sends to the Ingest Server over TCP

Wire protocol frame format:
  [4 bytes: frame_len BE uint32]
  [1 byte:  frame_type  0x01=video 0x02=audio 0xFF=heartbeat]
  [N bytes: payload]

Handshake:
  → SFGE magic (4 bytes)
  → key_len  (2 bytes BE)
  → key      (key_len bytes)
  ← status   (1 byte: 0=OK)
  ← token    (16 bytes session token)
"""

import asyncio
import logging
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BROADCASTER] %(message)s")
log = logging.getLogger("broadcaster")

INGEST_HOST = "127.0.0.1"


def find_ffmpeg() -> str:
    """Find FFmpeg binary — checks env, PATH, imageio-ffmpeg, common Windows paths."""
    import shutil
    env_path = os.environ.get("FFMPEG_PATH")
    if env_path and Path(env_path).exists():
        return env_path
    system = shutil.which("ffmpeg")
    if system:
        return system
    try:
        import imageio_ffmpeg
        bundled = imageio_ffmpeg.get_ffmpeg_exe()
        if bundled and Path(bundled).exists():
            log.info(f"Using bundled FFmpeg: {bundled}")
            return bundled
    except ImportError:
        pass
    for p in [
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
        str(Path.home() / "scoop" / "apps" / "ffmpeg" / "current" / "bin" / "ffmpeg.exe"),
    ]:
        if Path(p).exists():
            return p
    raise FileNotFoundError(
        "FFmpeg not found. Run: pip install imageio-ffmpeg  OR  winget install ffmpeg"
    )
INGEST_PORT = 9935
HEARTBEAT_INTERVAL = 5.0      # seconds between heartbeats
CHUNK_SIZE = 8192             # bytes per read chunk from FFmpeg
RECONNECT_DELAY = 3.0         # seconds before reconnect on drop


class Broadcaster:
    def __init__(self, stream_key: str, source: str, host=INGEST_HOST, port=INGEST_PORT):
        self.stream_key = stream_key
        self.source = source
        self.host = host
        self.port = port
        self._running = False
        self._bytes_sent = 0
        self._frames_sent = 0
        self._connected_at: float = 0
        self._session_token: bytes = b""

    # ── Connect + handshake ───────────────────────────────────────────────────
    async def connect(self) -> tuple:
        log.info(f"Connecting to ingest server {self.host}:{self.port}...")
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=10.0
        )

        # Send handshake
        key_bytes = self.stream_key.encode("utf-8")
        handshake = (
            b"SFGE"
            + struct.pack(">H", len(key_bytes))
            + key_bytes
        )
        writer.write(handshake)
        await writer.drain()

        # Read response
        response = await asyncio.wait_for(reader.readexactly(17), timeout=5.0)
        status = response[0]
        token  = response[1:]

        if status != 0:
            raise ConnectionRefusedError(f"Ingest server rejected stream key (status={status})")

        self._session_token = token
        self._connected_at = time.time()
        log.info(f"✓ Authenticated! Session token: {token.hex()[:8]}...")
        return reader, writer

    # ── Send a single framed chunk ────────────────────────────────────────────
    async def send_frame(self, writer: asyncio.StreamWriter, frame_type: int, data: bytes):
        frame = struct.pack(">IB", len(data), frame_type) + data
        writer.write(frame)
        await writer.drain()
        self._bytes_sent += len(data)
        self._frames_sent += 1

    # ── Heartbeat sender ──────────────────────────────────────────────────────
    async def _heartbeat_loop(self, writer: asyncio.StreamWriter):
        while self._running:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if self._running:
                await self.send_frame(writer, 0xFF, b"HB")
                uptime = int(time.time() - self._connected_at)
                mbps = (self._bytes_sent * 8) / (max(uptime, 1) * 1_000_000)
                log.info(
                    f"♥ Heartbeat | uptime={uptime}s | frames={self._frames_sent} "
                    f"| bitrate={mbps:.2f} Mbps"
                )

    # ── Read video source via FFmpeg and send ─────────────────────────────────
    async def stream_source(self, writer: asyncio.StreamWriter):
        """
        Uses FFmpeg to read the source video and output raw H.264 NAL units.
        In a real broadcaster, this is what OBS does internally.
        """
        source_path = self.source

        # Check if source is a test pattern or a file
        ffmpeg_bin = find_ffmpeg()
        if source_path == "testsrc":
            # Generate a synthetic test pattern (no input file needed)
            ffmpeg_input = [
                ffmpeg_bin,
                "-f", "lavfi",
                "-i", "testsrc=size=1280x720:rate=30",
                "-f", "lavfi",
                "-i", "sine=frequency=440:sample_rate=44100",
                "-t", "3600",               # 1 hour of test pattern
            ]
        else:
            ffmpeg_input = [
                ffmpeg_bin,
                "-re",                      # read at native frame rate
                "-stream_loop", "-1",       # loop the file
                "-i", source_path,
            ]

        ffmpeg_cmd = ffmpeg_input + [
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-tune", "zerolatency",
            "-b:v", "4000k",
            "-g", "60",
            "-sc_threshold", "0",
            "-c:a", "aac",
            "-b:a", "128k",
            "-f", "mpegts",               # output as MPEG-TS stream
            "pipe:1",                     # write to stdout
        ]

        log.info(f"Starting FFmpeg encoder for source: {source_path}")
        log.info(f"FFmpeg command: {' '.join(ffmpeg_cmd[:8])}...")

        process = subprocess.Popen(
            ffmpeg_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Read stderr in background for logging
        async def log_ffmpeg():
            loop = asyncio.get_event_loop()
            while True:
                line = await loop.run_in_executor(None, process.stderr.readline)
                if not line:
                    break
                decoded = line.decode("utf-8", errors="replace").strip()
                if decoded and ("fps=" in decoded or "Error" in decoded.lower()):
                    log.debug(f"FFmpeg: {decoded}")

        asyncio.create_task(log_ffmpeg())

        try:
            loop = asyncio.get_event_loop()
            frame_seq = 0

            while self._running:
                # Read a chunk from FFmpeg stdout (simulates encoded video data)
                chunk = await loop.run_in_executor(None, process.stdout.read, CHUNK_SIZE)
                if not chunk:
                    log.info("FFmpeg stream ended")
                    break

                # Alternate video/audio type for demo (real impl would parse TS headers)
                frame_type = 0x01 if frame_seq % 10 != 0 else 0x02  # 90% video, 10% audio
                await self.send_frame(writer, frame_type, chunk)
                frame_seq += 1

                # Small yield to prevent blocking event loop
                if frame_seq % 50 == 0:
                    await asyncio.sleep(0)

        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()

    # ── Main broadcast loop with reconnect ────────────────────────────────────
    async def run(self):
        self._running = True
        log.info(f"StreamForge Broadcaster starting")
        log.info(f"  Stream Key: {self.stream_key[:8]}...")
        log.info(f"  Source:     {self.source}")
        log.info(f"  Target:     {self.host}:{self.port}")

        while self._running:
            try:
                reader, writer = await self.connect()

                # Start heartbeat task
                hb_task = asyncio.create_task(self._heartbeat_loop(writer))

                try:
                    await self.stream_source(writer)
                finally:
                    hb_task.cancel()
                    try:
                        await hb_task
                    except asyncio.CancelledError:
                        pass
                    writer.close()

            except ConnectionRefusedError as e:
                log.error(f"Rejected: {e}")
                self._running = False
                break
            except (ConnectionResetError, asyncio.IncompleteReadError, OSError) as e:
                log.warning(f"Connection lost: {e}. Reconnecting in {RECONNECT_DELAY}s...")
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception as e:
                log.error(f"Unexpected error: {e}")
                await asyncio.sleep(RECONNECT_DELAY)

    def stop(self):
        self._running = False
        log.info("Broadcaster stopped")


# ── CLI entrypoint ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="StreamForge Broadcaster Client")
    parser.add_argument("--key",    required=True, help="Stream key (from StreamForge dashboard)")
    parser.add_argument("--source", default="testsrc", help="Video source file or 'testsrc' for test pattern")
    parser.add_argument("--host",   default=INGEST_HOST)
    parser.add_argument("--port",   type=int, default=INGEST_PORT)
    args = parser.parse_args()

    broadcaster = Broadcaster(
        stream_key=args.key,
        source=args.source,
        host=args.host,
        port=args.port,
    )

    try:
        asyncio.run(broadcaster.run())
    except KeyboardInterrupt:
        log.info("Broadcaster stopped by user")