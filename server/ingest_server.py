"""
StreamForge Ingest Server
─────────────────────────
Receives raw H.264/AAC data over TCP (RTMP-style),
validates stream keys, writes to a ring buffer, and
fans chunks out to the transcoder pipeline.
"""

import asyncio
import hashlib
import json
import logging
import os
import struct
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [INGEST] %(message)s")
log = logging.getLogger("ingest")

INGEST_HOST = "0.0.0.0"
INGEST_PORT = 9935           # Custom TCP ingest port (RTMP uses 1935)
RING_BUFFER_SECONDS = 10
CHUNK_SIZE = 4096
HEARTBEAT_TIMEOUT = 15       # seconds before stream is marked IDLE

# Queue: transcoder reads from file directly; this queue exists for future
# real-pipe integration. Size is large; overflow is silently dropped (not spammed).
TRANSCODER_QUEUE_SIZE = 5000
_queue_drop_logged: Dict[str, float] = {}   # rate-limit the "queue full" warning

# ── Stream registry (in-memory; use Redis in prod) ──────────────────────────
VALID_STREAM_KEYS = {
    "sk-xK92mNpQvR4TgH7sLmWe": {"stream_id": "ls-001", "name": "Gaming Tournament — Finals",   "region": "US-EAST"},
    "sk-AbCdEfGhIjKlMnOpQrSt": {"stream_id": "ls-002", "name": "Product Launch Event",          "region": "EU-WEST"},
    "sk-DevConfStream2024Key0": {"stream_id": "ls-003", "name": "Dev Conference Stream",          "region": "AP-SOUTH"},
}


@dataclass
class LiveStream:
    stream_id: str
    stream_key: str
    name: str
    region: str
    started_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    bytes_received: int = 0
    chunks_received: int = 0
    # Ring buffer: deque of (timestamp, raw_bytes) tuples
    ring_buffer: deque = field(default_factory=lambda: deque(maxlen=500))
    status: str = "live"
    writer: Optional[asyncio.StreamWriter] = None


class IngestServer:
    def __init__(self):
        self.active_streams: Dict[str, LiveStream] = {}
        self.transcoder_queue: asyncio.Queue = asyncio.Queue(maxsize=TRANSCODER_QUEUE_SIZE)
        self._stats_task = None
        self._queue_drop_count: Dict[str, int] = {}
        self._queue_drop_logged: Dict[str, float] = {}

    # ── Connection handler ────────────────────────────────────────────────────
    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        log.info(f"New connection from {peer}")

        try:
            stream = await self._handshake(reader, writer)
            if stream is None:
                log.warning(f"Handshake failed from {peer}")
                writer.close()
                return

            log.info(f"✓ Stream LIVE: [{stream.stream_id}] '{stream.name}' from {peer}")
            self.active_streams[stream.stream_id] = stream

            await self._receive_stream(reader, stream)

        except (asyncio.IncompleteReadError, ConnectionResetError):
            log.info(f"Connection closed by {peer}")
        except Exception as e:
            log.error(f"Connection error from {peer}: {e}")
        finally:
            # Mark stream idle
            sid = None
            for s in self.active_streams.values():
                if s.writer is writer:
                    sid = s.stream_id
                    break
            if sid and sid in self.active_streams:
                self.active_streams[sid].status = "idle"
                log.info(f"Stream {sid} marked IDLE (disconnected)")
            writer.close()

    # ── Handshake: read stream key, validate, send ACK ────────────────────────
    async def _handshake(self, reader, writer) -> Optional[LiveStream]:
        """
        Simple binary handshake protocol:
          Client → Server:  [4 bytes magic][2 bytes key_len][key_bytes]
          Server → Client:  [1 byte status: 0=OK, 1=REJECT][16 bytes stream_token]
        """
        MAGIC = b"SFGE"   # StreamForge Ingest

        try:
            header = await asyncio.wait_for(reader.readexactly(4), timeout=5.0)
        except asyncio.TimeoutError:
            return None

        if header != MAGIC:
            writer.write(b"\x01" + b"\x00" * 16)
            await writer.drain()
            return None

        key_len_bytes = await asyncio.wait_for(reader.readexactly(2), timeout=5.0)
        key_len = struct.unpack(">H", key_len_bytes)[0]

        if key_len > 256:
            writer.write(b"\x01" + b"\x00" * 16)
            await writer.drain()
            return None

        key_bytes = await asyncio.wait_for(reader.readexactly(key_len), timeout=5.0)
        stream_key = key_bytes.decode("utf-8", errors="replace")

        if stream_key not in VALID_STREAM_KEYS:
            log.warning(f"Rejected invalid stream key: {stream_key[:8]}...")
            writer.write(b"\x01" + b"\x00" * 16)
            await writer.drain()
            return None

        meta = VALID_STREAM_KEYS[stream_key]
        token = hashlib.md5(f"{stream_key}{time.time()}".encode()).digest()

        # ACK: OK + 16-byte session token
        writer.write(b"\x00" + token)
        await writer.drain()

        stream = LiveStream(
            stream_id=meta["stream_id"],
            stream_key=stream_key,
            name=meta["name"],
            region=meta["region"],
            writer=writer,
        )
        return stream

    # ── Main receive loop ─────────────────────────────────────────────────────
    async def _receive_stream(self, reader: asyncio.StreamReader, stream: LiveStream):
        """
        Reads framed chunks from broadcaster:
          Frame format: [4 bytes frame_len][1 byte frame_type][payload]
          frame_type: 0x01=video, 0x02=audio, 0x03=metadata, 0xFF=heartbeat
        """
        FRAME_HEADER_SIZE = 5

        while True:
            header = await asyncio.wait_for(
                reader.readexactly(FRAME_HEADER_SIZE),
                timeout=HEARTBEAT_TIMEOUT
            )

            frame_len = struct.unpack(">I", header[:4])[0]
            frame_type = header[4]

            if frame_len > 1_000_000:   # 1 MB max frame guard
                log.error(f"[{stream.stream_id}] Oversized frame {frame_len}B, dropping")
                break

            payload = await reader.readexactly(frame_len)
            stream.last_heartbeat = time.time()

            if frame_type == 0xFF:  # heartbeat
                continue

            # Update stats
            stream.bytes_received += frame_len
            stream.chunks_received += 1
            stream.status = "live"

            # Store in ring buffer (timestamp, type, data)
            chunk = (time.time(), frame_type, payload)
            stream.ring_buffer.append(chunk)

            # Push to transcoder queue — rate-limit the "queue full" warning
            try:
                self.transcoder_queue.put_nowait({
                    "stream_id": stream.stream_id,
                    "stream_key": stream.stream_key,
                    "timestamp": time.time(),
                    "frame_type": frame_type,
                    "data": payload,
                    "seq": stream.chunks_received,
                })
                # Reset drop counter on success
                self._queue_drop_count[stream.stream_id] = 0
            except asyncio.QueueFull:
                sid = stream.stream_id
                self._queue_drop_count[sid] = self._queue_drop_count.get(sid, 0) + 1
                now = time.time()
                last = self._queue_drop_logged.get(sid, 0)
                # Only log once every 10 seconds to avoid spam
                if now - last > 10:
                    log.debug(
                        f"[{sid}] Transcoder queue full "
                        f"(dropped {self._queue_drop_count[sid]} frames — "
                        f"transcoder reads from file directly, this is normal)"
                    )
                    self._queue_drop_logged[sid] = now

            if stream.chunks_received % 100 == 0:
                uptime = int(time.time() - stream.started_at)
                mbps = (stream.bytes_received * 8) / (uptime * 1_000_000) if uptime > 0 else 0
                log.info(f"[{stream.stream_id}] chunks={stream.chunks_received} "
                         f"uptime={uptime}s bitrate={mbps:.2f}Mbps")

    # ── Stats broadcaster (publishes to state file) ───────────────────────────
    async def _stats_loop(self):
        while True:
            await asyncio.sleep(2)
            state = {}
            for sid, s in self.active_streams.items():
                uptime = int(time.time() - s.started_at)
                mbps = (s.bytes_received * 8) / (max(uptime, 1) * 1_000_000)
                state[sid] = {
                    "stream_id": sid,
                    "name": s.name,
                    "region": s.region,
                    "status": s.status,
                    "uptime_seconds": uptime,
                    "uptime_fmt": f"{uptime//3600:02d}:{(uptime%3600)//60:02d}:{uptime%60:02d}",
                    "bitrate_mbps": round(mbps, 2),
                    "chunks": s.chunks_received,
                    "bytes": s.bytes_received,
                    "last_heartbeat": s.last_heartbeat,
                }
            state_path = os.path.join(os.path.dirname(__file__), "../hls_output/stream_state.json")
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            with open(state_path, "w") as f:
                json.dump(state, f, indent=2)

    # ── Start server ──────────────────────────────────────────────────────────
    async def start(self):
        server = await asyncio.start_server(
            self.handle_connection, INGEST_HOST, INGEST_PORT
        )
        self._stats_task = asyncio.create_task(self._stats_loop())

        log.info(f"StreamForge Ingest Server listening on {INGEST_HOST}:{INGEST_PORT}")
        log.info(f"Registered stream keys: {len(VALID_STREAM_KEYS)}")

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    server = IngestServer()
    asyncio.run(server.start())