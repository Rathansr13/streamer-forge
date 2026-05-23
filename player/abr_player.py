"""
StreamForge ABR Player Simulator
──────────────────────────────────
Simulates what a real HLS player (HLS.js, Shaka) does:
  1. Fetch master manifest → parse variant streams
  2. Pick best quality based on simulated bandwidth
  3. Fetch variant manifest → get segment list
  4. Fetch segments in order (with timing)
  5. Adapt bitrate based on download speed (ABR)
  6. Print real-time playback stats

Run this to see ABR in action without a browser.
"""

import argparse
import asyncio
import json
import logging
import re
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PLAYER] %(message)s")
log = logging.getLogger("player")

BUFFER_TARGET   = 15.0    # seconds of buffer to maintain
BUFFER_MIN      = 3.0     # below this, downgrade quality
BUFFER_PANIC    = 1.0     # below this, panic downgrade
SEGMENT_DURATION = 2.0    # seconds per segment
POLL_INTERVAL    = 1.0    # manifest poll interval (seconds)


@dataclass
class VariantStream:
    bandwidth: int
    resolution: str
    name: str
    uri: str


@dataclass
class Segment:
    uri: str
    duration: float
    sequence: int


@dataclass
class PlayerState:
    level: int = -1           # -1 = auto ABR
    buffer_level: float = 0.0
    playhead: float = 0.0
    segments_loaded: int = 0
    bytes_downloaded: int = 0
    dropped_frames: int = 0
    current_bandwidth_bps: float = 0.0
    stall_count: int = 0
    quality_switches: int = 0


class ManifestParser:
    @staticmethod
    def parse_master(content: str, base_url: str) -> List[VariantStream]:
        streams = []
        lines = content.strip().split("\n")
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("#EXT-X-STREAM-INF"):
                bw_match  = re.search(r"BANDWIDTH=(\d+)", line)
                res_match = re.search(r"RESOLUTION=([\dx]+)", line)
                name_match = re.search(r'NAME="([^"]+)"', line)
                bw  = int(bw_match.group(1))  if bw_match  else 0
                res = res_match.group(1)        if res_match else "?"
                name = name_match.group(1)      if name_match else str(i)
                uri_line = lines[i+1].strip() if i+1 < len(lines) else ""
                if not uri_line.startswith("http"):
                    uri_line = base_url.rsplit("/", 1)[0] + "/" + uri_line
                streams.append(VariantStream(bandwidth=bw, resolution=res, name=name, uri=uri_line))
                i += 2
                continue
            i += 1
        return sorted(streams, key=lambda s: s.bandwidth, reverse=True)

    @staticmethod
    def parse_variant(content: str, base_url: str, media_sequence_start: int = 0) -> tuple:
        segments = []
        lines = content.strip().split("\n")
        media_seq = media_sequence_start
        seq_counter = 0

        for i, line in enumerate(lines):
            line = line.strip()
            if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                media_seq = int(line.split(":")[1])
                seq_counter = media_seq
            elif line.startswith("#EXTINF:"):
                dur = float(line.split(":")[1].rstrip(","))
                uri = lines[i+1].strip() if i+1 < len(lines) else ""
                if uri and not uri.startswith("#"):
                    if not uri.startswith("http"):
                        uri = base_url.rsplit("/", 1)[0] + "/" + uri
                    segments.append(Segment(uri=uri, duration=dur, sequence=seq_counter))
                    seq_counter += 1
        return segments, media_seq


class ABRController:
    """
    Simple bandwidth-based ABR algorithm.
    In production, BOLA or DASH-IF algorithms are used.
    """
    def __init__(self, variants: List[VariantStream]):
        self.variants = variants  # sorted high → low bandwidth

    def select_level(self, bw_bps: float, buffer_level: float, current_level: int) -> int:
        if buffer_level < BUFFER_PANIC:
            # Panic: jump to lowest quality
            return len(self.variants) - 1

        if buffer_level < BUFFER_MIN:
            # Buffer too low: step down one level
            return min(current_level + 1, len(self.variants) - 1)

        # Normal ABR: pick best quality that fits in bandwidth (with 80% safety margin)
        safe_bw = bw_bps * 0.80
        for i, v in enumerate(self.variants):
            if v.bandwidth <= safe_bw:
                # Don't upgrade too aggressively (hysteresis: only upgrade if buffer is healthy)
                if i < current_level and buffer_level < BUFFER_TARGET * 0.8:
                    return current_level  # hold current level
                return i

        return len(self.variants) - 1  # fallback to lowest


def fetch_url(url: str) -> tuple:
    """Fetch a URL and return (content_bytes, duration_ms, size_bytes)."""
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "StreamForge-Player/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = resp.read()
        elapsed_ms = (time.time() - t0) * 1000
        return data, elapsed_ms, len(data)
    except urllib.error.URLError as e:
        raise ConnectionError(f"Failed to fetch {url}: {e}")


class HLSPlayer:
    def __init__(self, master_url: str, simulated_bw_mbps: float = 5.0):
        self.master_url = master_url
        self.sim_bw_bps = simulated_bw_mbps * 1_000_000
        self.state = PlayerState()
        self.variants: List[VariantStream] = []
        self.abr: Optional[ABRController] = None
        self._running = False
        self._seen_segments = set()
        self._current_level = 0

    def _log_stats(self):
        level = self.variants[self._current_level] if self.variants else None
        quality = level.name if level else "?"
        resolution = level.resolution if level else "?"
        bw_mbps = self.state.current_bandwidth_bps / 1_000_000
        print(
            f"\r  ▶ {quality:>6} ({resolution:>9}) | "
            f"buffer={self.state.buffer_level:4.1f}s | "
            f"DL={bw_mbps:4.1f}Mbps | "
            f"segs={self.state.segments_loaded:4d} | "
            f"stalls={self.state.stall_count} | "
            f"switches={self.state.quality_switches}",
            end="", flush=True
        )

    async def run(self):
        log.info(f"Loading stream: {self.master_url}")

        # Step 1: Fetch master manifest
        try:
            data, ms, _ = fetch_url(self.master_url)
        except ConnectionError as e:
            log.error(f"Cannot reach origin server: {e}")
            log.error("Make sure the origin server is running: python server/origin_server.py")
            return

        master_content = data.decode("utf-8")
        self.variants = ManifestParser.parse_master(master_content, self.master_url)

        if not self.variants:
            log.error("No variant streams found in master manifest")
            return

        log.info(f"Found {len(self.variants)} quality levels:")
        for i, v in enumerate(self.variants):
            log.info(f"  [{i}] {v.name:6} {v.bandwidth//1000:5} kbps  {v.resolution}")

        self.abr = ABRController(self.variants)
        self._current_level = len(self.variants) - 1  # start at lowest quality
        self._running = True

        print("\n  Starting playback simulation...\n")
        print("  Quality | Resolution | Buffer | DL Speed | Segments | Stalls | Switches")
        print("  " + "─" * 70)

        last_media_seq = 0

        while self._running:
            # Step 2: ABR decision
            new_level = self.abr.select_level(
                self.sim_bw_bps,
                self.state.buffer_level,
                self._current_level
            )
            if new_level != self._current_level:
                direction = "↑ UP" if new_level < self._current_level else "↓ DOWN"
                old_q = self.variants[self._current_level].name
                new_q = self.variants[new_level].name
                print(f"\n  🔀 Quality switch {direction}: {old_q} → {new_q}")
                self._current_level = new_level
                self.state.quality_switches += 1

            variant = self.variants[self._current_level]

            # Step 3: Fetch variant manifest
            try:
                manifest_data, _, _ = fetch_url(variant.uri)
                segments, media_seq = ManifestParser.parse_variant(
                    manifest_data.decode("utf-8"), variant.uri, last_media_seq
                )
            except ConnectionError as e:
                log.warning(f"Manifest fetch failed: {e}")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Step 4: Fetch new segments
            new_segments = [s for s in segments if s.sequence not in self._seen_segments]

            for seg in new_segments:
                if not self._running:
                    break

                # Simulate download with bandwidth tracking
                t0 = time.time()
                try:
                    seg_data, dl_ms, seg_bytes = fetch_url(seg.uri)
                except ConnectionError:
                    self.state.stall_count += 1
                    self.state.buffer_level = max(0, self.state.buffer_level - seg.duration)
                    continue

                elapsed = time.time() - t0
                self.state.current_bandwidth_bps = (seg_bytes * 8) / max(elapsed, 0.001)

                # Simulate: if download took longer than segment duration → stall
                if elapsed > seg.duration:
                    stall_time = elapsed - seg.duration
                    self.state.stall_count += 1
                    self.state.buffer_level = max(0, self.state.buffer_level - stall_time)
                    print(f"\n  ⚠ STALL detected! {stall_time:.1f}s rebuffering...")
                else:
                    self.state.buffer_level += seg.duration

                # Buffer drain (playback consumption)
                self.state.buffer_level -= seg.duration * 0.1   # slight drain per segment
                self.state.buffer_level = max(0, min(self.state.buffer_level, 60.0))

                self.state.segments_loaded += 1
                self.state.bytes_downloaded += seg_bytes
                self.state.playhead += seg.duration
                self._seen_segments.add(seg.sequence)
                last_media_seq = seg.sequence

                self._log_stats()

                # Simulate real-time playback speed
                await asyncio.sleep(seg.duration * 0.1)

            # Poll for new segments
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self):
        self._running = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StreamForge ABR Player Simulator")
    parser.add_argument("--url",       default="http://localhost:8080/live/ls-001/master.m3u8")
    parser.add_argument("--bandwidth", type=float, default=5.0, help="Simulated bandwidth in Mbps")
    args = parser.parse_args()

    player = HLSPlayer(args.url, simulated_bw_mbps=args.bandwidth)
    try:
        asyncio.run(player.run())
    except KeyboardInterrupt:
        print("\n\nPlayer stopped")