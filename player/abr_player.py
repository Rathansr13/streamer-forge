"""
StreamForge ABR Player Simulator (CLI)
───────────────────────────────────────
Simulates what HLS.js / Shaka Player does when playing a VOD asset:

  1. Fetch master.m3u8 → parse all variant streams
  2. Fetch variant manifest → get full segment list (VOD = complete list)
  3. Pick starting quality based on simulated bandwidth
  4. Download each segment, measure real download speed
  5. ABR algorithm decides to upgrade / hold / downgrade quality
  6. Print live stats table to terminal

Usage:
  python abr_player.py --asset vod-ABC123DEF456
  python abr_player.py --url http://localhost:8080/vod/vod-ABC123/master.m3u8
  python abr_player.py --asset vod-ABC123 --bandwidth 2.5
  python abr_player.py --list          (list available assets then pick one)
"""

import argparse
import asyncio
import json
import logging
import re
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import List, Optional

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [PLAYER] %(message)s")
log = logging.getLogger("player")

API_BASE         = "http://localhost:8080"
BUFFER_TARGET    = 20.0   # seconds — VOD player keeps more buffer
BUFFER_MIN       = 4.0    # below this → downgrade
BUFFER_PANIC     = 1.5    # below this → panic to lowest
POLL_INTERVAL    = 0.5    # VOD: segments are all available, poll faster
BW_SAFETY        = 0.80   # only use 80% of measured bandwidth for ABR


@dataclass
class Variant:
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
class PlaybackStats:
    level: int = -1
    buffer: float = 0.0
    playhead: float = 0.0
    segments: int = 0
    bytes_dl: int = 0
    bandwidth_bps: float = 0.0
    stalls: int = 0
    switches: int = 0
    dropped: int = 0


# ── Manifest parsing ───────────────────────────────────────────────────────
def parse_master(text: str, base_url: str) -> List[Variant]:
    variants = []
    lines = text.strip().splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF"):
            bw   = int(re.search(r"BANDWIDTH=(\d+)", line).group(1)) if re.search(r"BANDWIDTH=(\d+)", line) else 0
            res  = re.search(r"RESOLUTION=([\dx]+)", line)
            name = re.search(r'NAME="([^"]+)"', line)
            uri  = lines[i + 1].strip() if i + 1 < len(lines) else ""
            if not uri.startswith("http"):
                uri = base_url.rsplit("/", 1)[0] + "/" + uri
            variants.append(Variant(
                bandwidth  = bw,
                resolution = res.group(1)  if res  else "?",
                name       = name.group(1) if name else str(len(variants)),
                uri        = uri,
            ))
            i += 2
            continue
        i += 1
    return sorted(variants, key=lambda v: v.bandwidth, reverse=True)


def parse_variant(text: str, base_url: str) -> List[Segment]:
    """Parse a variant .m3u8 — for VOD all segments are listed."""
    segments = []
    lines = text.strip().splitlines()
    seq = 0
    for line in lines:
        line = line.strip()
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            seq = int(line.split(":")[1])
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF:"):
            try:
                dur = float(line.split(":")[1].rstrip(","))
            except Exception:
                dur = 4.0
            uri = lines[i + 1].strip() if i + 1 < len(lines) else ""
            if uri and not uri.startswith("#"):
                if not uri.startswith("http"):
                    uri = base_url.rsplit("/", 1)[0] + "/" + uri
                segments.append(Segment(uri=uri, duration=dur, sequence=seq))
                seq += 1
            i += 2
            continue
        i += 1
    return segments


# ── ABR algorithm ──────────────────────────────────────────────────────────
class ABR:
    """
    Simple throughput-based ABR with hysteresis and buffer safety.
    """
    def __init__(self, variants: List[Variant]):
        self.variants = variants   # sorted high → low bandwidth

    def select(self, bw_bps: float, buffer: float, current: int) -> int:
        # Panic: buffer critically low
        if buffer < BUFFER_PANIC:
            return len(self.variants) - 1

        # Buffer low: step down one level
        if buffer < BUFFER_MIN:
            return min(current + 1, len(self.variants) - 1)

        # Normal ABR: pick best quality that fits within safe bandwidth
        safe = bw_bps * BW_SAFETY
        for i, v in enumerate(self.variants):
            if v.bandwidth <= safe:
                # Hysteresis: don't upgrade unless buffer is healthy
                if i < current and buffer < BUFFER_TARGET * 0.7:
                    return current
                return i

        # Bandwidth too low even for lowest quality — stay at lowest
        return len(self.variants) - 1


# ── HTTP fetch helper ──────────────────────────────────────────────────────
def fetch(url: str, timeout: int = 15) -> tuple:
    """Returns (bytes, elapsed_ms, size_bytes). Raises on error."""
    t0 = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "StreamForge-Player/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
    elapsed_ms = (time.time() - t0) * 1000
    return data, elapsed_ms, len(data)


# ── Player ─────────────────────────────────────────────────────────────────
class HLSPlayer:
    def __init__(self, master_url: str, sim_bw_mbps: float = 10.0):
        self.master_url  = master_url
        self.sim_bw_bps  = sim_bw_mbps * 1_000_000
        self.stats       = PlaybackStats()
        self.variants: List[Variant] = []
        self.abr: Optional[ABR]      = None
        self._level      = -1        # current quality index
        self._running    = False

    def _print_header(self):
        print(f"\n  \033[96mStreamForge ABR Player\033[0m")
        print(f"  URL: {self.master_url}")
        print(f"  Simulated bandwidth: {self.sim_bw_bps/1_000_000:.1f} Mbps\n")
        print(f"  {'Quality':>8}  {'Res':>10}  {'Buffer':>7}  {'DL Speed':>10}  {'Segs':>5}  {'Stalls':>6}  {'Switches':>8}")
        print("  " + "─" * 70)

    def _print_stats(self):
        v   = self.variants[self._level] if self._level >= 0 and self._level < len(self.variants) else None
        q   = v.name       if v else "?"
        res = v.resolution if v else "?"
        bw  = self.stats.bandwidth_bps / 1_000_000
        print(
            f"\r  {q:>8}  {res:>10}  {self.stats.buffer:>6.1f}s  "
            f"{bw:>8.1f}M  {self.stats.segments:>5}  "
            f"{self.stats.stalls:>6}  {self.stats.switches:>8}",
            end="", flush=True
        )

    async def run(self):
        # ── 1. Load master manifest ─────────────────────────────────────────
        print("  Loading master manifest...")
        try:
            data, _, _ = fetch(self.master_url)
        except Exception as e:
            print(f"\033[91m✗ Cannot reach {self.master_url}\n  {e}\033[0m")
            print("  Is the server running?  python run_all.py")
            return

        self.variants = parse_master(data.decode("utf-8"), self.master_url)
        if not self.variants:
            print("\033[91m✗ No variant streams in master manifest\033[0m")
            return

        self.abr    = ABR(self.variants)
        self._level = len(self.variants) - 1   # start lowest
        self._running = True

        print(f"\n  Found {len(self.variants)} quality levels:")
        for i, v in enumerate(self.variants):
            print(f"    [{i}] {v.name:6}  {v.resolution:10}  {v.bandwidth//1000:5} kbps")

        self._print_header()

        # ── 2. Main playback loop ───────────────────────────────────────────
        all_seen: set = set()

        while self._running:
            # ABR decision
            new_level = self.abr.select(self.sim_bw_bps, self.stats.buffer, self._level)
            if new_level != self._level:
                old_q = self.variants[self._level].name
                new_q = self.variants[new_level].name
                direction = "↑" if new_level < self._level else "↓"
                print(f"\n  \033[93m{direction} Quality switch: {old_q} → {new_q}\033[0m")
                self._level = new_level
                self.stats.switches += 1

            variant = self.variants[self._level]

            # Fetch variant manifest (VOD: full list always available)
            try:
                mdata, _, _ = fetch(variant.uri)
                segments     = parse_variant(mdata.decode("utf-8"), variant.uri)
            except Exception as e:
                log.warning(f"Manifest fetch failed: {e}")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Process new segments only
            new_segs = [s for s in segments if s.sequence not in all_seen]

            if not new_segs:
                # VOD complete
                if any("#EXT-X-ENDLIST" in mdata.decode("utf-8", errors="replace") for _ in [1]):
                    print(f"\n\n  \033[92m✓ Playback complete\033[0m")
                    self._print_final()
                    break
                await asyncio.sleep(POLL_INTERVAL)
                continue

            for seg in new_segs:
                if not self._running:
                    break

                t0 = time.time()
                try:
                    seg_data, dl_ms, seg_bytes = fetch(seg.uri, timeout=20)
                except Exception as e:
                    log.warning(f"Segment fetch failed: {e}")
                    self.stats.stalls += 1
                    self.stats.buffer = max(0, self.stats.buffer - seg.duration)
                    continue

                elapsed = time.time() - t0
                self.sim_bw_bps = (seg_bytes * 8) / max(elapsed, 0.001)
                self.stats.bandwidth_bps = self.sim_bw_bps

                # Stall detection
                if elapsed > seg.duration:
                    stall = elapsed - seg.duration
                    self.stats.stalls += 1
                    self.stats.buffer  = max(0, self.stats.buffer - stall)
                    print(f"\n  \033[91m⚠ Stall: {stall:.1f}s rebuffering\033[0m")
                else:
                    self.stats.buffer += seg.duration

                # Simulate playback drain
                self.stats.buffer   = max(0, min(self.stats.buffer - seg.duration * 0.15, 120.0))
                self.stats.segments += 1
                self.stats.bytes_dl += seg_bytes
                self.stats.playhead += seg.duration
                all_seen.add(seg.sequence)

                self._print_stats()
                await asyncio.sleep(seg.duration * 0.08)   # simulate real-time

            await asyncio.sleep(POLL_INTERVAL)

    def _print_final(self):
        total_mb = self.stats.bytes_dl / 1_000_000
        print(f"""
  ─────────────────────────────────────
  Playback Summary
  ─────────────────────────────────────
  Segments downloaded : {self.stats.segments}
  Total data          : {total_mb:.1f} MB
  Quality switches    : {self.stats.switches}
  Stall events        : {self.stats.stalls}
  Total playhead      : {self.stats.playhead:.0f}s
  ─────────────────────────────────────
""")

    def stop(self):
        self._running = False


# ── List assets helper ─────────────────────────────────────────────────────
def pick_asset(server: str) -> str:
    """Fetch asset list and let user pick one interactively."""
    try:
        with urllib.request.urlopen(f"{server}/api/assets", timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        print(f"✗ Cannot reach {server}: {e}")
        sys.exit(1)

    assets = [a for a in data.get("assets", []) if a.get("status") == "ready"]
    if not assets:
        print("No ready assets found. Upload one first:\n  python run_all.py")
        sys.exit(0)

    print(f"\n\033[96mAvailable assets ({len(assets)} ready):\033[0m\n")
    for i, a in enumerate(assets):
        print(f"  [{i+1}] {a['asset_id']:24}  {a.get('name','—')[:30]:30}  "
              f"{a.get('width','?')}×{a.get('height','?')}  "
              f"{a.get('duration') and str(int(a['duration']))+'s' or '—'}")

    print()
    while True:
        try:
            choice = input("  Pick an asset [1]: ").strip() or "1"
            idx = int(choice) - 1
            if 0 <= idx < len(assets):
                return assets[idx]["asset_id"]
        except (ValueError, KeyboardInterrupt):
            sys.exit(0)
        print(f"  Please enter a number between 1 and {len(assets)}")


# ── Entry point ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="StreamForge ABR Player Simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python abr_player.py --asset vod-ABC123DEF456
  python abr_player.py --url http://localhost:8080/vod/vod-ABC/master.m3u8
  python abr_player.py --asset vod-ABC123 --bandwidth 2.0
  python abr_player.py --list
        """
    )
    parser.add_argument("--asset",     help="Asset ID (e.g. vod-ABC123DEF456)")
    parser.add_argument("--url",       help="Direct HLS master URL")
    parser.add_argument("--server",    default=API_BASE)
    parser.add_argument("--bandwidth", type=float, default=10.0, help="Simulated DL bandwidth in Mbps")
    parser.add_argument("--list",      action="store_true", help="List ready assets and pick one")
    args = parser.parse_args()

    if args.list or (not args.asset and not args.url):
        asset_id = pick_asset(args.server)
        master_url = f"{args.server}/vod/{asset_id}/master.m3u8"
    elif args.asset:
        master_url = f"{args.server}/vod/{args.asset}/master.m3u8"
    else:
        master_url = args.url

    player = HLSPlayer(master_url, sim_bw_mbps=args.bandwidth)
    try:
        asyncio.run(player.run())
    except KeyboardInterrupt:
        player.stop()
        print("\n\n  Player stopped.")


if __name__ == "__main__":
    main()