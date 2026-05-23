"""
StreamForge Transcoder Pipeline
────────────────────────────────
Reads raw video chunks from the ingest queue,
assembles them into a temp buffer, drives FFmpeg
for multi-bitrate transcoding, and writes HLS
segments + manifests to the output directory.

ABR Ladder produced:
  1080p  H.264  4500 kbps
   720p  H.264  2500 kbps
   480p  H.264  1200 kbps
   360p  H.264   600 kbps
  audio  AAC     128 kbps

Fixes applied:
  - Removed delete_segments flag (keeps all .ts files on disk)
  - Increased hls_list_size to 30 (60s sliding window)
  - Removed -re flag so FFmpeg loops fast enough to stay ahead
  - Segment watcher uses file mtime to detect truly NEW segments
  - Queue is drained silently (transcoder drives from file directly)
  - FFmpeg logs always shown so errors surface immediately
"""

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSCODE] %(message)s")
log = logging.getLogger("transcoder")

HLS_OUTPUT_DIR = Path(__file__).parent.parent / "hls_output"
SEGMENT_DURATION = 2       # seconds per HLS segment
SEGMENT_LIST_SIZE = 30     # sliding window — 30 × 2s = 60s of buffer available
                           # large enough for any short looping video


def find_ffmpeg() -> str:
    """
    Locate FFmpeg binary with fallback chain:
      1. FFMPEG_PATH environment variable
      2. System PATH (works if FFmpeg is installed and on PATH)
      3. imageio-ffmpeg bundled binary (always available via pip)
      4. Common Windows install locations
    """
    import shutil

    env_path = os.environ.get("FFMPEG_PATH")
    if env_path and Path(env_path).exists():
        log.info(f"FFmpeg found via FFMPEG_PATH env: {env_path}")
        return env_path

    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        log.info(f"FFmpeg found on PATH: {system_ffmpeg}")
        return system_ffmpeg

    try:
        import imageio_ffmpeg
        bundled = imageio_ffmpeg.get_ffmpeg_exe()
        if bundled and Path(bundled).exists():
            log.info(f"FFmpeg found via imageio-ffmpeg: {bundled}")
            return bundled
    except ImportError:
        pass

    windows_paths = [
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe",
        Path.home() / "ffmpeg" / "bin" / "ffmpeg.exe",
        Path.home() / "scoop" / "apps" / "ffmpeg" / "current" / "bin" / "ffmpeg.exe",
    ]
    for p in windows_paths:
        if Path(p).exists():
            log.info(f"FFmpeg found at: {p}")
            return str(p)

    raise FileNotFoundError(
        "\n\n"
        "═══ FFmpeg not found! ═══\n"
        "StreamForge requires FFmpeg for video transcoding.\n\n"
        "Install options:\n"
        "  Windows:  winget install ffmpeg\n"
        "            OR: choco install ffmpeg\n"
        "            OR: scoop install ffmpeg\n"
        "            OR download from https://ffmpeg.org/download.html\n"
        "            Then add to PATH or set FFMPEG_PATH=C:\\ffmpeg\\bin\\ffmpeg.exe\n\n"
        "  macOS:    brew install ffmpeg\n"
        "  Linux:    sudo apt install ffmpeg\n\n"
        "  Quick fix (no install): pip install imageio-ffmpeg\n"
        "═══════════════════════\n"
    )


FFMPEG_BIN = find_ffmpeg()

# ABR ladder definition
ABR_LADDER = [
    {"name": "1080p", "height": 1080, "width": 1920, "video_br": "4500k", "audio_br": "192k", "preset": "ultrafast"},
    {"name": "720p",  "height": 720,  "width": 1280, "video_br": "2500k", "audio_br": "128k", "preset": "ultrafast"},
    {"name": "480p",  "height": 480,  "width": 852,  "video_br": "1200k", "audio_br": "96k",  "preset": "ultrafast"},
    {"name": "360p",  "height": 360,  "width": 640,  "video_br": "600k",  "audio_br": "64k",  "preset": "ultrafast"},
]


@dataclass
class TranscodeJob:
    stream_id: str
    input_path: str
    output_dir: Path
    started_at: float = field(default_factory=time.time)
    segments_written: int = 0
    process: Optional[subprocess.Popen] = None


class HLSPackager:
    """
    Manages the rolling HLS manifest for one stream.
    Appends new segment filenames and rewrites the .m3u8.
    """
    def __init__(self, stream_id: str, variant_name: str, output_dir: Path):
        self.stream_id = stream_id
        self.variant_name = variant_name
        self.output_dir = output_dir
        self.segments: List[str] = []
        self.sequence = 0
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def add_segment(self, filename: str, duration: float = SEGMENT_DURATION):
        self.segments.append((filename, duration))
        # Keep a larger window in memory for the manifest
        if len(self.segments) > SEGMENT_LIST_SIZE:
            self.segments.pop(0)
            self.sequence += 1
        self._write_manifest()

    def _write_manifest(self):
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{SEGMENT_DURATION}",
            f"#EXT-X-MEDIA-SEQUENCE:{self.sequence}",
        ]
        for fname, dur in self.segments:
            lines.append(f"#EXTINF:{dur:.3f},")
            lines.append(fname)

        manifest_path = self.output_dir / f"{self.variant_name}.m3u8"
        manifest_path.write_text("\n".join(lines) + "\n")


class MasterManifestWriter:
    """Writes the HLS master playlist linking all variant streams."""
    @staticmethod
    def write(stream_id: str, output_dir: Path):
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            "",
        ]
        bw_map  = {"1080p": 4700000, "720p": 2700000, "480p": 1300000, "360p": 700000}
        res_map = {"1080p": "1920x1080", "720p": "1280x720", "480p": "852x480", "360p": "640x360"}

        for v in ABR_LADDER:
            bw  = bw_map[v["name"]]
            res = res_map[v["name"]]
            lines.append(f'#EXT-X-STREAM-INF:BANDWIDTH={bw},RESOLUTION={res},NAME="{v["name"]}"')
            lines.append(f'{v["name"]}/{v["name"]}.m3u8')

        master_path = output_dir / "master.m3u8"
        master_path.write_text("\n".join(lines) + "\n")
        log.info(f"[{stream_id}] Master manifest written → {master_path}")


class StreamTranscoder:
    """
    Manages FFmpeg transcoding for one live stream.

    Key design decisions:
    - NO delete_segments: keeps all .ts files on disk so players never get 404
    - NO -re flag on transcoder: lets FFmpeg run slightly faster than real-time
      so segments are always ready ahead of the player
    - SEGMENT_LIST_SIZE=30: 60s of segments always in manifest
    - Segment watcher uses mtime: detects new vs existing segments reliably
    """
    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        self.output_dir = HLS_OUTPUT_DIR / stream_id
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.packagers: Dict[str, HLSPackager] = {}
        for v in ABR_LADDER:
            variant_dir = self.output_dir / v["name"]
            variant_dir.mkdir(parents=True, exist_ok=True)
            self.packagers[v["name"]] = HLSPackager(stream_id, v["name"], variant_dir)

        MasterManifestWriter.write(stream_id, self.output_dir)

        self._ffmpeg_proc: Optional[subprocess.Popen] = None
        self._running = False
        self._segment_watcher_thread: Optional[threading.Thread] = None
        self._segments_per_variant: Dict[str, int] = {v["name"]: 0 for v in ABR_LADDER}

    def build_ffmpeg_command(self, input_path: str) -> List[str]:
        """
        Build FFmpeg command for multi-bitrate HLS output.

        Notable flags:
          -stream_loop -1     : loop the source infinitely (live simulation)
          NO -re              : run faster than real-time so we always have segments ready
          -hls_list_size 30   : keep 30 segments (60s) in the manifest
          NO delete_segments  : never delete .ts files — players can always fetch them
          +split_by_time      : split segments cleanly at time boundaries
        """
        cmd = [
            FFMPEG_BIN,
            "-stream_loop", "-1",        # loop source infinitely
            "-i", input_path,
            "-y",                         # overwrite outputs
        ]

        for v in ABR_LADDER:
            variant_dir = str(self.output_dir / v["name"])
            fps = 30
            gop = SEGMENT_DURATION * fps   # keyframe every segment boundary

            cmd += [
                # Video
                "-vf",           f"scale={v['width']}:{v['height']}:force_original_aspect_ratio=decrease,pad={v['width']}:{v['height']}:(ow-iw)/2:(oh-ih)/2",
                "-c:v",          "libx264",
                "-preset",       v["preset"],
                "-tune",         "zerolatency",
                "-b:v",          v["video_br"],
                "-maxrate",      v["video_br"],
                "-bufsize",      f"{int(v['video_br'][:-1]) * 2}k",
                "-g",            str(gop),
                "-sc_threshold", "0",
                "-keyint_min",   str(gop),
                "-r",            str(fps),
                # Audio
                "-c:a",          "aac",
                "-b:a",          v["audio_br"],
                "-ar",           "44100",
                "-ac",           "2",
                # HLS output — NO delete_segments, keep all .ts files
                "-f",            "hls",
                "-hls_time",     str(SEGMENT_DURATION),
                "-hls_list_size", str(SEGMENT_LIST_SIZE),
                "-hls_flags",    "append_list+split_by_time",   # removed delete_segments
                "-hls_segment_filename", f"{variant_dir}/seg_%06d.ts",
                f"{variant_dir}/{v['name']}.m3u8",
            ]

        return cmd

    def start(self, source_video: str):
        """Start FFmpeg transcoding from a source video (simulates live ingest)."""
        self._running = True
        log.info(f"[{self.stream_id}] Starting transcoder → {source_video}")
        log.info(f"[{self.stream_id}] Output dir: {self.output_dir}")
        log.info(f"[{self.stream_id}] Segment window: {SEGMENT_LIST_SIZE} × {SEGMENT_DURATION}s = {SEGMENT_LIST_SIZE*SEGMENT_DURATION}s")

        cmd = self.build_ffmpeg_command(source_video)
        log.info(f"[{self.stream_id}] FFmpeg: {' '.join(cmd[:6])} ... ({len(cmd)} args total)")

        self._ffmpeg_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        threading.Thread(target=self._read_ffmpeg_logs, daemon=True).start()
        self._segment_watcher_thread = threading.Thread(
            target=self._watch_segments, daemon=True
        )
        self._segment_watcher_thread.start()
        log.info(f"[{self.stream_id}] FFmpeg PID: {self._ffmpeg_proc.pid}")

    def _read_ffmpeg_logs(self):
        """Read FFmpeg stderr — log progress lines, surface all errors."""
        if not self._ffmpeg_proc:
            return
        for raw in self._ffmpeg_proc.stderr:
            line = raw.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            # Always show speed/fps progress
            if "fps=" in line and "speed=" in line:
                log.info(f"[{self.stream_id}] {line[-120:]}")
            elif any(k in line.lower() for k in ["error", "invalid", "fail", "unable", "no such"]):
                log.error(f"[{self.stream_id}] FFmpeg ERROR: {line}")
            elif "hls" in line.lower() or "opening" in line.lower():
                log.debug(f"[{self.stream_id}] {line}")

        rc = self._ffmpeg_proc.wait()
        if rc != 0:
            log.error(f"[{self.stream_id}] FFmpeg exited with code {rc}")
        else:
            log.info(f"[{self.stream_id}] FFmpeg finished (code 0)")

    def _watch_segments(self):
        """
        Watch output dirs for new .ts segments using mtime.
        Uses file modification time to detect truly new segments,
        not just re-encountered existing ones.
        """
        # Track last-seen mtime per variant
        last_mtime: Dict[str, Dict[str, float]] = {v["name"]: {} for v in ABR_LADDER}

        while self._running:
            for v in ABR_LADDER:
                variant_dir = self.output_dir / v["name"]
                try:
                    if not variant_dir.exists():
                        continue
                    segs = sorted(variant_dir.glob("seg_*.ts"), key=lambda p: p.name)
                    for seg in segs:
                        try:
                            mtime = seg.stat().st_mtime
                        except FileNotFoundError:
                            continue

                        prev_mtime = last_mtime[v["name"]].get(seg.name, 0)
                        if mtime > prev_mtime:
                            last_mtime[v["name"]][seg.name] = mtime
                            # Only report as truly new if we haven't seen it before
                            if prev_mtime == 0:
                                self._segments_per_variant[v["name"]] += 1
                                self.packagers[v["name"]].add_segment(seg.name)
                                log.info(
                                    f"[{self.stream_id}] ✓ Segment {v['name']}/{seg.name} "
                                    f"(#{self._segments_per_variant[v['name']]})"
                                )
                except Exception as e:
                    log.error(f"Segment watcher error [{v['name']}]: {e}")
            time.sleep(0.25)   # poll every 250ms for low latency detection

    def stop(self):
        self._running = False
        if self._ffmpeg_proc:
            self._ffmpeg_proc.terminate()
            try:
                self._ffmpeg_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._ffmpeg_proc.kill()
            log.info(f"[{self.stream_id}] Transcoder stopped")

    def get_stats(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "running": self._running,
            "ffmpeg_pid": self._ffmpeg_proc.pid if self._ffmpeg_proc else None,
            "segments_per_variant": self._segments_per_variant,
            "output_dir": str(self.output_dir),
            "master_manifest": str(self.output_dir / "master.m3u8"),
        }


class TranscoderPipeline:
    """Orchestrates multiple concurrent stream transcoders."""
    def __init__(self):
        self.transcoders: Dict[str, StreamTranscoder] = {}

    def start_stream(self, stream_id: str, source_video: str) -> StreamTranscoder:
        if stream_id in self.transcoders:
            log.warning(f"[{stream_id}] Already transcoding, restarting...")
            self.stop_stream(stream_id)

        tc = StreamTranscoder(stream_id)
        tc.start(source_video)
        self.transcoders[stream_id] = tc
        return tc

    def stop_stream(self, stream_id: str):
        if stream_id in self.transcoders:
            self.transcoders[stream_id].stop()
            del self.transcoders[stream_id]

    def stop_all(self):
        for tc in list(self.transcoders.values()):
            tc.stop()
        self.transcoders.clear()

    def get_all_stats(self) -> dict:
        return {sid: tc.get_stats() for sid, tc in self.transcoders.items()}


if __name__ == "__main__":
    import sys
    stream_id = sys.argv[1] if len(sys.argv) > 1 else "ls-001"
    source    = sys.argv[2] if len(sys.argv) > 2 else "sample.mp4"

    pipeline = TranscoderPipeline()
    tc = pipeline.start_stream(stream_id, source)

    try:
        while True:
            stats = tc.get_stats()
            log.info(f"Stats: {json.dumps(stats, indent=2)}")
            time.sleep(5)
    except KeyboardInterrupt:
        pipeline.stop_all()
        log.info("Transcoder pipeline stopped")
        #test