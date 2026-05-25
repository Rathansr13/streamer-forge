"""
StreamForge VOD Transcoder
──────────────────────────
Pure VOD pipeline — no live streaming, no looping.

Given a source video file, this module:
  1. Probes source with ffprobe (resolution, duration)
  2. Encodes only variants ≤ source height (no upscaling)
  3. Writes HLS segments with hls_list_size=0 (keeps ALL segments)
  4. Adds #EXT-X-ENDLIST so players know it's a complete VOD asset
  5. Generates a thumbnail at 10% of duration
  6. Writes master.m3u8 + asset_meta.json
  7. Updates encode_progress (0-100) so the API can poll it

Called from origin_server.py in a background thread.
"""

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSCODE] %(message)s")
log = logging.getLogger("transcoder")

HLS_OUTPUT_DIR  = Path(__file__).parent.parent / "hls_output"
SEGMENT_SECONDS = 4      # VOD: 4s segments — good balance of seek accuracy vs file count


def find_ffmpeg() -> str:
    """
    Locate FFmpeg binary with fallback chain:
      1. FFMPEG_PATH environment variable
      2. System PATH
      3. imageio-ffmpeg bundled binary (pip install imageio-ffmpeg)
      4. Common Windows install locations
    """
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
        "\n\n═══ FFmpeg not found! ═══\n"
        "  pip install imageio-ffmpeg   (quickest fix)\n"
        "  winget install ffmpeg        (Windows)\n"
        "  brew install ffmpeg          (macOS)\n"
        "  sudo apt install ffmpeg      (Linux)\n"
        "═══════════════════════\n"
    )


FFMPEG_BIN = find_ffmpeg()

# ABR ladder — only variants at or below source height are encoded
ABR_LADDER = [
    {"name": "1080p", "height": 1080, "width": 1920, "video_br": "4500k", "audio_br": "192k"},
    {"name": "720p",  "height": 720,  "width": 1280, "video_br": "2500k", "audio_br": "128k"},
    {"name": "480p",  "height": 480,  "width": 852,  "video_br": "1200k", "audio_br": "96k"},
    {"name": "360p",  "height": 360,  "width": 640,  "video_br": "600k",  "audio_br": "64k"},
]

BW_MAP  = {"1080p": 4700000, "720p": 2700000, "480p": 1300000, "360p": 700000}
RES_MAP = {"1080p": "1920x1080", "720p": "1280x720", "480p": "852x480", "360p": "640x360"}


def probe_video(path: str) -> dict:
    """Return {duration, width, height} via ffprobe."""
    ffprobe = FFMPEG_BIN.replace("ffmpeg", "ffprobe")
    if not Path(ffprobe).exists():
        ffprobe = shutil.which("ffprobe") or FFMPEG_BIN
    cmd = [ffprobe, "-v", "quiet", "-print_format", "json",
           "-show_streams", "-show_format", path]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        data = json.loads(r.stdout)
        info = {"duration": None, "width": None, "height": None}
        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                info["width"]    = s.get("width")
                info["height"]   = s.get("height")
                info["duration"] = float(
                    s.get("duration") or
                    data.get("format", {}).get("duration", 0) or 0
                )
        return info
    except Exception as e:
        log.warning(f"ffprobe failed: {e}")
        return {"duration": None, "width": None, "height": None}


def encode_vod(asset_id: str, source_path: str, on_progress=None):
    """
    Full VOD encode + package pipeline.

    Args:
        asset_id:    unique asset identifier — output goes to hls_output/{asset_id}/
        source_path: absolute path to the uploaded video file
        on_progress: optional callback(status:str, pct:int) for live progress updates

    Returns:
        dict with keys: status, duration, width, height, variants, error_msg
    """
    out_dir = HLS_OUTPUT_DIR / asset_id
    out_dir.mkdir(parents=True, exist_ok=True)

    def progress(status: str, pct: int):
        if on_progress:
            on_progress(status, pct)

    try:
        # ── 1. Probe ───────────────────────────────────────────────────────
        progress("probing", 2)
        probe = probe_video(source_path)
        duration = probe["duration"]
        src_w    = probe["width"]  or 1920
        src_h    = probe["height"] or 1080
        log.info(f"[{asset_id}] Source: {src_w}x{src_h} duration={duration:.1f}s" if duration else f"[{asset_id}] Source: {src_w}x{src_h}")

        # ── 2. Select variants (never upscale) ────────────────────────────
        variants = [v for v in ABR_LADDER if v["height"] <= src_h]
        if not variants:
            variants = [ABR_LADDER[-1]]   # fallback: 360p
        log.info(f"[{asset_id}] Variants to encode: {[v['name'] for v in variants]}")

        # ── 3. Encode each variant ─────────────────────────────────────────
        progress("encoding", 5)
        total = len(variants)

        for vi, v in enumerate(variants):
            vdir = out_dir / v["name"]
            vdir.mkdir(parents=True, exist_ok=True)

            fps = 30
            gop = SEGMENT_SECONDS * fps
            pad_filter = (
                f"scale={v['width']}:{v['height']}:force_original_aspect_ratio=decrease,"
                f"pad={v['width']}:{v['height']}:(ow-iw)/2:(oh-ih)/2"
            )

            cmd = [
                FFMPEG_BIN,
                "-i", source_path,          # no -stream_loop: VOD plays once
                "-y",
                "-vf",           pad_filter,
                "-c:v",          "libx264",
                "-preset",       "fast",
                "-crf",          "23",
                "-b:v",          v["video_br"],
                "-maxrate",      v["video_br"],
                "-bufsize",      f"{int(v['video_br'][:-1]) * 2}k",
                "-g",            str(gop),
                "-sc_threshold", "0",
                "-keyint_min",   str(gop),
                "-r",            str(fps),
                "-c:a",          "aac",
                "-b:a",          v["audio_br"],
                "-ar",           "44100",
                "-ac",           "2",
                # VOD HLS flags:
                #   hls_list_size=0   → keep ALL segments (no sliding window)
                #   EXT-X-ENDLIST     → written automatically by FFmpeg for VOD
                "-f",            "hls",
                "-hls_time",     str(SEGMENT_SECONDS),
                "-hls_list_size", "0",
                "-hls_flags",    "split_by_time",
                "-hls_segment_filename", str(vdir / "seg_%06d.ts"),
                str(vdir / f"{v['name']}.m3u8"),
            ]

            log.info(f"[{asset_id}] Encoding {v['name']} ({vi+1}/{total})...")
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Parse FFmpeg progress from stderr
            dur_s = duration or 1
            for raw in proc.stderr:
                line = raw.decode("utf-8", errors="replace")
                if "time=" in line:
                    try:
                        t = line.split("time=")[1].split()[0]
                        h, m, s = t.split(":")
                        elapsed = int(h) * 3600 + int(m) * 60 + float(s)
                        variant_frac = (vi + min(elapsed / dur_s, 1.0)) / total
                        pct = int(5 + variant_frac * 83)   # 5%–88%
                        progress("encoding", pct)
                    except Exception:
                        pass
                if any(k in line.lower() for k in ["error", "invalid", "no such file"]):
                    log.error(f"[{asset_id}] FFmpeg: {line.strip()}")

            proc.wait()
            if proc.returncode != 0:
                stderr_tail = proc.stderr.read().decode("utf-8", errors="replace")[-400:]
                raise RuntimeError(f"FFmpeg failed ({v['name']}): {stderr_tail}")

            log.info(f"[{asset_id}] ✓ {v['name']} encoded")

        # ── 4. Thumbnail at 10% of duration ──────────────────────────────
        progress("packaging", 90)
        thumb_time = min((duration or 10) * 0.1, 5)
        thumb_cmd  = [
            FFMPEG_BIN, "-ss", str(thumb_time), "-i", source_path, "-y",
            "-vframes", "1",
            "-vf", "scale=640:360:force_original_aspect_ratio=decrease",
            str(out_dir / "thumb.jpg")
        ]
        subprocess.run(thumb_cmd, capture_output=True, timeout=30)

        # ── 5. Write master.m3u8 ─────────────────────────────────────────
        progress("packaging", 95)
        lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
        for v in variants:
            lines.append(
                f'#EXT-X-STREAM-INF:BANDWIDTH={BW_MAP[v["name"]]},'
                f'RESOLUTION={RES_MAP[v["name"]]},NAME="{v["name"]}"'
            )
            lines.append(f'{v["name"]}/{v["name"]}.m3u8')
        (out_dir / "master.m3u8").write_text("\n".join(lines) + "\n")

        # ── 6. Write asset_meta.json ──────────────────────────────────────
        meta = {
            "asset_id":    asset_id,
            "status":      "ready",
            "duration":    duration,
            "width":       src_w,
            "height":      src_h,
            "variants":    [v["name"] for v in variants],
            "encoded_at":  time.time(),
        }
        (out_dir / "asset_meta.json").write_text(json.dumps(meta, indent=2))

        progress("ready", 100)
        log.info(f"[{asset_id}] ✓ READY — {len(variants)} variants")
        return {**meta, "error_msg": None}

    except Exception as e:
        log.error(f"[{asset_id}] Encoding FAILED: {e}")
        err_meta = {
            "asset_id": asset_id, "status": "error",
            "error_msg": str(e), "variants": [],
            "duration": None, "width": None, "height": None,
        }
        (out_dir / "asset_meta.json").write_text(json.dumps(err_meta, indent=2))
        if on_progress:
            on_progress("error", 0)
        return err_meta


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python transcoder.py <asset_id> <source_video>")
        sys.exit(1)
    asset_id    = sys.argv[1]
    source_path = sys.argv[2]

    def on_prog(status, pct):
        print(f"  [{pct:3d}%] {status}")

    result = encode_vod(asset_id, source_path, on_progress=on_prog)
    print(json.dumps(result, indent=2))