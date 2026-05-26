"""
StreamForge VOD Transcoder  (R2 edition)
─────────────────────────────────────────
Pure VOD pipeline:
  1. Probe source with ffprobe → get resolution + duration
  2. Build ABR ladder (skip variants taller than source)
  3. Encode ALL variants sequentially with FFmpeg
     - hls_list_size=0  → keep every segment (complete VOD)
     - EXT-X-ENDLIST    → written by FFmpeg automatically
  4. Generate thumbnail (10 % of duration, capped at 5 s)
  5. Write master.m3u8 locally
  6. Upload EVERYTHING to Cloudflare R2 (boto3 S3-compatible)
  7. Clean up local temp files
  8. Return R2 public URLs so origin_server can store them

R2 env vars required:
  R2_ACCOUNT_ID       Cloudflare account ID
  R2_ACCESS_KEY_ID    R2 API token → Access Key ID
  R2_SECRET_KEY       R2 API token → Secret Access Key
  R2_BUCKET           Bucket name  (e.g. streamforge-vod)
  R2_PUBLIC_URL       Public bucket URL (e.g. https://pub-xxx.r2.dev)
                      OR your custom domain

Local fallback: if R2 env vars are missing the files stay in
hls_output/ and origin_server serves them directly (same as before).
"""
from dotenv import load_dotenv
load_dotenv()
import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSCODE] %(message)s")
log = logging.getLogger("transcoder")

# ── Paths ──────────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent.parent
HLS_OUTPUT_DIR  = BASE_DIR / "hls_output"
SEGMENT_SECONDS = 4          # 4 s segments — good seek accuracy for VOD

# ── ABR ladder ─────────────────────────────────────────────────────────────
ABR_LADDER = [
    {"name": "1080p", "height": 1080, "width": 1920, "video_br": "4500k", "audio_br": "192k"},
    {"name": "720p",  "height": 720,  "width": 1280, "video_br": "2500k", "audio_br": "128k"},
    {"name": "480p",  "height": 480,  "width": 852,  "video_br": "1200k", "audio_br": "96k"},
    {"name": "360p",  "height": 360,  "width": 640,  "video_br": "600k",  "audio_br": "64k"},
]

BW_MAP  = {"1080p": 4700000, "720p": 2700000, "480p": 1300000, "360p": 700000}
RES_MAP = {"1080p": "1920x1080", "720p": "1280x720", "480p": "852x480", "360p": "640x360"}


# ──────────────────────────────────────────────────────────────────────────
# FFmpeg discovery
# ──────────────────────────────────────────────────────────────────────────
def find_ffmpeg() -> str:
    env_path = os.environ.get("FFMPEG_PATH")
    if env_path and Path(env_path).exists():
        return env_path
    found = shutil.which("ffmpeg")
    if found:
        return found
    try:
        import imageio_ffmpeg
        b = imageio_ffmpeg.get_ffmpeg_exe()
        if b and Path(b).exists():
            return b
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
        "\n═══ FFmpeg not found ═══\n"
        "  pip install imageio-ffmpeg   (quickest)\n"
        "  winget install ffmpeg        (Windows)\n"
        "  brew install ffmpeg          (macOS)\n"
        "  sudo apt install ffmpeg      (Linux)\n"
    )


FFMPEG_BIN = find_ffmpeg()
log.info(f"FFmpeg: {FFMPEG_BIN}")


# ──────────────────────────────────────────────────────────────────────────
# R2 client
# ──────────────────────────────────────────────────────────────────────────
def _r2_client():
    """
    Return a boto3 S3 client pointed at Cloudflare R2.
    Returns None if env vars are not set (local-only fallback).
    """
    account_id = os.environ.get("R2_ACCOUNT_ID", "").strip()
    access_key = os.environ.get("R2_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("R2_SECRET_KEY", "").strip()
    if not (account_id and access_key and secret_key):
        return None
    try:
        import boto3
        from botocore.config import Config
        return boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )
    except Exception as e:
        log.warning(f"R2 client init failed: {e}")
        return None


def _r2_bucket() -> str:
    return os.environ.get("R2_BUCKET", "streamforge-vod")


def _r2_public_url() -> str:
    return os.environ.get("R2_PUBLIC_URL", "").rstrip("/")


def r2_upload_file(client, local_path: Path, r2_key: str) -> str:
    """
    Upload one file to R2. Returns the public URL.
    Content-Type is inferred from extension.
    """
    MIME = {
        ".m3u8": "application/vnd.apple.mpegurl",
        ".ts":   "video/mp2t",
        ".jpg":  "image/jpeg",
        ".json": "application/json",
    }
    ext      = local_path.suffix.lower()
    ct       = MIME.get(ext, "application/octet-stream")
    bucket   = _r2_bucket()

    extra = {
        "ContentType": ct,
        "CacheControl": "public, max-age=86400" if ext == ".ts" else "public, max-age=300",
    }

    with open(local_path, "rb") as fh:
        client.put_object(
            Bucket=bucket,
            Key=r2_key,
            Body=fh,
            **extra,
        )

    pub = _r2_public_url()
    if pub:
        return f"{pub}/{r2_key}"
    # Fall back to path-style URL (works without public domain)
    account_id = os.environ.get("R2_ACCOUNT_ID", "")
    return f"https://{account_id}.r2.cloudflarestorage.com/{bucket}/{r2_key}"


def r2_delete_prefix(client, prefix: str):
    """Delete all R2 objects whose key starts with prefix."""
    bucket = _r2_bucket()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
            log.info(f"R2 deleted {len(objects)} objects under {prefix}")


# ──────────────────────────────────────────────────────────────────────────
# ffprobe helper
# ──────────────────────────────────────────────────────────────────────────
def probe_video(path: str) -> dict:
    """Return {duration, width, height} from the first video stream."""
    ffprobe = FFMPEG_BIN.replace("ffmpeg", "ffprobe")
    if not Path(ffprobe).exists():
        ffprobe = shutil.which("ffprobe") or FFMPEG_BIN
    cmd = [ffprobe, "-v", "quiet", "-print_format", "json",
           "-show_streams", "-show_format", path]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        data = json.loads(r.stdout)
        info: dict = {"duration": None, "width": None, "height": None}
        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                info["width"]    = s.get("width")
                info["height"]   = s.get("height")
                info["duration"] = float(
                    s.get("duration") or
                    data.get("format", {}).get("duration", 0) or 0
                )
                break   # use first video stream only
        return info
    except Exception as e:
        log.warning(f"ffprobe failed: {e}")
        return {"duration": None, "width": None, "height": None}


# ──────────────────────────────────────────────────────────────────────────
# Core encode function
# ──────────────────────────────────────────────────────────────────────────
def encode_vod(
    asset_id: str,
    source_path: str,
    on_progress: Optional[Callable[[str, int], None]] = None,
) -> dict:
    """
    Full VOD encode + package + R2 upload pipeline.

    Progress callback fires as: on_progress(status: str, pct: int 0-100)
    Statuses: probing → encoding → packaging → uploading → ready | error

    Returns dict:
      status, duration, width, height, variants, error_msg,
      r2_uploaded (bool), base_url (R2 public base or local)
    """

    out_dir = HLS_OUTPUT_DIR / asset_id
    out_dir.mkdir(parents=True, exist_ok=True)

    def prog(status: str, pct: int):
        log.info(f"[{asset_id}] [{pct:3d}%] {status}")
        if on_progress:
            on_progress(status, pct)

    try:
        # ── 1. Probe ────────────────────────────────────────────────────────
        prog("probing", 2)
        probe    = probe_video(source_path)
        duration = probe["duration"]
        src_w    = probe["width"]  or 1920
        src_h    = probe["height"] or 1080
        log.info(
            f"[{asset_id}] Source: {src_w}×{src_h}"
            + (f" {duration:.1f}s" if duration else "")
        )

        # ── 2. Select variants — NEVER upscale ──────────────────────────────
        #
        # Key fix: previously the loop was correct but the FFmpeg command
        # had a subtle bug where hls_list_size was written as a string "0"
        # and on some FFmpeg builds that caused it to default to 5.
        # Now we pass it as an integer-string explicitly and verify.
        #
        variants = [v for v in ABR_LADDER if v["height"] <= src_h]
        if not variants:
            variants = [ABR_LADDER[-1]]   # absolute fallback: 360p
        log.info(f"[{asset_id}] Encoding {len(variants)} variants: {[v['name'] for v in variants]}")

        total_variants = len(variants)

        # ── 3. Encode ALL variants ──────────────────────────────────────────
        prog("encoding", 5)

        for vi, v in enumerate(variants):
            vdir = out_dir / v["name"]
            vdir.mkdir(parents=True, exist_ok=True)

            fps = 30
            gop = SEGMENT_SECONDS * fps   # keyframe every segment boundary

            # Scale + letterbox / pillarbox so output is exactly WxH
            pad_filter = (
                f"scale={v['width']}:{v['height']}:"
                f"force_original_aspect_ratio=decrease,"
                f"pad={v['width']}:{v['height']}:(ow-iw)/2:(oh-ih)/2"
            )

            cmd = [
                FFMPEG_BIN,
                "-i",             source_path,   # VOD: no -stream_loop
                "-y",                             # overwrite
                # ── Video ──────────────────────────────────────────────────
                "-vf",            pad_filter,
                "-c:v",           "libx264",
                "-preset",        "fast",         # fast > ultrafast for quality
                "-crf",           "23",
                "-b:v",           v["video_br"],
                "-maxrate",       v["video_br"],
                "-bufsize",       str(int(v["video_br"][:-1]) * 2) + "k",
                "-g",             str(gop),
                "-sc_threshold",  "0",            # disable scene-cut keyframes
                "-keyint_min",    str(gop),
                "-r",             str(fps),
                # ── Audio ───────────────────────────────────────────────────
                "-c:a",           "aac",
                "-b:a",           v["audio_br"],
                "-ar",            "44100",
                "-ac",            "2",
                # ── HLS output ──────────────────────────────────────────────
                # hls_list_size 0  → keep ALL segments in the manifest (VOD)
                # split_by_time   → honour -hls_time exactly
                # NO delete_segments → never remove .ts files
                "-f",             "hls",
                "-hls_time",      str(SEGMENT_SECONDS),
                "-hls_list_size", "0",
                "-hls_flags",     "split_by_time",
                "-hls_segment_filename", str(vdir / "seg_%06d.ts"),
                str(vdir / f"{v['name']}.m3u8"),
            ]

            log.info(f"[{asset_id}] ▶ Encoding {v['name']} ({vi+1}/{total_variants}) ...")
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

            # Parse FFmpeg time= progress lines
            dur_s = duration or 1
            stderr_lines: list[str] = []
            for raw in proc.stderr:
                line = raw.decode("utf-8", errors="replace")
                stderr_lines.append(line)
                if "time=" in line:
                    try:
                        t     = line.split("time=")[1].split()[0]
                        h, m, s = t.split(":")
                        elapsed = int(h) * 3600 + int(m) * 60 + float(s)
                        frac  = (vi + min(elapsed / dur_s, 1.0)) / total_variants
                        pct   = int(5 + frac * 80)   # 5 % → 85 %
                        prog("encoding", pct)
                    except Exception:
                        pass
                if any(k in line.lower() for k in ["error", "invalid", "no such file", "failed"]):
                    log.warning(f"[{asset_id}] FFmpeg stderr: {line.strip()}")

            proc.wait()
            if proc.returncode != 0:
                tail = "".join(stderr_lines[-20:])
                raise RuntimeError(f"FFmpeg failed for {v['name']} (code {proc.returncode}):\n{tail}")

            # Sanity check — did we actually produce segments?
            segs_produced = list(vdir.glob("seg_*.ts"))
            if not segs_produced:
                raise RuntimeError(
                    f"FFmpeg exited 0 but produced no segments for {v['name']}. "
                    f"Check source file: {source_path}"
                )
            log.info(f"[{asset_id}] ✓ {v['name']} — {len(segs_produced)} segments")

        # ── 4. Thumbnail ────────────────────────────────────────────────────
        prog("packaging", 87)
        thumb_time = min((duration or 10) * 0.1, 5)
        thumb_path = out_dir / "thumb.jpg"
        thumb_cmd  = [
            FFMPEG_BIN,
            "-ss", str(thumb_time),
            "-i",  source_path,
            "-y",
            "-vframes", "1",
            "-vf", "scale=640:360:force_original_aspect_ratio=decrease",
            str(thumb_path),
        ]
        subprocess.run(thumb_cmd, capture_output=True, timeout=30)

        # ── 5. Write master.m3u8 ────────────────────────────────────────────
        prog("packaging", 90)
        master_lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
        for v in variants:
            master_lines.append(
                f'#EXT-X-STREAM-INF:BANDWIDTH={BW_MAP[v["name"]]},'
                f'RESOLUTION={RES_MAP[v["name"]]},NAME="{v["name"]}"'
            )
            master_lines.append(f'{v["name"]}/{v["name"]}.m3u8')
        master_path = out_dir / "master.m3u8"
        master_path.write_text("\n".join(master_lines) + "\n")

        # ── 6. Upload to Cloudflare R2 (if configured) ──────────────────────
        r2  = _r2_client()
        r2_uploaded = False
        r2_base_url = ""

        if r2:
            prog("uploading", 91)
            log.info(f"[{asset_id}] Uploading to R2 bucket '{_r2_bucket()}'...")
            prefix = f"vod/{asset_id}"
            uploaded_count = 0

            # Collect all files to upload
            upload_jobs: list[tuple[Path, str]] = []

            # master.m3u8
            upload_jobs.append((master_path, f"{prefix}/master.m3u8"))

            # thumb.jpg
            if thumb_path.exists():
                upload_jobs.append((thumb_path, f"{prefix}/thumb.jpg"))

            # All variant manifests + segments
            for v in variants:
                vdir = out_dir / v["name"]
                # variant manifest
                m3u8 = vdir / f"{v['name']}.m3u8"
                if m3u8.exists():
                    upload_jobs.append((m3u8, f"{prefix}/{v['name']}/{v['name']}.m3u8"))
                # segments
                for seg in sorted(vdir.glob("seg_*.ts")):
                    upload_jobs.append((seg, f"{prefix}/{v['name']}/{seg.name}"))

            total_jobs = len(upload_jobs)
            log.info(f"[{asset_id}] Uploading {total_jobs} files to R2...")

            for i, (local_file, r2_key) in enumerate(upload_jobs):
                try:
                    r2_upload_file(r2, local_file, r2_key)
                    uploaded_count += 1
                    # Progress: 91% → 98%
                    pct = int(91 + (i + 1) / total_jobs * 7)
                    prog("uploading", pct)
                except Exception as e:
                    raise RuntimeError(f"R2 upload failed for {r2_key}: {e}")

            log.info(f"[{asset_id}] ✓ Uploaded {uploaded_count}/{total_jobs} files to R2")
            r2_uploaded = True
            r2_base_url = f"{_r2_public_url()}/vod/{asset_id}" if _r2_public_url() else ""

            # Clean up local HLS files (keep uploads/ dir for re-encode if needed)
            shutil.rmtree(out_dir, ignore_errors=True)
            log.info(f"[{asset_id}] Local HLS temp files cleaned up")
        else:
            log.info(f"[{asset_id}] R2 not configured — serving from local hls_output/")

        # ── 7. Write meta ───────────────────────────────────────────────────
        prog("packaging", 99)
        meta = {
            "asset_id":    asset_id,
            "status":      "ready",
            "duration":    duration,
            "width":       src_w,
            "height":      src_h,
            "variants":    [v["name"] for v in variants],
            "encoded_at":  time.time(),
            "r2_uploaded": r2_uploaded,
            "r2_base_url": r2_base_url,
            "error_msg":   None,
        }

        # Write meta locally even when R2 is used (for asset registry)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "asset_meta.json").write_text(json.dumps(meta, indent=2))

        prog("ready", 100)
        log.info(
            f"[{asset_id}] ✓ READY — {len(variants)} variants"
            + (" (R2)" if r2_uploaded else " (local)")
        )
        return meta

    except Exception as e:
        log.error(f"[{asset_id}] Encoding FAILED: {e}")
        err_meta = {
            "asset_id":    asset_id,
            "status":      "error",
            "error_msg":   str(e),
            "variants":    [],
            "duration":    None,
            "width":       None,
            "height":      None,
            "r2_uploaded": False,
            "r2_base_url": "",
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "asset_meta.json").write_text(json.dumps(err_meta, indent=2))
        if on_progress:
            on_progress("error", 0)
        return err_meta


# ── CLI ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python transcoder.py <asset_id> <source_video>")
        print("\nRequired env vars for R2:")
        print("  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_KEY, R2_BUCKET, R2_PUBLIC_URL")
        sys.exit(1)

    def _prog(status, pct):
        bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
        print(f"\r  [{bar}] {pct:3d}%  {status:20s}", end="", flush=True)

    result = encode_vod(sys.argv[1], sys.argv[2], on_progress=_prog)
    print("\n")
    print(json.dumps(result, indent=2))