from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from novel_tts.common.ffmpeg import run_ffmpeg
from novel_tts.config.models import NovelConfig


@dataclass
class BackgroundOptimizationResult:
    path: Path
    duration_seconds: float
    original_size_bytes: int
    optimized_size_bytes: int
    bitrate_bps: int
    codec_name: str
    width: int
    height: int
    frame_rate: str

    @property
    def bytes_saved(self) -> int:
        return self.original_size_bytes - self.optimized_size_bytes


def _background_path(config: NovelConfig) -> Path:
    return config.storage.image_dir / "background.mp4"


def _temp_output_path(background_path: Path) -> Path:
    return background_path.with_name(f"{background_path.stem}.optimized.tmp{background_path.suffix}")


def _ffprobe_json(path: Path) -> dict:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name,width,height,r_frame_rate,bit_rate",
            "-show_entries",
            "format=duration,size,bit_rate",
            "-of",
            "json",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout or "{}")


def optimize_background_video(
    config: NovelConfig,
    *,
    crf: int = 24,
    preset: str = "slow",
) -> BackgroundOptimizationResult:
    background_path = _background_path(config)
    if not background_path.exists():
        raise FileNotFoundError(f"Background video not found: {background_path}")

    original_size = background_path.stat().st_size
    temp_output = _temp_output_path(background_path)
    if temp_output.exists():
        temp_output.unlink()

    run_ffmpeg(
        [
            "-y",
            "-i",
            str(background_path),
            "-map",
            "0:v:0",
            "-c:v",
            "libx265",
            "-preset",
            str(preset),
            "-crf",
            str(int(crf)),
            "-tag:v",
            "hvc1",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(temp_output),
        ]
    )

    if not temp_output.exists() or temp_output.stat().st_size <= 0:
        raise RuntimeError(f"Optimized background video was not created: {temp_output}")

    probe = _ffprobe_json(temp_output)
    stream = (probe.get("streams") or [{}])[0]
    fmt = probe.get("format") or {}
    optimized_size = temp_output.stat().st_size
    os.replace(temp_output, background_path)

    return BackgroundOptimizationResult(
        path=background_path,
        duration_seconds=float(fmt.get("duration", 0.0) or 0.0),
        original_size_bytes=original_size,
        optimized_size_bytes=optimized_size,
        bitrate_bps=int(fmt.get("bit_rate", 0) or 0),
        codec_name=str(stream.get("codec_name", "") or ""),
        width=int(stream.get("width", 0) or 0),
        height=int(stream.get("height", 0) or 0),
        frame_rate=str(stream.get("r_frame_rate", "") or ""),
    )
