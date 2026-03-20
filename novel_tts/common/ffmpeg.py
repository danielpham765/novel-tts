from __future__ import annotations

import json
import subprocess
from pathlib import Path


def run_ffmpeg(args: list[str]) -> None:
    subprocess.run(["ffmpeg", *args], check=True)


def ffmpeg_has_filter(filter_name: str) -> bool:
    result = subprocess.run(
        ["ffmpeg", "-hide_banner", "-filters"],
        check=True,
        capture_output=True,
        text=True,
    )
    marker = f" {filter_name} "
    return any(marker in line for line in result.stdout.splitlines())


def ffprobe_duration(path: Path) -> float:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "json",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout or "{}")
    return float(payload.get("format", {}).get("duration", 0.0) or 0.0)
