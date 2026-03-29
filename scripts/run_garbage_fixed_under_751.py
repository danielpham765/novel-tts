#!/usr/bin/env python3

from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime
from pathlib import Path


ROOT_DIR = Path("/Users/danielpham/sync-workspace/05_Stories/novel-tts")
LIST_FILE = ROOT_DIR / "input" / "thai-hu-chi-ton" / "garbage_fixed.txt"
LOG_DIR = ROOT_DIR / ".logs" / "thai-hu-chi-ton"
LOG_FILE = LOG_DIR / "garbage_fixed_under_751.log"
UV_CACHE_DIR = Path("/tmp/uv-cache")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append_log(message: str) -> None:
    line = f"{message}\n"
    print(message, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line)


def finished_batches() -> set[str]:
    if not LOG_FILE.exists():
        return set()
    text = LOG_FILE.read_text(encoding="utf-8", errors="ignore")
    return set(re.findall(r"finished (chuong_\d+-\d+)", text))


def iter_batches() -> list[str]:
    batches: list[str] = []
    for raw in LIST_FILE.read_text(encoding="utf-8").splitlines():
        batch = raw.strip()
        if batch.startswith("chuong_"):
            batches.append(batch)
    return batches


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    UV_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    append_log(f"== {now_str()} start run_garbage_fixed_under_751.py ==")

    done = finished_batches()
    env = os.environ.copy()
    env["UV_CACHE_DIR"] = str(UV_CACHE_DIR)

    for batch in iter_batches():
        range_part = batch.removeprefix("chuong_")
        start_str, end_str = range_part.split("-", 1)
        start = int(start_str)
        if start >= 751:
            continue

        if batch in done:
            append_log(f"== {now_str()} skip {batch} (already finished) ==")
            continue

        append_log(f"")
        append_log(f"== {now_str()} running {batch} ==")

        cmd = [
            "uv",
            "run",
            "novel-tts",
            "pipeline",
            "run",
            "thai-hu-chi-ton",
            "--from-stage",
            "tts",
            "--to-stage",
            "video",
            "--force",
            "--range",
            range_part,
        ]

        with LOG_FILE.open("a", encoding="utf-8") as log_fh:
            proc = subprocess.run(
                cmd,
                cwd=ROOT_DIR,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                text=True,
            )

        if proc.returncode != 0:
            append_log(f"== {now_str()} failed {batch} (exit={proc.returncode}) ==")
            return proc.returncode

        append_log(f"== {now_str()} finished {batch} ==")

    append_log(f"")
    append_log(f"== {now_str()} completed run_garbage_fixed_under_751.py ==")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
