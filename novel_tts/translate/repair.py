from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig
from novel_tts.translate.novel import (
    PLACEHOLDER_TOKEN_RE,
    chapter_part_path,
    count_han_chars,
    has_han,
    load_source_chapters,
)

LOGGER = get_logger(__name__)
CHAPTER_HEADING_RE = re.compile(r"(?m)^Chương\s+(\d+):")


def _count_duplicate_paragraphs(text: str, *, min_len: int = 120) -> int:
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    seen: set[str] = set()
    dup_count = 0
    for para in paras:
        if len(para) < min_len:
            continue
        if para in seen:
            dup_count += 1
        else:
            seen.add(para)
    return dup_count


@dataclass(frozen=True)
class RepairJob:
    job_id: str
    file_name: str
    chapter_num: int
    reasons: tuple[str, ...]


def _job_id(file_name: str, chapter_num: int) -> str:
    return f"{file_name}::{int(chapter_num):04d}"


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def find_repair_jobs_in_range(config: NovelConfig, start: int, end: int) -> list[RepairJob]:
    """Find chapters in [start, end] that likely need re-translation."""
    if start > end:
        start, end = end, start

    jobs: list[RepairJob] = []
    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num_str, _chapter_text in load_source_chapters(config, source_path):
            try:
                chap = int(str(chapter_num_str))
            except Exception:
                continue
            if chap < start or chap > end:
                continue

            reasons: list[str] = []
            part_path = chapter_part_path(config, source_path, str(chap))
            if not part_path.exists():
                reasons.append("missing-part")
            else:
                if part_path.stat().st_size <= 0:
                    reasons.append("empty-part")
                if part_path.stat().st_mtime < source_path.stat().st_mtime:
                    reasons.append("stale-part")
                text = _read_text(part_path)
                if not text.strip():
                    reasons.append("empty-part")
                if PLACEHOLDER_TOKEN_RE.search(text):
                    reasons.append("placeholder-token")
                if has_han(text):
                    # Even a few Han chars means the translation is incomplete or has residue.
                    reasons.append(f"han-residue:{count_han_chars(text)}")
                heading_count = len(CHAPTER_HEADING_RE.findall(text))
                if heading_count > 1:
                    reasons.append(f"duplicate-headers:{heading_count}")
                dup_paras = _count_duplicate_paragraphs(text)
                if dup_paras >= 3:
                    reasons.append(f"duplicate-paragraphs:{dup_paras}")

            if reasons:
                jobs.append(
                    RepairJob(
                        job_id=_job_id(source_path.name, chap),
                        file_name=source_path.name,
                        chapter_num=chap,
                        reasons=tuple(reasons),
                    )
                )
    return jobs


def enqueue_repair_jobs(config: NovelConfig, jobs: list[RepairJob]) -> int:
    if not jobs:
        print("No repair jobs found.")
        return 0

    # Import locally to avoid import cycles at module import time.
    from novel_tts.queue.translation_queue import add_job_ids_to_queue

    job_ids = [job.job_id for job in jobs]
    return add_job_ids_to_queue(config, job_ids, force=True, label="translate repair")
