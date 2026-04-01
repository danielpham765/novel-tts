from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ChapterEntry:
    chapter_number: int
    title: str
    url: str
    page_id: int | None = None
    part: int = 1
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class ParsedChapter:
    chapter_number: int
    title: str
    content: str


@dataclass
class FetchResult:
    url: str
    final_url: str
    html: str
    status_code: int | None = None
    title: str = ""
    strategy_name: str = ""
    challenge_detected: bool = False
    block_reason: str = ""
    proxy_name: str = ""
    proxy_server: str = ""
    debug_artifacts: list[Path] = field(default_factory=list)
