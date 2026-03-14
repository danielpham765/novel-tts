from __future__ import annotations

import re
from typing import Protocol

from novel_tts.common.text import normalize_whitespace

from .types import ChapterEntry, ParsedChapter


class SourceResolver(Protocol):
    source_id: str

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        ...

    def find_directory_page_urls(self, html: str, base_url: str) -> list[str]:
        ...

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        ...

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        ...


class BaseResolver:
    source_id: str

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        raise NotImplementedError

    def find_directory_page_urls(self, html: str, base_url: str) -> list[str]:
        return []

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        raise NotImplementedError

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        return None


def parse_chapter_number(text: str) -> int | None:
    normalized = normalize_whitespace(text)
    for pattern in (
        r"(?:chapter|chuong|ch(?:\.|apter)?)[^\d]{0,8}(\d+)",
        r"第\s*(\d+)\s*章",
        r"\b(\d{1,5})\b",
    ):
        match = re.search(pattern, normalized, flags=re.I)
        if match:
            return int(match.group(1))
    return None


def format_chapter_title(chapter_number: int, title: str) -> str:
    title = normalize_whitespace(re.sub(r"\s*\(\d+/\d+\)\s*$", "", title or ""))
    if not title:
        return f"Chuong {chapter_number}"
    if parse_chapter_number(title) == chapter_number:
        return title
    return f"Chuong {chapter_number} - {title}"
