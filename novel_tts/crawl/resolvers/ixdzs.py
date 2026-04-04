from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class IxdzsResolver(BaseResolver):
    source_id = "ixdzs"
    _NOISE_LINE_PATTERN = re.compile(
        r"(上一章|下一章|書籍頁|加入書架|看女頻小說每天能領現金紅包)",
        re.I,
    )

    @staticmethod
    def _extract_book_id(base_url: str) -> str:
        parsed = urlparse(base_url)
        match = re.search(r"/read/(\d+)/?", parsed.path)
        return match.group(1) if match else ""

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        soup = BeautifulSoup(html, "html.parser")
        book_id = self._extract_book_id(base_url)
        if not book_id:
            return {}

        latest = 0
        for url in (
            soup.select_one('meta[property="og:novel:latest_chapter_url"]'),
            soup.select_one('meta[property="og:url"]'),
        ):
            if not url:
                continue
            content = (url.get("content") or "").strip()
            match = re.search(r"/p(\d+)\.html", content)
            if match:
                latest = max(latest, int(match.group(1)))

        if latest <= 0:
            title_node = soup.select_one(".sub-text-r")
            latest = parse_chapter_number(title_node.get_text(" ", strip=True)) if title_node else 0
        if latest <= 0:
            return {}

        entries: dict[int, ChapterEntry] = {}
        for chapter_number in range(1, latest + 1):
            entries[chapter_number] = ChapterEntry(
                chapter_number=chapter_number,
                title=f"第{chapter_number}章",
                url=urljoin(base_url, f"/read/{book_id}/p{chapter_number}.html"),
            )
        return entries

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        soup = BeautifulSoup(html, "html.parser")
        for selector in ("script", "style", "noscript"):
            for node in soup.select(selector):
                node.decompose()

        raw_title = ""
        title_node = soup.select_one(".page-d-name") or soup.select_one("article h3") or soup.select_one("title")
        if title_node:
            raw_title = normalize_whitespace(title_node.get_text(" ", strip=True))
        if not raw_title:
            raw_title = fallback_title or f"第{expected_chapter_number}章"
        chapter_number = parse_chapter_number(raw_title) or expected_chapter_number

        lines: list[str] = []
        content_root = soup.select_one("article.page-content section") or soup.select_one("article.page-content")
        if content_root:
            for paragraph in content_root.select("p"):
                classes = paragraph.get("class") or []
                if "abg" in classes:
                    continue
                clean = normalize_whitespace(paragraph.get_text(" ", strip=True))
                if not clean:
                    continue
                if self._NOISE_LINE_PATTERN.search(clean):
                    continue
                if raw_title and clean == raw_title:
                    continue
                lines.append(clean)

        return ParsedChapter(
            chapter_number=chapter_number,
            title=raw_title.strip() or f"第{chapter_number}章",
            content="\n".join(lines),
        )
