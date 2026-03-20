from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class TtkanResolver(BaseResolver):
    source_id = "ttkan"
    _NOISE_LINE_PATTERN = re.compile(
        r"("
        r"上一章|下一章|返回目录|加入书签|加入收藏|本章完|手机阅读"
        r"|^作者[:：]"
        r"|^更新时间[:：]"
        r"|^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?$"
        r")",
        re.I,
    )

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        soup = BeautifulSoup(html, "html.parser")
        entries: dict[int, ChapterEntry] = {}
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            title = normalize_whitespace(link.get_text(" ", strip=True))
            if not href or not title:
                continue
            chapter_number = parse_chapter_number(title)
            if chapter_number is None:
                continue
            entries[chapter_number] = ChapterEntry(chapter_number, title, urljoin(base_url, href))
        return entries

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        soup = BeautifulSoup(html, "html.parser")
        for selector in ("script", "style", "noscript"):
            for node in soup.select(selector):
                node.decompose()

        raw_title = ""
        title_node = soup.select_one("h1") or soup.select_one(".title") or soup.select_one("title")
        if title_node:
            raw_title = normalize_whitespace(title_node.get_text(" ", strip=True))
        if not raw_title:
            raw_title = fallback_title or f"第{expected_chapter_number}章"

        chapter_number = parse_chapter_number(raw_title) or expected_chapter_number

        content = ""
        for selector in (
            "#content",
            "#nr1",
            ".content",
            ".chapter-content",
            ".txtnav",
            ".read-content",
            ".articlecon",
            ".yd_text2",
            ".book-content",
        ):
            node = soup.select_one(selector)
            if not node:
                continue
            block = node.decode_contents()
            block = block.replace("<br/>", "\n").replace("<br>", "\n").replace("<br />", "\n")
            block = re.sub(r"</p>\s*<p[^>]*>", "\n", block, flags=re.I)
            block = re.sub(r"<p[^>]*>", "", block, flags=re.I)
            block = re.sub(r"</p>", "\n", block, flags=re.I)
            block = re.sub(r"<[^>]+>", "", block)
            block = normalize_whitespace(block)
            if len(block) > len(content):
                content = block

        lines: list[str] = []
        for line in content.splitlines():
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if self._NOISE_LINE_PATTERN.search(clean):
                continue
            if raw_title and clean == normalize_whitespace(raw_title):
                continue
            lines.append(clean)

        title = raw_title.strip() or f"第{chapter_number}章"
        return ParsedChapter(chapter_number=chapter_number, title=title, content="\n".join(lines))

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        soup = BeautifulSoup(html, "html.parser")
        candidates: list[tuple[str, str]] = []
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            if not href:
                continue
            text = normalize_whitespace(link.get_text(" ", strip=True))
            if not text:
                continue
            candidates.append((text, urljoin(current_url, href)))
        for text, href in candidates:
            if re.search(r"(下一页|下页|next)", text, flags=re.I):
                return href
        return None
