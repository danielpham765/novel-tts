from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class OneQxsResolver(BaseResolver):
    source_id = "1qxs"
    _TITLE_PAGE_SUFFIX_RE = re.compile(
        r"^(?P<title>.+?)\s*[（(](?P<page>(?:第)?\d+/\d+(?:页|頁)?)[）)]\s*$"
    )
    _NOISE_LINE_PATTERN = re.compile(
        r"("
        r"上一页|下一页|上一章|下一章|返回目录|加入书签|加入收藏|本章完|手机阅读"
        r"|本章未完"
        r"|点击.*继续阅读"
        r"|加\|载\|更\|多"
        r"|阅\|读\|模\|式"
        r"|畅\|读\|模\|式"
        r"|无\|法\|显\|示\|本\|章\|节\|全\|部\|内\|容"
        r"|请\|返\|回\|原\|网\|页阅\|读"
        r"|请30秒过后刷新重试"
        r"|访问太频繁了"
        r")",
        re.I,
    )

    @staticmethod
    def _extract_lines(node) -> list[str]:
        lines: list[str] = []
        for paragraph in node.select("p"):
            clean = normalize_whitespace(paragraph.get_text("\n", strip=True))
            if clean:
                lines.append(clean)

        if lines:
            return lines

        text = node.get_text("\n", strip=True)
        return [normalize_whitespace(line) for line in text.splitlines() if normalize_whitespace(line)]

    @staticmethod
    def _extract_url_page_key(url: str) -> str:
        path = urlparse(url).path.strip("/")
        segments = [segment for segment in path.split("/") if segment]
        trailing_numeric = [segment for segment in segments if segment.isdigit()]
        if len(trailing_numeric) >= 2 and int(trailing_numeric[-1]) <= 20:
            return trailing_numeric[-2]
        if trailing_numeric:
            return trailing_numeric[-1]
        return path

    @classmethod
    def _normalize_title(cls, title: str) -> str:
        clean = normalize_whitespace(title)
        match = cls._TITLE_PAGE_SUFFIX_RE.match(clean)
        if not match:
            return clean
        return match.group("title").rstrip()

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        soup = BeautifulSoup(html, "html.parser")
        entries: dict[int, ChapterEntry] = {}
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            if not href or href.startswith("#") or href.lower().startswith("javascript:"):
                continue
            title = normalize_whitespace(link.get_text(" ", strip=True))
            if not title:
                continue
            chapter_number = parse_chapter_number(title)
            if chapter_number is None:
                continue
            abs_url = urljoin(base_url, href)
            entries[chapter_number] = ChapterEntry(chapter_number, title, abs_url)
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
        raw_title = self._normalize_title(raw_title)
        chapter_number = parse_chapter_number(raw_title) or expected_chapter_number

        content_lines: list[str] = []
        for selector in (
            "#nr1",
            "#content",
            "#txt",
            ".txtnav",
            ".content",
            ".chapter-content",
        ):
            node = soup.select_one(selector)
            if not node:
                continue
            candidate_lines = self._extract_lines(node)
            if len("\n".join(candidate_lines)) > len("\n".join(content_lines)):
                content_lines = candidate_lines

        lines: list[str] = []
        normalized_raw_title = normalize_whitespace(raw_title)
        for line in content_lines:
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if self._NOISE_LINE_PATTERN.search(clean):
                continue
            if normalized_raw_title and clean == normalized_raw_title:
                continue
            lines.append(clean)

        title = self._normalize_title(raw_title).strip() or f"第{chapter_number}章"
        return ParsedChapter(chapter_number=chapter_number, title=title, content="\n".join(lines))

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        """
        1qxs can paginate a single chapter (e.g., 1/3, 2/3, 3/3).
        We follow only "next page" links within the same chapter and never hop via "next chapter".
        """
        soup = BeautifulSoup(html, "html.parser")
        current_page_key = self._extract_url_page_key(current_url)

        candidates: list[tuple[str, str]] = []
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            if not href:
                continue
            text = normalize_whitespace(link.get_text(" ", strip=True))
            if not text:
                continue
            abs_url = urljoin(current_url, href)
            candidates.append((text, abs_url))

        for text, href in candidates:
            if not re.search(r"(下一页|下页|next)", text, flags=re.I):
                continue
            if self._extract_url_page_key(href) == current_page_key:
                return href

        return None
