from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class IxdzsResolver(BaseResolver):
    source_id = "ixdzs"
    _BOOK_PATH_PATTERN = re.compile(r"/read/(\d+)/?", re.I)
    _CHAPTER_PATH_PATTERN = re.compile(r"/p(\d+)\.html", re.I)
    _NOISE_LINE_PATTERN = re.compile(
        r"(上一章|下一章|書籍頁|加入書架|看女頻小說每天能領現金紅包)",
        re.I,
    )

    @staticmethod
    def _extract_book_id(base_url: str) -> str:
        parsed = urlparse(base_url)
        match = IxdzsResolver._BOOK_PATH_PATTERN.search(parsed.path)
        return match.group(1) if match else ""

    def _extract_book_id_from_soup(self, soup: BeautifulSoup, base_url: str) -> str:
        book_id = self._extract_book_id(base_url)
        if book_id:
            return book_id

        for selector in (
            'meta[property="og:novel:read_url"]',
            'meta[property="og:url"]',
            'link[rel="alternate"][hreflang]',
        ):
            for node in soup.select(selector):
                candidate = (node.get("content") or node.get("href") or "").strip()
                match = self._BOOK_PATH_PATTERN.search(candidate)
                if match:
                    return match.group(1)

        for anchor in soup.select('a[href*="/read/"]'):
            href = (anchor.get("href") or "").strip()
            match = self._BOOK_PATH_PATTERN.search(href)
            if match:
                return match.group(1)
        return ""

    def _extract_read_base_url(self, soup: BeautifulSoup, base_url: str, book_id: str) -> str:
        if book_id:
            direct_match = self._BOOK_PATH_PATTERN.search(base_url)
            if direct_match and direct_match.group(1) == book_id:
                return base_url

        for selector in (
            'meta[property="og:novel:read_url"]',
            'meta[property="og:url"]',
            'link[rel="alternate"][hreflang]',
        ):
            for node in soup.select(selector):
                candidate = (node.get("content") or node.get("href") or "").strip()
                match = self._BOOK_PATH_PATTERN.search(candidate)
                if match and match.group(1) == book_id:
                    return candidate

        for anchor in soup.select('a[href*="/read/"]'):
            href = (anchor.get("href") or "").strip()
            match = self._BOOK_PATH_PATTERN.search(href)
            if match and match.group(1) == book_id:
                return href
        return base_url

    def _extract_latest_chapter(self, soup: BeautifulSoup) -> int:
        latest = 0
        for selector in (
            'meta[property="og:novel:latest_chapter_url"]',
            'meta[property="og:novel:latest_chapter_name"]',
            ".sub-text-r",
            ".n-text p",
            ".u-chapter a",
        ):
            for node in soup.select(selector):
                candidate = (
                    node.get("content")
                    if hasattr(node, "get")
                    else None
                ) or node.get_text(" ", strip=True)
                if not candidate:
                    continue
                match = self._CHAPTER_PATH_PATTERN.search(candidate)
                if match:
                    latest = max(latest, int(match.group(1)))
                    continue
                parsed = parse_chapter_number(candidate)
                if parsed:
                    latest = max(latest, parsed)
        return latest

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        soup = BeautifulSoup(html, "html.parser")
        book_id = self._extract_book_id_from_soup(soup, base_url)
        if not book_id:
            return {}
        read_base_url = self._extract_read_base_url(soup, base_url, book_id)

        latest = self._extract_latest_chapter(soup)
        if latest <= 0:
            return {}

        entries: dict[int, ChapterEntry] = {}
        for chapter_number in range(1, latest + 1):
            entries[chapter_number] = ChapterEntry(
                chapter_number=chapter_number,
                title=f"第{chapter_number}章",
                url=urljoin(read_base_url, f"/read/{book_id}/p{chapter_number}.html"),
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
