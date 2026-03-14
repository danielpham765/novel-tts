from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class Shuba69Resolver(BaseResolver):
    source_id = "69shuba"

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

    def find_directory_page_urls(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        page_urls: set[str] = set()
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            if not href:
                continue
            abs_url = urljoin(base_url, href)
            if re.search(r"/indexlist/\d+/\d+/?$", abs_url):
                page_urls.add(abs_url)
        return sorted(page_urls)

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        soup = BeautifulSoup(html, "html.parser")
        raw_title = ""
        title_node = soup.select_one("h1")
        if title_node:
            raw_title = normalize_whitespace(title_node.get_text(" ", strip=True))
        if not raw_title:
            title_node = soup.select_one("title")
            raw_title = normalize_whitespace(title_node.get_text(" ", strip=True)) if title_node else fallback_title
        chapter_number = parse_chapter_number(raw_title) or expected_chapter_number

        content_node = (
            soup.select_one("#nr1")
            or soup.select_one(".txtnav")
            or soup.select_one("#content")
        )
        content = ""
        if content_node:
            content = content_node.decode_contents()
            content = re.sub(r"<font[^>]*>|</font>", "", content, flags=re.I)
            content = content.replace("<br/>", "\n").replace("<br>", "\n").replace("<br />", "\n")
            content = re.sub(r"</p>\s*<p>", "\n", content, flags=re.I)
            content = re.sub(r"<[^>]+>", "", content)
            content = normalize_whitespace(content)
        lines = []
        for line in content.splitlines():
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if re.match(r"^loadAdv\(", clean, flags=re.I):
                continue
            if re.search(
                r"(window\.pubfuturetag|^作者[:：]|\d{4}-\d{2}-\d{2}|本章完|翻过去|后面有涩图|求追读|求收藏|上一章|下一章|返回目錄|首頁|書架|加入書籤)",
                clean,
                flags=re.I,
            ):
                continue
            if raw_title and raw_title in clean:
                continue
            lines.append(clean)
        return ParsedChapter(
            chapter_number=chapter_number,
            title=(raw_title or f"第{chapter_number}章").strip(),
            content="\n".join(lines),
        )
