from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ParsedChapter


class OneQxsResolver(BaseResolver):
    source_id = "1qxs"

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
            block = node.decode_contents()
            block = re.sub(r"<font[^>]*>|</font>", "", block, flags=re.I)
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
            if re.search(r"(上一页|下一页|上一章|下一章|返回目录|加入书签|加入收藏|本章完)", clean):
                continue
            if raw_title and clean == normalize_whitespace(raw_title):
                continue
            lines.append(clean)

        title = raw_title.strip() or f"第{chapter_number}章"
        return ParsedChapter(chapter_number=chapter_number, title=title, content="\n".join(lines))

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        """
        1qxs can paginate a single chapter (e.g., 1/3, 2/3, 3/3).
        We follow "next page" links, but avoid hopping to the next chapter when possible.
        """
        soup = BeautifulSoup(html, "html.parser")
        current_key = re.sub(r"[?#].*$", "", current_url)
        current_base = re.sub(r"(_\d+)?(\.html)?$", "", current_key)

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
            if not re.search(r"(下一页|下页|next|下一章)", text, flags=re.I):
                continue
            href_key = re.sub(r"[?#].*$", "", href)
            href_base = re.sub(r"(_\d+)?(\.html)?$", "", href_key)
            # Prefer same-base pagination URLs (page 2/3, 3/3).
            if href_base == current_base:
                return href

        # Fallback: any "next page" link.
        for text, href in candidates:
            if re.search(r"(下一页|下页|next)", text, flags=re.I):
                return href
        return None

