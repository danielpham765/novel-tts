from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class HjwzwResolver(BaseResolver):
    source_id = "hjwzw"

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
            if "/Book/Read/" not in href:
                continue
            entries[chapter_number] = ChapterEntry(chapter_number, title, urljoin(base_url, href))
        return entries

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

        content_node = None
        for node in soup.select("td > div, div"):
            text = normalize_whitespace(node.get_text("\n", strip=True))
            if len(text) < 200:
                continue
            if "請記住本站域名" not in text and f"第{chapter_number}章" not in text:
                continue
            if raw_title and raw_title not in text and f"第{chapter_number}章" not in text:
                continue
            content_node = node
            break

        content = ""
        if content_node:
            content = content_node.decode_contents()
            content = re.sub(r"<br\s*/?>", "\n", content, flags=re.I)
            content = re.sub(r"</p>\s*<p[^>]*>", "\n", content, flags=re.I)
            content = re.sub(r"<p[^>]*>", "", content, flags=re.I)
            content = re.sub(r"</p>", "\n", content, flags=re.I)
            content = re.sub(r"<[^>]+>", "", content)
            content = normalize_whitespace(content)

        lines: list[str] = []
        seen_body = False
        title_variants = {
            normalize_whitespace(raw_title),
            normalize_whitespace(raw_title.replace(" ", "")),
            normalize_whitespace(f"第{chapter_number}章"),
            normalize_whitespace(f"第{chapter_number}章".replace(" ", "")),
        }
        for line in content.splitlines():
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if re.search(
                r"(請記住本站域名|黃金屋|作者[:：]|分類[:：]|目錄|上一章|下一章|書架|投推薦票|加入書簽|加入收藏|手機用戶請訪問|如果您喜歡)",
                clean,
                flags=re.I,
            ):
                continue
            if clean in title_variants:
                continue
            if re.fullmatch(rf"第{chapter_number}章.*\(第\d+/\d+頁\)", clean):
                continue
            if re.fullmatch(rf"第{chapter_number}章.*\(\d+/\d+\)", clean):
                continue
            if not seen_body and raw_title and clean == raw_title:
                continue
            if not seen_body and clean.startswith("太虛至尊"):
                continue
            seen_body = True
            lines.append(clean)

        return ParsedChapter(
            chapter_number=chapter_number,
            title=(raw_title or f"第{chapter_number}章").strip(),
            content="\n".join(lines),
        )
