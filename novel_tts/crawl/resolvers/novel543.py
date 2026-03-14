from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, format_chapter_title, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class Novel543Resolver(BaseResolver):
    source_id = "novel543"

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        soup = BeautifulSoup(html, "html.parser")
        entries: list[ChapterEntry] = []
        for link in soup.select("a[href]"):
            href = link.get("href", "").strip()
            title = normalize_whitespace(link.get_text(" ", strip=True))
            if not href or not title:
                continue
            chapter_number = parse_chapter_number(title)
            if chapter_number is None:
                continue
            abs_url = urljoin(base_url, href)
            page_match = re.search(r"/(\d+)_\d+(?:_(\d+))?\.html", abs_url)
            page_id = int(page_match.group(1)) if page_match else None
            part = int(page_match.group(2)) if page_match and page_match.group(2) else 1
            entries.append(ChapterEntry(chapter_number, title, abs_url, page_id=page_id, part=part))
        entries.sort(key=lambda item: (item.chapter_number, item.part))
        unique: dict[int, ChapterEntry] = {}
        for entry in entries:
            if entry.chapter_number not in unique or entry.part < unique[entry.chapter_number].part:
                unique[entry.chapter_number] = entry
        return unique

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        soup = BeautifulSoup(html, "html.parser")
        for selector in ("script", "style", "noscript"):
            for node in soup.select(selector):
                node.decompose()

        title_candidates = [
            normalize_whitespace(node.get_text(" ", strip=True))
            for selector in ("h1", ".title", ".bookname h1", "title")
            for node in soup.select(selector)[:1]
        ]
        detected_title = next((item for item in title_candidates if item), fallback_title or f"Chuong {expected_chapter_number}")
        chapter_number = parse_chapter_number(detected_title) or expected_chapter_number
        title = format_chapter_title(chapter_number, detected_title)

        content = ""
        for selector in (
            "#txt",
            "#content",
            ".content",
            ".contentbox",
            ".read-content",
            ".chapter-content",
            ".txtnav",
            ".articlecon",
            ".yd_text2",
            ".book-content",
            ".txt_cont",
            ".novelcontent",
        ):
            node = soup.select_one(selector)
            if not node:
                continue
            block = normalize_whitespace(
                node.decode_contents()
                .replace("<br/>", "\n")
                .replace("<br>", "\n")
                .replace("<br />", "\n")
            )
            block = re.sub(r"<[^>]+>", "", block)
            if len(block) > len(content):
                content = block

        lines = []
        title_no_suffix = normalize_whitespace(re.sub(r"\s*\(\d+/\d+\)\s*$", "", title))
        for line in content.splitlines():
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if clean == title_no_suffix:
                continue
            if re.fullmatch(r"(?:trang chủ|mục lục|trước chương|sau chương|上一章|下一章|返回目录)", clean, flags=re.I):
                continue
            lines.append(clean)
        return ParsedChapter(chapter_number=chapter_number, title=title, content="\n".join(lines))

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        current_match = re.search(r"/(\d+)_\d+(?:_(\d+))?\.html", current_url)
        if not current_match:
            return None
        current_page_id = int(current_match.group(1))
        current_part = int(current_match.group(2) or 1)
        soup = BeautifulSoup(html, "html.parser")
        candidates: list[tuple[int, str, str]] = []
        for link in soup.select("a[href]"):
            href = urljoin(current_url, link.get("href", ""))
            match = re.search(r"/(\d+)_\d+(?:_(\d+))?\.html", href)
            if not match:
                continue
            if int(match.group(1)) != current_page_id:
                continue
            part = int(match.group(2) or 1)
            if part <= current_part:
                continue
            text = normalize_whitespace(link.get_text(" ", strip=True))
            candidates.append((part, href, text))
        candidates.sort(key=lambda item: item[0])
        for part, href, text in candidates:
            if re.search(r"(next|tiep|trang sau|下一页|下页)", text, flags=re.I):
                return href
        for part, href, _ in candidates:
            if part == current_part + 1:
                return href
        return None
