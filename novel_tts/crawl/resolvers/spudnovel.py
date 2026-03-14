from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class SpudNovelResolver(BaseResolver):
    source_id = "spudnovel"

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        soup = BeautifulSoup(html, "html.parser")
        entries: dict[int, ChapterEntry] = {}
        for link in soup.select("a[href*='/site/chapter?id=']"):
            href = link.get("href", "").strip()
            title = normalize_whitespace(link.get_text(" ", strip=True))
            if not href or not title:
                continue
            chapter_number = parse_chapter_number(title)
            if chapter_number is None:
                continue
            abs_url = urljoin(base_url, href)
            match = re.search(r"[?&]id=(\d+)", abs_url)
            chapter_id = int(match.group(1)) if match else None
            entries[chapter_number] = ChapterEntry(
                chapter_number=chapter_number,
                title=title,
                url=abs_url,
                metadata={"chapter_id": chapter_id},
            )
        return dict(sorted(entries.items(), key=lambda item: item[0]))

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        soup = BeautifulSoup(html, "html.parser")
        for selector in ("script", "style", "noscript", "iframe"):
            for node in soup.select(selector):
                node.decompose()

        title_candidates = [
            normalize_whitespace(node.get_text(" ", strip=True))
            for selector in ("h1", ".title", ".chapter-title", ".article-title", ".headline", "title")
            for node in soup.select(selector)[:1]
        ]
        detected_title = next((item for item in title_candidates if item), fallback_title or f"第{expected_chapter_number}章")
        chapter_number = parse_chapter_number(detected_title) or expected_chapter_number

        content = ""
        for selector in (
            ".read-content",
            ".content",
            "#content",
            ".chapter-content",
            ".article-content",
            ".page-content",
            "article",
            ".panel-body",
            ".txtnav",
        ):
            node = soup.select_one(selector)
            if not node:
                continue
            block = node.decode_contents()
            block = block.replace("<br/>", "\n").replace("<br>", "\n").replace("<br />", "\n")
            block = re.sub(r"</p>\s*<p[^>]*>", "\n\n", block, flags=re.I)
            block = re.sub(r"<[^>]+>", "", block)
            block = normalize_whitespace(block)
            if len(block) > len(content):
                content = block

        for marker in (
            "📖 土豆小说网统计已有",
            "读完本章，你的感受是？",
            "| 目录 |",
            "\n目录\n",
            "\n下一章\n",
        ):
            if marker in content:
                content = content.split(marker, 1)[0].strip()

        next_chapter_pattern = re.compile(rf"(?:^|\n)\s*第{expected_chapter_number + 1}章[^\n]*", flags=re.M)
        next_match = next_chapter_pattern.search(content)
        if next_match:
            content = content[: next_match.start()].strip()

        lines: list[str] = []
        normalized_title = normalize_whitespace(re.sub(r"\s*\(\d+/\d+\)\s*$", "", detected_title))
        for line in content.splitlines():
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if normalized_title and clean == normalized_title:
                continue
            if clean in {"小", "中", "大"}:
                continue
            if re.fullmatch(r"[|｜]+", clean):
                continue
            if re.match(r"^(小说名：|更新时间：|作者：|章节字数：)", clean):
                continue
            if re.search(r"(上一章|下一章|返回目录|章节报错|加入书签|推荐票|上一页|下一页)", clean):
                continue
            lines.append(clean)

        return ParsedChapter(
            chapter_number=chapter_number,
            title=normalized_title or f"第{chapter_number}章",
            content="\n".join(lines),
        )
