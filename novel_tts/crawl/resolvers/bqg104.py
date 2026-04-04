from __future__ import annotations

import json
import re
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from urllib.request import urlopen

from novel_tts.common.text import normalize_whitespace

from ..base import BaseResolver, parse_chapter_number
from ..types import ChapterEntry, ParsedChapter


class Bqg104Resolver(BaseResolver):
    source_id = "bqg104"
    _NOISE_LINE_PATTERN = re.compile(
        r"(上一章|下一章|返回目录|加入书签|加入收藏|本章完|手机阅读|举报错误章节)",
        re.I,
    )

    @staticmethod
    def _extract_book_id(url: str) -> str:
        parsed = urlparse(url)
        query_id = (parse_qs(parsed.query).get("id") or [""])[0].strip()
        if query_id:
            return query_id
        match = re.search(r"/#/book/(\d+)/?", url)
        if match:
            return match.group(1)
        match = re.search(r"/book/(\d+)(?:/|$)", parsed.path)
        if match:
            return match.group(1)
        return ""

    @classmethod
    def _chapter_api_url(cls, base_url: str, book_id: str, chapter_number: int) -> str:
        parsed = urlparse(base_url)
        api_base = f"{parsed.scheme}://{parsed.netloc}/api/chapter"
        return f"{api_base}?id={book_id}&chapterid={chapter_number}"

    @classmethod
    def _book_api_url(cls, base_url: str, book_id: str) -> str:
        parsed = urlparse(base_url)
        api_base = f"{parsed.scheme}://{parsed.netloc}/api/book"
        return f"{api_base}?id={book_id}"

    @classmethod
    def _booklist_api_url(cls, base_url: str, book_id: str) -> str:
        parsed = urlparse(base_url)
        api_base = f"{parsed.scheme}://{parsed.netloc}/api/booklist"
        return f"{api_base}?id={book_id}"

    @staticmethod
    def _load_json_url(url: str) -> dict[str, object]:
        with urlopen(url, timeout=30) as response:
            payload = response.read().decode("utf-8")
        return json.loads(payload)

    @classmethod
    def _coerce_directory_payload(cls, html: str, base_url: str, book_id: str) -> dict[str, object]:
        stripped = (html or "").lstrip()
        if stripped.startswith("{"):
            return json.loads(html)
        if not book_id:
            return {}
        return cls._load_json_url(cls._booklist_api_url(base_url, book_id))

    @classmethod
    def _probe_chapter_number(cls, base_url: str, book_id: str, chapter_id: int) -> int | None:
        try:
            payload = cls._load_json_url(cls._chapter_api_url(base_url, book_id, chapter_id))
        except Exception:
            return None
        return parse_chapter_number(normalize_whitespace(str(payload.get("chaptername") or "")))

    @classmethod
    def _find_diff_segments(
        cls,
        *,
        base_url: str,
        book_id: str,
        last_chapter_id: int,
    ) -> list[tuple[int, int]]:
        start_actual = cls._probe_chapter_number(base_url, book_id, 1)
        end_actual = cls._probe_chapter_number(base_url, book_id, last_chapter_id)
        if start_actual is None or end_actual is None:
            return []

        segments: list[tuple[int, int]] = []

        def diff_at(chapter_id: int) -> int | None:
            actual = cls._probe_chapter_number(base_url, book_id, chapter_id)
            if actual is None:
                return None
            return chapter_id - actual

        def locate_changes(low_id: int, high_id: int) -> None:
            low_diff = diff_at(low_id)
            high_diff = diff_at(high_id)
            if low_diff is None or high_diff is None or low_diff == high_diff or low_id >= high_id:
                return

            left = low_id + 1
            right = high_id
            boundary_id = high_id
            while left <= right:
                mid = (left + right) // 2
                mid_diff = diff_at(mid)
                if mid_diff is None:
                    left = mid + 1
                    continue
                if mid_diff == low_diff:
                    left = mid + 1
                else:
                    boundary_id = mid
                    right = mid - 1

            boundary_actual = cls._probe_chapter_number(base_url, book_id, boundary_id)
            boundary_diff = diff_at(boundary_id)
            if boundary_actual is None or boundary_diff is None:
                return
            segments.append((boundary_actual, boundary_diff))
            locate_changes(boundary_id, high_id)

        locate_changes(1, last_chapter_id)
        segments.sort(key=lambda item: item[0])
        return segments

    @staticmethod
    def _chapter_id_for_number(chapter_number: int, diff_segments: list[tuple[int, int]]) -> int:
        diff = 0
        for start_chapter_number, segment_diff in diff_segments:
            if chapter_number >= start_chapter_number:
                diff = segment_diff
            else:
                break
        return int(chapter_number) + diff

    def parse_directory(self, html: str, base_url: str) -> dict[int, ChapterEntry]:
        book_id = self._extract_book_id(base_url)
        payload = self._coerce_directory_payload(html, base_url, book_id)
        titles = payload.get("list", [])
        if not isinstance(titles, list):
            return {}

        diff_segments: list[tuple[int, int]] = []
        if book_id:
            try:
                book_payload = self._load_json_url(self._book_api_url(base_url, book_id))
            except Exception:
                book_payload = {}
            last_chapter_id = int(str(book_payload.get("lastchapterid") or len(titles) or 0) or 0)
            diff_segments = self._find_diff_segments(
                base_url=base_url,
                book_id=book_id,
                last_chapter_id=last_chapter_id,
            )

        entries: dict[int, ChapterEntry] = {}
        for index, raw_title in enumerate(titles, start=1):
            title = normalize_whitespace(str(raw_title or ""))
            chapter_number = parse_chapter_number(title) or index
            if not book_id:
                continue
            chapter_id = self._chapter_id_for_number(chapter_number, diff_segments)
            entries[chapter_number] = ChapterEntry(
                chapter_number=chapter_number,
                title=title or f"第{chapter_number}章",
                url=self._chapter_api_url(base_url, book_id, chapter_id),
                metadata={"chapter_id": chapter_id},
            )
        return entries

    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        payload = json.loads(html)
        raw_title = normalize_whitespace(str(payload.get("chaptername") or fallback_title or ""))
        chapter_number = parse_chapter_number(raw_title) or expected_chapter_number
        content = normalize_whitespace(str(payload.get("txt") or ""))

        lines: list[str] = []
        for line in content.splitlines():
            clean = normalize_whitespace(line)
            if not clean:
                continue
            if self._NOISE_LINE_PATTERN.search(clean):
                continue
            if raw_title and clean == raw_title:
                continue
            lines.append(clean)

        title = raw_title or f"第{chapter_number}章"
        return ParsedChapter(chapter_number=chapter_number, title=title, content="\n".join(lines))

    def correct_chapter_url(self, current_url: str, expected_chapter_number: int, actual_chapter_number: int) -> str | None:
        delta = int(expected_chapter_number) - int(actual_chapter_number)
        if delta == 0:
            return None

        parsed = urlparse(current_url)
        query = parse_qs(parsed.query)
        current_ids = query.get("chapterid") or []
        if not current_ids:
            return None
        try:
            current_chapter_id = int(current_ids[0])
        except (TypeError, ValueError):
            return None

        corrected_chapter_id = current_chapter_id + delta
        if corrected_chapter_id <= 0 or corrected_chapter_id == current_chapter_id:
            return None
        query["chapterid"] = [str(corrected_chapter_id)]
        corrected_query = urlencode(query, doseq=True)
        return urlunparse(parsed._replace(query=corrected_query))
