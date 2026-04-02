from __future__ import annotations

import json
import re
import time
import unicodedata
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.common.text import normalize_whitespace
from novel_tts.config.models import NovelConfig, SourceConfig

from .registry import build_default_registry
from .repair_config import (
    RepairCandidate,
    ChapterRepairRule,
    RepairConfig,
    generate_repair_config_from_research,
    load_repair_config,
    repair_config_path,
    save_repair_config,
)
from .strategies import CrawlProxySessionState, build_strategy_chain
from .types import ChapterEntry

LOGGER = get_logger(__name__)
FAILURE_MANIFEST_NAME = "crawl_failures.json"
INVALID_TITLE_TOKENS = (
    "error 1015",
    "access denied",
    "just a moment",
)
INVALID_CONTENT_TOKENS = (
    "you are being rate limited",
    "banned you temporarily",
    "used cloudflare to restrict access",
    "verify you are human",
    "performing security verification",
)
WATERMARK_CONTENT_PATTERNS = (
    re.compile(r"记住本站域名", re.I),
    re.compile(r"記住本站域名", re.I),
    re.compile(r"本站域名", re.I),
    re.compile(r"小说免费阅读", re.I),
    re.compile(r"请收藏", re.I),
    re.compile(r"一七小说", re.I),
    re.compile(r"1qxs(?:\.com)?", re.I),
    re.compile(r"章节报错\s*分享给朋友", re.I),
    re.compile(r"章節報錯\s*分享給朋友", re.I),
    re.compile(r"速讀谷", re.I),
    re.compile(r"更新不易.*記得分享", re.I),
    re.compile(r"追台灣小說就上台灣小說網", re.I),
    re.compile(r"台灣小說網", re.I),
    re.compile(r"google搜索\s*twkan", re.I),
    re.compile(r"twkan\.com", re.I),
    re.compile(r"twkan", re.I),
)
METADATA_NOISE_LINE_PATTERNS = (
    re.compile(r"^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?$", re.I),
    re.compile(r"^作者[:：]\s*.+$", re.I),
    re.compile(r"^更新时间[:：]\s*.+$", re.I),
)
METADATA_NOISE_SCAN_LINES = 6
MIN_CONTENT_CHARS = 80
BATCH_FILENAME_PATTERN = re.compile(r"chuong_(\d+)-(\d+)\.txt$")
MIN_DUPLICATE_BLOCK_CHARS = 160
MIN_DUPLICATE_ADJACENT_BLOCK_CHARS = 120
MIN_DUPLICATE_LINE_CHARS = 60
MIN_DUPLICATE_LINE_SEQUENCE_CHARS = 240
MIN_DUPLICATE_LINE_SEQUENCE_LINES = 3
DUPLICATE_REPEATED_RATIO_THRESHOLD = 0.18
PAGINATED_TITLE_SUFFIX_RE = re.compile(
    r"^(?P<title>.+?)\s*[（(](?P<page>(?:第)?\d+/\d+(?:页|頁)?)[）)]\s*$"
)


@dataclass
class CrawlVerifyIssue:
    code: str
    message: str
    chapter_number: int | None = None
    path: Path | None = None


@dataclass
class CrawlVerifyReport:
    checked_files: list[Path]
    checked_chapters: list[int]
    issues: list[CrawlVerifyIssue]
    stale_failures_removed: int = 0
    failure_manifest_deleted: bool = False

    @property
    def ok(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class RepairAction:
    action: str
    chapter: int
    reason: str
    target_file: str
    timestamp: str
    from_source_id: str = ""
    from_url: str = ""


@dataclass
class RepairReport:
    novel_id: str
    from_chapter: int
    to_chapter: int
    executed_at: str
    actions: list[RepairAction]
    log_path: Path
    modified_files: list[Path]


@dataclass
class SourceDiscoveryResult:
    source_config: SourceConfig
    entries: dict[int, ChapterEntry]
    latest_chapter: int


def _has_requested_chapters(entries: dict[int, ChapterEntry], from_chapter: int, to_chapter: int) -> bool:
    return all(chapter_number in entries for chapter_number in range(from_chapter, to_chapter + 1))


def _failure_manifest_path(config: NovelConfig) -> Path:
    return config.storage.progress_dir / FAILURE_MANIFEST_NAME


def _load_failure_manifest(config: NovelConfig) -> dict[str, object]:
    path = _failure_manifest_path(config)
    if not path.exists():
        return {
            "novel_id": config.novel_id,
            "source": config.source_id,
            "updated_at": "",
            "failures": {},
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _save_failure_manifest(config: NovelConfig, manifest: dict[str, object]) -> None:
    path = _failure_manifest_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _record_failure(
    config: NovelConfig,
    manifest: dict[str, object],
    *,
    chapter_number: int,
    batch_start: int,
    batch_end: int,
    url: str,
    reason: str,
    details: str,
) -> None:
    failures = manifest.setdefault("failures", {})
    failures[str(chapter_number)] = {
        "chapter_number": chapter_number,
        "batch_start": batch_start,
        "batch_end": batch_end,
        "url": url,
        "reason": reason,
        "details": details,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_failure_manifest(config, manifest)


def _clear_failure(config: NovelConfig, manifest: dict[str, object], chapter_number: int) -> None:
    failures = manifest.setdefault("failures", {})
    if str(chapter_number) in failures:
        del failures[str(chapter_number)]
        _save_failure_manifest(config, manifest)


def _validate_chapter_content(title: str, content: str) -> str | None:
    normalized_title = normalize_whitespace(title).lower()
    normalized_content = normalize_whitespace(content).lower()
    if any(token in normalized_title for token in INVALID_TITLE_TOKENS):
        return f"invalid_title:{normalized_title}"
    # Allow shorter placeholder chapters for index gaps.
    if not re.search(r"(略过|略過)", normalized_title, flags=re.I):
        if len(normalized_content) < MIN_CONTENT_CHARS:
            return f"content_too_short:{len(normalized_content)}"
    for token in INVALID_CONTENT_TOKENS:
        if token in normalized_content:
            return f"invalid_content_token:{token}"
    return None


def _normalize_duplicate_token(value: str) -> str:
    value = normalize_whitespace(value)
    value = re.sub(r"[ ]{2,}", " ", value)
    return value


def _normalize_detection_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalize_whitespace(normalized).casefold()
    return normalized


def _detect_watermark_content(content: str) -> str | None:
    normalized_content = _normalize_detection_text(content)
    if not normalized_content:
        return None
    for pattern in WATERMARK_CONTENT_PATTERNS:
        if pattern.search(normalized_content):
            return pattern.pattern
    for line in content.splitlines():
        normalized_line = _normalize_detection_text(line)
        if not normalized_line:
            continue
        for pattern in WATERMARK_CONTENT_PATTERNS:
            if pattern.search(normalized_line):
                return pattern.pattern
    return None


def _is_watermark_line(line: str) -> bool:
    return _detect_watermark_content(line) is not None


def _detect_metadata_noise_content(content: str) -> str | None:
    non_empty_seen = 0
    for line in content.splitlines():
        clean = normalize_whitespace(line)
        if not clean:
            continue
        non_empty_seen += 1
        if non_empty_seen > METADATA_NOISE_SCAN_LINES:
            break
        for pattern in METADATA_NOISE_LINE_PATTERNS:
            if pattern.search(clean):
                return pattern.pattern
    return None


def _is_metadata_noise_line(line: str) -> bool:
    clean = normalize_whitespace(line)
    if not clean:
        return False
    for pattern in METADATA_NOISE_LINE_PATTERNS:
        if pattern.search(clean):
            return True
    return False


def _strip_paginated_title_suffix(title: str) -> str:
    normalized_title = normalize_whitespace(title)
    match = PAGINATED_TITLE_SUFFIX_RE.match(normalized_title)
    if not match:
        return normalized_title
    return match.group("title").rstrip()


def _has_paginated_title_suffix(title: str) -> bool:
    normalized_title = normalize_whitespace(title)
    return PAGINATED_TITLE_SUFFIX_RE.match(normalized_title) is not None


def _normalize_title_for_compare(title: str) -> str:
    return normalize_whitespace(_strip_paginated_title_suffix(title))


def _canonicalize_chapter_block(block: str) -> str:
    stripped_block = block.strip("\n")
    if not stripped_block:
        return ""

    header, sep, body = stripped_block.partition("\n")
    cleaned_header = _strip_paginated_title_suffix(header).strip()
    body_lines = [line.rstrip() for line in body.splitlines()] if sep else []

    # Drop duplicated title lines at the start of the body, including paginated variants.
    normalized_header = _normalize_title_for_compare(cleaned_header)
    while body_lines and not normalize_whitespace(body_lines[0]):
        body_lines.pop(0)
    while body_lines:
        first_line = body_lines[0]
        if _normalize_title_for_compare(first_line) != normalized_header:
            break
        body_lines.pop(0)
        while body_lines and not normalize_whitespace(body_lines[0]):
            body_lines.pop(0)

    cleaned_body = "\n".join(body_lines).strip()
    return cleaned_header + (f"\n\n{cleaned_body}" if cleaned_body else "")


def _detect_duplicated_content(content: str) -> str | None:
    """
    Heuristic duplicate detection for buggy sources that repeat paragraphs/blocks inside a chapter.

    - Flags adjacent duplicated blocks (separated by blank lines).
    - Flags non-adjacent duplicated blocks if repeated content ratio is high.
    - Flags consecutive duplicated long lines.
    """
    raw = content.strip()
    if not raw:
        return None

    blocks = [
        "\n".join(_normalize_duplicate_token(line) for line in block.splitlines() if _normalize_duplicate_token(line))
        for block in re.split(r"\n{2,}", raw)
    ]
    blocks = [block for block in blocks if block]
    if len(blocks) < 2:
        blocks = []

    adjacent_runs = 0
    if blocks:
        for idx in range(len(blocks) - 1):
            if blocks[idx] == blocks[idx + 1] and len(blocks[idx]) >= MIN_DUPLICATE_ADJACENT_BLOCK_CHARS:
                adjacent_runs += 1

    big_blocks = [block for block in blocks if len(block) >= MIN_DUPLICATE_BLOCK_CHARS]
    repeated_ratio = 0.0
    repeated_blocks = 0
    if big_blocks:
        counts = Counter(big_blocks)
        repeated = {block: count for block, count in counts.items() if count >= 2}
        repeated_blocks = len(repeated)
        repeated_chars = sum(len(block) * (count - 1) for block, count in repeated.items())
        repeated_ratio = repeated_chars / max(1, len(raw))

    consecutive_line_dupes = 0
    lines = [_normalize_duplicate_token(line) for line in raw.splitlines()]
    last = None
    run = 0
    for line in lines:
        if not line or len(line) < MIN_DUPLICATE_LINE_CHARS:
            last = None
            run = 0
            continue
        if line == last:
            run += 1
        else:
            last = line
            run = 0
        if run >= 1:
            consecutive_line_dupes += 1

    repeated_line_counts = Counter(
        line for line in lines if line and len(line) >= MIN_DUPLICATE_LINE_CHARS
    )
    repeated_line_chars = sum(len(line) * (count - 1) for line, count in repeated_line_counts.items() if count >= 2)
    repeated_line_items = sum(1 for count in repeated_line_counts.values() if count >= 2)
    repeated_line_ratio = repeated_line_chars / max(1, len(raw))

    longest_seq_lines = 0
    longest_seq_chars = 0
    eligible_lines = [line if len(line) >= MIN_DUPLICATE_LINE_CHARS else "" for line in lines]
    for left in range(len(eligible_lines)):
        if not eligible_lines[left]:
            continue
        for right in range(left + 1, len(eligible_lines)):
            if eligible_lines[left] != eligible_lines[right]:
                continue
            seq_lines = 0
            seq_chars = 0
            cursor = 0
            while (
                left + cursor < len(eligible_lines)
                and right + cursor < len(eligible_lines)
                and eligible_lines[left + cursor]
                and eligible_lines[left + cursor] == eligible_lines[right + cursor]
            ):
                seq_lines += 1
                seq_chars += len(eligible_lines[left + cursor])
                cursor += 1
            if (
                seq_lines > longest_seq_lines
                or (seq_lines == longest_seq_lines and seq_chars > longest_seq_chars)
            ):
                longest_seq_lines = seq_lines
                longest_seq_chars = seq_chars

    if adjacent_runs:
        return f"adjacent_duplicate_blocks={adjacent_runs}"
    if repeated_blocks and repeated_ratio >= DUPLICATE_REPEATED_RATIO_THRESHOLD:
        return f"repeated_blocks={repeated_blocks} repeated_ratio={repeated_ratio:.0%}"
    if (
        repeated_line_items >= MIN_DUPLICATE_LINE_SEQUENCE_LINES
        and repeated_line_ratio >= DUPLICATE_REPEATED_RATIO_THRESHOLD
    ):
        return f"repeated_lines={repeated_line_items} repeated_ratio={repeated_line_ratio:.0%}"
    if (
        longest_seq_lines >= MIN_DUPLICATE_LINE_SEQUENCE_LINES
        and longest_seq_chars >= MIN_DUPLICATE_LINE_SEQUENCE_CHARS
    ):
        return f"repeated_line_sequence lines={longest_seq_lines} chars={longest_seq_chars}"
    if consecutive_line_dupes:
        return f"consecutive_duplicate_lines={consecutive_line_dupes}"
    return None


def _write_batch(origin_dir: Path, start_chapter: int, end_chapter: int, blocks: list[str]) -> Path:
    origin_dir.mkdir(parents=True, exist_ok=True)
    output_path = origin_dir / f"chuong_{start_chapter}-{end_chapter}.txt"
    output_path.write_text("\n\n\n".join(blocks).strip() + "\n", encoding="utf-8")
    return output_path


def _write_merged_batch(
    origin_dir: Path,
    start_chapter: int,
    end_chapter: int,
    blocks: list[str],
    chapter_numbers: list[int],
    chapter_regex: str,
) -> Path:
    """
    Write a batch file while preserving already-crawled chapters from overlapping batch files.

    This keeps partial historical batches from splitting one logical batch into multiple files
    after incremental crawls.
    """
    origin_dir.mkdir(parents=True, exist_ok=True)
    output_path = origin_dir / f"chuong_{start_chapter}-{end_chapter}.txt"

    merged_blocks: dict[int, str] = {}
    overlapping_sources: list[Path] = []
    for path in _iter_origin_batch_files(origin_dir):
        if not _batch_file_overlaps_range(path, start_chapter, end_chapter):
            continue
        overlapping_sources.append(path)
        raw = path.read_text(encoding="utf-8")
        for chapter_number, title, body in _split_crawled_chapters(raw, chapter_regex):
            merged_blocks.setdefault(chapter_number, f"{title}\n\n{body}".strip())

    for chapter_number, block in zip(chapter_numbers, blocks):
        merged_blocks[chapter_number] = block.strip()

    merged = [merged_blocks[number].strip() for number in sorted(merged_blocks)]
    output_path.write_text("\n\n\n".join(merged).strip() + "\n", encoding="utf-8")

    for source_path in overlapping_sources:
        if source_path == output_path:
            continue
        if source_path.exists():
            source_path.unlink()

    return output_path


def _iter_origin_batch_files(origin_dir: Path) -> list[Path]:
    if not origin_dir.exists():
        return []
    files = [path for path in origin_dir.glob("chuong_*.txt") if path.is_file()]
    return sorted(
        files,
        key=lambda path: (
            int(BATCH_FILENAME_PATTERN.match(path.name).group(1)) if BATCH_FILENAME_PATTERN.match(path.name) else 10**12,
            path.name,
        ),
    )


def _existing_origin_chapter_numbers(
    *,
    origin_dir: Path,
    chapter_regex: str,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
) -> set[int]:
    existing: set[int] = set()
    for path in _iter_origin_batch_files(origin_dir):
        if not _batch_file_overlaps_range(path, from_chapter, to_chapter):
            continue
        raw = path.read_text(encoding="utf-8")
        for chapter_number, _title, _body in _split_crawled_chapters(raw, chapter_regex):
            if from_chapter is not None and chapter_number < from_chapter:
                continue
            if to_chapter is not None and chapter_number > to_chapter:
                continue
            existing.add(chapter_number)
    return existing


def _batch_file_overlaps_range(path: Path, from_chapter: int | None, to_chapter: int | None) -> bool:
    if from_chapter is None and to_chapter is None:
        return True
    match = BATCH_FILENAME_PATTERN.match(path.name)
    if not match:
        return True
    batch_start = int(match.group(1))
    batch_end = int(match.group(2))
    if from_chapter is not None and batch_end < from_chapter:
        return False
    if to_chapter is not None and batch_start > to_chapter:
        return False
    return True


def _split_crawled_chapters(raw: str, chapter_regex: str) -> list[tuple[int, str, str]]:
    matches = list(re.finditer(chapter_regex, raw, flags=re.M))
    if not matches:
        return []

    chapters: list[tuple[int, str, str]] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        block = raw[start:end].strip("\n")
        if not block:
            continue
        header, _, remainder = block.partition("\n")
        chapter_number = int(match.group(1))
        chapters.append((chapter_number, header.strip(), remainder.strip()))
    return chapters


def _default_repair_log_path(config: NovelConfig) -> Path:
    return config.storage.logs_dir / config.novel_id / "crawl" / "addition-replacement_chapter_list.txt"


def _placeholder_block(chapter_number: int, cfg: RepairConfig) -> str:
    title_suffix = (cfg.placeholder_title_suffix or "略过").strip() or "略过"
    title = f"第{chapter_number}章 {title_suffix}"
    content = (cfg.placeholder_content_zh or "").strip()
    if not content:
        content = "本章内容与主线剧情无关。"
    return f"{title}\n\n{content}\n"


def _insert_placeholders_into_batch(
    *,
    path: Path,
    missing_chapters: list[int],
    chapter_regex: str,
    cfg: RepairConfig,
) -> tuple[bool, list[tuple[int, str]]]:
    """
    Inserts placeholder chapters into a single origin batch file.

    Returns:
      - changed: whether the file was modified
      - inserted: list of (chapter_number, reason)
    """
    if not missing_chapters:
        return False, []

    raw = path.read_text(encoding="utf-8")
    matches = list(re.finditer(chapter_regex, raw, flags=re.M))
    if not matches:
        return False, []

    # Build a stable mapping from chapter number -> start index in file.
    chapter_starts: dict[int, int] = {}
    for match in matches:
        try:
            chapter_number = int(match.group(1))
        except Exception:
            continue
        # Keep the first occurrence if duplicates exist; verify already flags duplicates separately.
        chapter_starts.setdefault(chapter_number, match.start())

    if not chapter_starts:
        return False, []

    inserted: list[tuple[int, str]] = []
    changed = False

    # Insert in ascending order to keep predictable output.
    # Adjust offsets by re-parsing starts after each insertion (simple and robust for small batches).
    for missing in sorted(set(missing_chapters)):
        raw = path.read_text(encoding="utf-8") if changed else raw
        matches = list(re.finditer(chapter_regex, raw, flags=re.M))
        chapter_starts = {}
        for match in matches:
            try:
                chapter_number = int(match.group(1))
            except Exception:
                continue
            chapter_starts.setdefault(chapter_number, match.start())

        if missing in chapter_starts:
            continue

        next_candidates = [start for num, start in chapter_starts.items() if num > missing]
        insert_at = min(next_candidates) if next_candidates else len(raw)

        block = _placeholder_block(missing, cfg).strip() + "\n"
        prefix = raw[:insert_at].rstrip("\n")
        suffix = raw[insert_at:].lstrip("\n")
        raw = f"{prefix}\n\n\n{block}\n\n\n{suffix}".strip() + "\n"
        changed = True
        inserted.append((missing, "missing_entry"))

    if changed:
        path.write_text(raw, encoding="utf-8")
    return changed, inserted


def _write_repair_report(report: RepairReport) -> None:
    report.log_path.parent.mkdir(parents=True, exist_ok=True)

    placeholders = sorted((a for a in report.actions if a.action == "placeholder_added"), key=lambda a: a.chapter)
    replaced = sorted(
        (a for a in report.actions if a.action in {"replaced_from_source", "fallback_replaced"}), key=lambda a: a.chapter
    )
    notes = sorted((a for a in report.actions if a.action == "dedup_applied"), key=lambda a: a.chapter)

    lines: list[str] = []
    lines.append(f"novel_id: {report.novel_id}")
    lines.append(f"range: {report.from_chapter}-{report.to_chapter}")
    lines.append(f"executed_at: {report.executed_at}")
    lines.append(f"actions: {len(report.actions)}")
    lines.append("")

    lines.append("## Added placeholders")
    if not placeholders:
        lines.append("(none)")
    else:
        for action in placeholders:
            lines.append(
                f"- chapter {action.chapter} | reason={action.reason} | file={action.target_file} | at={action.timestamp}"
            )
    lines.append("")

    lines.append("## Replaced chapters")
    if not replaced:
        lines.append("(none)")
    else:
        for action in replaced:
            src = action.from_source_id or "unknown"
            url = action.from_url or ""
            tail = f" | url={url}" if url else ""
            lines.append(
                f"- chapter {action.chapter} | source={src}{tail} | reason={action.reason} | file={action.target_file} | at={action.timestamp}"
            )
    lines.append("")

    lines.append("## Notes")
    if not notes:
        lines.append("(none)")
    else:
        for action in notes:
            lines.append(f"- chapter {action.chapter} | {action.action} | file={action.target_file} | at={action.timestamp}")
    lines.append("")

    report.log_path.write_text("\n".join(lines), encoding="utf-8")


def _load_source_config_from_repo(root: Path, source_id: str) -> dict[str, object]:
    path = root / "configs" / "sources" / f"{source_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Source config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _dedupe_adjacent_blocks(content: str) -> tuple[str, bool]:
    raw = content.strip()
    if not raw:
        return content, False
    blocks = [block.strip() for block in re.split(r"\n{2,}", raw) if block.strip()]
    if len(blocks) < 2:
        return content, False
    out: list[str] = []
    changed = False
    last = None
    for block in blocks:
        if last is not None and block == last and len(block) >= MIN_DUPLICATE_ADJACENT_BLOCK_CHARS:
            changed = True
            continue
        out.append(block)
        last = block
    merged = "\n\n".join(out).strip()
    if merged and merged != raw:
        return merged + "\n", True
    return content, changed


def _split_crawled_chapter_spans(raw: str, chapter_regex: str) -> list[tuple[int, int, int]]:
    """
    Returns [(chapter_number, start, end)] spans for each chapter header match in the raw file.
    """
    matches = list(re.finditer(chapter_regex, raw, flags=re.M))
    spans: list[tuple[int, int, int]] = []
    for index, match in enumerate(matches):
        try:
            chapter_number = int(match.group(1))
        except Exception:
            continue
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        spans.append((chapter_number, start, end))
    return spans


def _replace_chapter_in_batch(*, path: Path, chapter: int, new_block: str, chapter_regex: str) -> bool:
    raw = path.read_text(encoding="utf-8")
    spans = _split_crawled_chapter_spans(raw, chapter_regex)
    for number, start, end in spans:
        if number != chapter:
            continue
        prefix = raw[:start].rstrip("\n")
        suffix = raw[end:].lstrip("\n")
        block = new_block.strip() + "\n"
        updated = f"{prefix}\n\n\n{block}\n\n\n{suffix}".strip() + "\n"
        if updated != raw:
            path.write_text(updated, encoding="utf-8")
            return True
            return False
    return False


def _remove_watermark_lines_in_batch(*, path: Path, chapters: set[int], chapter_regex: str) -> tuple[bool, list[int]]:
    """
    Removes watermark/promo lines from selected chapters in a batch file.
    Returns (changed, cleaned_chapters).
    """
    if not chapters:
        return False, []

    raw = path.read_text(encoding="utf-8")
    spans = _split_crawled_chapter_spans(raw, chapter_regex)
    if not spans:
        return False, []

    changed = False
    cleaned: list[int] = []
    rebuilt_parts: list[str] = []
    last_end = 0

    for number, start, end in spans:
        rebuilt_parts.append(raw[last_end:start])
        block = raw[start:end]
        if number in chapters:
            header, sep, body = block.partition("\n")
            if sep:
                lines = [line for line in body.splitlines() if not _is_watermark_line(line)]
                new_body = "\n".join(line.rstrip() for line in lines).strip()
                new_block = header.strip() + ("\n\n" + new_body if new_body else "")
                new_block = new_block.strip() + "\n"
                if new_block != block:
                    changed = True
                    cleaned.append(number)
                block = new_block
        rebuilt_parts.append(block)
        last_end = end

    rebuilt_parts.append(raw[last_end:])
    updated = "".join(rebuilt_parts).strip() + "\n"
    if changed and updated != raw:
        path.write_text(updated, encoding="utf-8")
        return True, sorted(set(cleaned))
    return False, []


def _remove_metadata_lines_in_batch(*, path: Path, chapters: set[int], chapter_regex: str) -> tuple[bool, list[int]]:
    """
    Removes metadata/noise lines from selected chapters in a batch file.
    Only scans the first few non-empty body lines to avoid over-cleaning prose.
    Returns (changed, cleaned_chapters).
    """
    if not chapters:
        return False, []

    raw = path.read_text(encoding="utf-8")
    spans = _split_crawled_chapter_spans(raw, chapter_regex)
    if not spans:
        return False, []

    changed = False
    cleaned: list[int] = []
    rebuilt_parts: list[str] = []
    last_end = 0

    for number, start, end in spans:
        rebuilt_parts.append(raw[last_end:start])
        block = raw[start:end]
        if number in chapters:
            header, sep, body = block.partition("\n")
            if sep:
                lines: list[str] = []
                non_empty_seen = 0
                removed = False
                for line in body.splitlines():
                    clean = normalize_whitespace(line)
                    if clean:
                        non_empty_seen += 1
                    if clean and non_empty_seen <= METADATA_NOISE_SCAN_LINES and _is_metadata_noise_line(clean):
                        removed = True
                        continue
                    lines.append(line)
                new_body = "\n".join(line.rstrip() for line in lines).strip()
                new_block = header.strip() + ("\n\n" + new_body if new_body else "")
                new_block = new_block.strip() + "\n"
                if removed and new_block != block:
                    changed = True
                    cleaned.append(number)
                block = new_block
        rebuilt_parts.append(block)
        last_end = end

    rebuilt_parts.append(raw[last_end:])
    updated = "".join(rebuilt_parts).strip() + "\n"
    if changed and updated != raw:
        path.write_text(updated, encoding="utf-8")
        return True, sorted(set(cleaned))
    return False, []


def _normalize_paginated_titles_in_batch(*, path: Path, chapters: set[int], chapter_regex: str) -> tuple[bool, list[int]]:
    """
    Removes trailing page markers like "(4/4)" from selected chapter titles.
    Returns (changed, cleaned_chapters).
    """
    if not chapters:
        return False, []

    raw = path.read_text(encoding="utf-8")
    spans = _split_crawled_chapter_spans(raw, chapter_regex)
    if not spans:
        return False, []

    changed = False
    cleaned: list[int] = []
    rebuilt_parts: list[str] = []
    last_end = 0

    for number, start, end in spans:
        rebuilt_parts.append(raw[last_end:start])
        block = raw[start:end]
        if number in chapters:
            header, sep, body = block.partition("\n")
            cleaned_header = _strip_paginated_title_suffix(header)
            if cleaned_header != header:
                changed = True
                cleaned.append(number)
                block = cleaned_header.strip() + (f"{sep}{body}" if sep else "")  # preserve original body/newlines
        rebuilt_parts.append(block)
        last_end = end

    rebuilt_parts.append(raw[last_end:])
    updated = "".join(rebuilt_parts)
    if changed and updated != raw:
        path.write_text(updated, encoding="utf-8")
        return True, sorted(set(cleaned))
    return False, []


def _canonicalize_chapter_blocks_in_batch(*, path: Path, chapters: set[int], chapter_regex: str) -> tuple[bool, list[int]]:
    """
    Rebuilds selected chapter blocks into canonical form:
    - title line normalized
    - duplicate title lines removed from body start
    - body separated from title by a blank line
    - chapter blocks separated from each other by triple newlines
    """
    if not chapters:
        return False, []

    raw = path.read_text(encoding="utf-8")
    parsed = _split_crawled_chapters(raw, chapter_regex)
    if not parsed:
        return False, []

    changed = False
    cleaned: list[int] = []
    rebuilt_blocks: list[str] = []
    rebuilt_numbers: list[int] = []

    for chapter_number, title, body in parsed:
        original_block = title.strip() + (f"\n{body}" if body else "")
        if chapter_number in chapters:
            canonical_block = _canonicalize_chapter_block(original_block)
            if canonical_block != original_block.strip("\n"):
                changed = True
                cleaned.append(chapter_number)
            canonical_body = canonical_block.partition("\n")[2].strip()
            if rebuilt_numbers and rebuilt_numbers[-1] == chapter_number:
                previous_block = rebuilt_blocks[-1]
                previous_body = previous_block.partition("\n")[2].strip()
                # Prefer the duplicate block that actually has content.
                if previous_body and not canonical_body:
                    changed = True
                    cleaned.append(chapter_number)
                    continue
                if canonical_body and not previous_body:
                    changed = True
                    cleaned.append(chapter_number)
                    rebuilt_blocks[-1] = canonical_block
                    continue
            rebuilt_blocks.append(canonical_block)
            rebuilt_numbers.append(chapter_number)
        else:
            rebuilt_blocks.append(original_block.strip("\n"))
            rebuilt_numbers.append(chapter_number)

    updated = "\n\n\n".join(block for block in rebuilt_blocks if block.strip()) + "\n"
    if changed and updated != raw:
        path.write_text(updated, encoding="utf-8")
        return True, sorted(set(cleaned))
    return False, []


def _remove_duplicate_chapters_in_batch(*, path: Path, chapters: set[int], chapter_regex: str) -> tuple[bool, list[int]]:
    """
    Removes duplicated chapter blocks for specific chapter numbers, keeping the first occurrence.
    Returns (changed, removed_chapters).
    """
    if not chapters:
        return False, []
    raw = path.read_text(encoding="utf-8")
    spans = _split_crawled_chapter_spans(raw, chapter_regex)
    if not spans:
        return False, []
    seen: set[int] = set()
    keep_ranges: list[tuple[int, int]] = []
    removed: set[int] = set()
    for number, start, end in spans:
        if number in chapters:
            if number in seen:
                removed.add(number)
                continue
            seen.add(number)
        keep_ranges.append((start, end))
    if not removed:
        return False, []
    # Rebuild content by concatenating kept spans + any preface before first span.
    out = raw[: keep_ranges[0][0]].rstrip("\n") if keep_ranges else raw
    kept_blocks = [raw[start:end].strip("\n") for start, end in keep_ranges]
    merged = "\n\n\n".join(block for block in kept_blocks if block).strip()
    updated = (out + "\n\n\n" + merged).strip() + "\n" if merged else (out.strip() + "\n")
    if updated != raw:
        path.write_text(updated, encoding="utf-8")
        return True, sorted(removed)
    return False, []


def _rewrite_index_gap_placeholders_in_batch(
    *,
    path: Path,
    index_gaps: set[int],
    chapter_regex: str,
    cfg: RepairConfig,
) -> tuple[bool, list[int]]:
    """
    Rewrites existing placeholder content for index-gap chapters to the current placeholder template.
    Only rewrites when the existing block looks like a placeholder (to avoid overwriting real content).
    """
    if not index_gaps:
        return False, []
    raw = path.read_text(encoding="utf-8")
    chapters = _split_crawled_chapters(raw, chapter_regex)
    if not chapters:
        return False, []
    rewritten: list[int] = []
    for chapter_number, title, body in chapters:
        if chapter_number not in index_gaps:
            continue
        normalized_title = normalize_whitespace(title)
        normalized_body = normalize_whitespace(body)
        if not (
            re.search(r"(略过|略過)", normalized_title)
            or "占位" in normalized_body
            or "更新/请假通知" in normalized_body
            or "本章为更新" in normalized_body
        ):
            continue
        block = _placeholder_block(chapter_number, cfg)
        if _replace_chapter_in_batch(path=path, chapter=chapter_number, new_block=block, chapter_regex=chapter_regex):
            rewritten.append(chapter_number)
    return bool(rewritten), sorted(set(rewritten))


def _insert_chapter_into_best_batch(*, origin_dir: Path, chapter: int, block: str) -> Path:
    """
    Inserts a chapter into the best existing batch file if possible; otherwise creates a new single-chapter batch.
    """
    for path in _iter_origin_batch_files(origin_dir):
        match = BATCH_FILENAME_PATTERN.match(path.name)
        if not match:
            continue
        start = int(match.group(1))
        end = int(match.group(2))
        if start <= chapter <= end:
            # Insert by treating it as a missing placeholder insertion with a single chapter.
            raw = path.read_text(encoding="utf-8")
            spans = _split_crawled_chapter_spans(raw, r"^第(\d+)章([^\n]*)")
            next_candidates = [start for num, start, _end in spans if num > chapter]
            insert_at = min(next_candidates) if next_candidates else len(raw)
            prefix = raw[:insert_at].rstrip("\n")
            suffix = raw[insert_at:].lstrip("\n")
            updated = f"{prefix}\n\n\n{block.strip()}\n\n\n{suffix}".strip() + "\n"
            path.write_text(updated, encoding="utf-8")
            return path
    # Create new batch.
    origin_dir.mkdir(parents=True, exist_ok=True)
    output_path = origin_dir / f"chuong_{chapter}-{chapter}.txt"
    output_path.write_text(block.strip() + "\n", encoding="utf-8")
    return output_path


def _fetch_replacement_block(
    *,
    config: NovelConfig,
    chapter: int,
    rule: ChapterRepairRule,
    now: str,
    actions: list[RepairAction],
    dedupe: bool,
) -> tuple[str, str, str, bool]:
    """
    Returns (block, source_id, url, dedup_applied).
    """
    registry = build_default_registry()
    last_exc: Exception | None = None
    for index, cand in enumerate(rule.candidates):
        if cand.kind == "inline":
            title = (cand.title or f"第{chapter}章").strip()
            content = (cand.content or "").strip()
            block = f"{title}\n\n{content}\n"
            dedup_applied = False
            if dedupe:
                deduped, changed = _dedupe_adjacent_blocks(content)
                if changed:
                    dedup_applied = True
                    block = f"{title}\n\n{deduped}".rstrip() + "\n"
            action = "replaced_from_source" if index == 0 else "fallback_replaced"
            actions.append(
                RepairAction(
                    action=action,
                    chapter=chapter,
                    reason="manual_override",
                    target_file="",
                    timestamp=now,
                    from_source_id="inline",
                    from_url="",
                )
            )
            return block, "inline", "", dedup_applied

        source_id = (cand.source_id or "").strip()
        url = (cand.url or "").strip()
        if not source_id or not url:
            continue
        try:
            source_raw = _load_source_config_from_repo(config.storage.root, source_id)
            crawl_raw = source_raw.get("crawl", {}) if isinstance(source_raw.get("crawl", {}), dict) else {}
            browser_raw = crawl_raw.pop("browser_debug", {}) if isinstance(crawl_raw.get("browser_debug", {}), dict) else {}
            crawl_cfg = config.crawl.__class__(**crawl_raw, browser_debug=config.crawl.browser_debug.__class__(**browser_raw))

            resolver = registry.get(source_raw.get("resolver_id", source_id))
            # Clone config for accurate logging + delays/timeout/backoff.
            tmp_source = config.source.__class__(
                source_id=source_id,
                resolver_id=source_raw.get("resolver_id", source_id),
                crawl=crawl_cfg,
            )
            tmp_cfg = config.__class__(
                novel_id=config.novel_id,
                title=config.title,
                slug=config.slug,
                source_language=config.source_language,
                target_language=config.target_language,
                source_id=source_id,
                source=tmp_source,
                storage=config.storage,
                crawl=crawl_cfg,
                models=config.models,
                translation=config.translation,
                captions=config.captions,
                queue=config.queue,
                tts=config.tts,
                media=config.media,
                proxy_gateway=config.proxy_gateway,
            )
            strategy_chain = build_strategy_chain(
                tmp_cfg.crawl,
                tmp_cfg.crawl.browser_debug,
                proxy_gateway=tmp_cfg.proxy_gateway,
                redis_cfg=tmp_cfg.queue.redis,
                proxy_session_state=CrawlProxySessionState(),
            )
            entry = ChapterEntry(chapter, f"第{chapter}章", url)
            block, _parsed_number, _stats = _fetch_chapter(entry, tmp_cfg, resolver, strategy_chain)
            title_line, _, body = block.partition("\n")
            dedup_applied = False
            if dedupe:
                deduped_body, changed = _dedupe_adjacent_blocks(body)
                if changed:
                    dedup_applied = True
                    block = f"{title_line.strip()}\n\n{deduped_body}".rstrip() + "\n"
            action = "replaced_from_source" if index == 0 else "fallback_replaced"
            actions.append(
                RepairAction(
                    action=action,
                    chapter=chapter,
                    reason="manual_override" if index == 0 else "garbage_detected",
                    target_file="",
                    timestamp=now,
                    from_source_id=source_id,
                    from_url=url,
                )
            )
            return block, source_id, url, dedup_applied
        except Exception as exc:
            last_exc = exc
            continue

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"No replacement candidates available for chapter {chapter}")


def verify_crawled_content(
    config: NovelConfig,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
    filenames: list[str] | None = None,
    *,
    fix_stale_manifest: bool = True,
    delete_empty_manifest: bool = True,
) -> CrawlVerifyReport:
    checked_files: list[Path] = []
    checked_chapters: list[int] = []
    issues: list[CrawlVerifyIssue] = []
    seen_chapters: dict[int, Path] = {}
    valid_chapters: set[int] = set()
    stale_failures_removed = 0
    failure_manifest_deleted = False

    if filenames:
        batch_files = [config.storage.origin_dir / name for name in filenames]
    else:
        batch_files = [
            path
            for path in _iter_origin_batch_files(config.storage.origin_dir)
            if _batch_file_overlaps_range(path, from_chapter, to_chapter)
        ]

    if not batch_files:
        issues.append(
            CrawlVerifyIssue(
                code="no_origin_files",
                message=f"Khong tim thay file crawl trong {config.storage.origin_dir}",
                path=config.storage.origin_dir,
            )
        )
        return CrawlVerifyReport(
            checked_files=checked_files,
            checked_chapters=checked_chapters,
            issues=issues,
            stale_failures_removed=stale_failures_removed,
            failure_manifest_deleted=failure_manifest_deleted,
        )

    for batch_file in batch_files:
        checked_files.append(batch_file)
        if not batch_file.exists():
            issues.append(
                CrawlVerifyIssue(code="missing_file", message=f"Thieu file {batch_file.name}", path=batch_file)
            )
            continue

        raw = batch_file.read_text(encoding="utf-8")
        chapters = _split_crawled_chapters(raw, config.translation.chapter_regex)
        filename_match = BATCH_FILENAME_PATTERN.match(batch_file.name)
        expected_range = None
        if filename_match:
            expected_range = (int(filename_match.group(1)), int(filename_match.group(2)))
        else:
            issues.append(
                CrawlVerifyIssue(
                    code="invalid_batch_filename",
                    message=f"Ten file khong dung format batch: {batch_file.name}",
                    path=batch_file,
                )
            )

        if not chapters:
            issues.append(
                CrawlVerifyIssue(
                    code="unparseable_file",
                    message=f"Khong tach duoc chapter tu {batch_file.name}",
                    path=batch_file,
                )
            )
            continue

        chapter_numbers_in_file: list[int] = []
        for chapter_number, title, body in chapters:
            if from_chapter is not None and chapter_number < from_chapter:
                continue
            if to_chapter is not None and chapter_number > to_chapter:
                continue

            chapter_numbers_in_file.append(chapter_number)
            checked_chapters.append(chapter_number)

            previous_path = seen_chapters.get(chapter_number)
            if previous_path is not None:
                issues.append(
                    CrawlVerifyIssue(
                        code="duplicate_chapter",
                        message=f"Chuong {chapter_number} bi trung trong {previous_path.name} va {batch_file.name}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )
            else:
                seen_chapters[chapter_number] = batch_file

            normalized_title = normalize_whitespace(title)
            normalized_body = normalize_whitespace(body)
            if not normalized_body:
                issues.append(
                    CrawlVerifyIssue(
                        code="empty_chapter",
                        message=f"Chuong {chapter_number} rong trong {batch_file.name}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )
                continue

            if _has_paginated_title_suffix(normalized_title):
                issues.append(
                    CrawlVerifyIssue(
                        code="paginated_title",
                        message=f"Tieu de chuong {chapter_number} con hau to phan trang: {normalized_title}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )

            watermark_error = _detect_watermark_content(body)
            if watermark_error is not None:
                issues.append(
                    CrawlVerifyIssue(
                        code="watermark_content",
                        message=f"Chuong {chapter_number} co dong watermark/promo: {watermark_error}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )
                continue

            metadata_noise_error = _detect_metadata_noise_content(body)
            if metadata_noise_error is not None:
                issues.append(
                    CrawlVerifyIssue(
                        code="metadata_content",
                        message=f"Chuong {chapter_number} co dong metadata/noise: {metadata_noise_error}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )
                continue

            validation_error = _validate_chapter_content(normalized_title, normalized_body)
            if validation_error is not None:
                issues.append(
                    CrawlVerifyIssue(
                        code="invalid_chapter_content",
                        message=f"Chuong {chapter_number} co noi dung nghi loi: {validation_error}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )
            else:
                duplicate_error = _detect_duplicated_content(normalized_body)
                if duplicate_error is not None:
                    issues.append(
                        CrawlVerifyIssue(
                            code="duplicated_content",
                            message=f"Chuong {chapter_number} co noi dung bi lap: {duplicate_error}",
                            chapter_number=chapter_number,
                            path=batch_file,
                        )
                    )
                else:
                    valid_chapters.add(chapter_number)

            detected_number = re.search(config.translation.chapter_regex, normalized_title, flags=re.M)
            if detected_number and int(detected_number.group(1)) != chapter_number:
                issues.append(
                    CrawlVerifyIssue(
                        code="header_mismatch",
                        message=f"Tieu de chuong {chapter_number} khong khop so chapter: {normalized_title}",
                        chapter_number=chapter_number,
                        path=batch_file,
                    )
                )

        if expected_range is not None:
            expected_numbers = set(range(expected_range[0], expected_range[1] + 1))
            if from_chapter is not None or to_chapter is not None:
                range_start = from_chapter if from_chapter is not None else expected_range[0]
                range_end = to_chapter if to_chapter is not None else expected_range[1]
                expected_numbers &= set(range(range_start, range_end + 1))
            actual_numbers = set(chapter_numbers_in_file)
            actual_min = min(actual_numbers) if actual_numbers else None
            actual_max = max(actual_numbers) if actual_numbers else None
            for missing in sorted(expected_numbers - actual_numbers):
                if actual_min is not None and missing < actual_min:
                    continue
                if actual_max is not None and missing > actual_max:
                    continue
                issues.append(
                    CrawlVerifyIssue(
                        code="missing_chapter_in_batch",
                        message=f"Thieu chuong {missing}",
                        chapter_number=missing,
                        path=batch_file,
                    )
                )

    if from_chapter is not None and to_chapter is not None:
        checked_set = set(checked_chapters)
        for chapter_number in range(from_chapter, to_chapter + 1):
            if chapter_number not in checked_set:
                issues.append(
                    CrawlVerifyIssue(
                        code="missing_chapter_in_range",
                        message=f"Khong tim thay chuong {chapter_number} trong range {from_chapter}-{to_chapter}",
                        chapter_number=chapter_number,
                    )
                )

    effective_from = from_chapter
    effective_to = to_chapter
    if effective_from is None and effective_to is None and checked_chapters:
        effective_from = min(checked_chapters)
        effective_to = max(checked_chapters)

    manifest = _load_failure_manifest(config)
    failures = manifest.get("failures", {})
    if isinstance(failures, dict):
        failures_changed = False
        for raw_key, payload in sorted(failures.items(), key=lambda item: int(item[0])):
            chapter_number = int(raw_key)
            if effective_from is not None and chapter_number < effective_from:
                continue
            if effective_to is not None and chapter_number > effective_to:
                continue
            reason = payload.get("reason", "unknown") if isinstance(payload, dict) else "unknown"
            details = payload.get("details", "") if isinstance(payload, dict) else ""
            is_stale = chapter_number in valid_chapters
            if is_stale and fix_stale_manifest:
                del failures[raw_key]
                stale_failures_removed += 1
                failures_changed = True
                continue
            code = "stale_manifest" if is_stale else "failure_manifest_entry"
            issues.append(
                CrawlVerifyIssue(
                    code=code,
                    message=f"Manifest bao loi chuong {chapter_number}: {reason} {details}".strip(),
                    chapter_number=chapter_number,
                    path=_failure_manifest_path(config),
                )
            )
        if failures_changed and fix_stale_manifest:
            manifest_path = _failure_manifest_path(config)
            if delete_empty_manifest and not failures:
                if manifest_path.exists():
                    manifest_path.unlink()
                    failure_manifest_deleted = True
            else:
                _save_failure_manifest(config, manifest)

    checked_chapters = sorted(set(checked_chapters))
    issues.sort(key=lambda issue: (issue.chapter_number is None, issue.chapter_number or 0, issue.code, issue.message))
    return CrawlVerifyReport(
        checked_files=checked_files,
        checked_chapters=checked_chapters,
        issues=issues,
        stale_failures_removed=stale_failures_removed,
        failure_manifest_deleted=failure_manifest_deleted,
    )


def repair_crawled_content(
    config: NovelConfig,
    from_chapter: int | None,
    to_chapter: int | None,
    *,
    filenames: list[str] | None = None,
    log_path: Path | None = None,
    generate_repair_config_if_missing: bool = False,
) -> RepairReport:
    """
    Repairs already-crawled origin files within a range and writes a report.

    Current behavior (minimal, local-only):
      - Inserts placeholder chapters into batch files where verify reports missing chapters.
      - Overwrites the report log file each run.
    """
    executed_at = datetime.now(timezone.utc).isoformat()
    log_path = log_path or _default_repair_log_path(config)

    rc_path = repair_config_path(config.storage.input_dir)
    if rc_path.exists():
        repair_cfg = load_repair_config(rc_path)
    else:
        if not generate_repair_config_if_missing:
            raise FileNotFoundError(f"Missing repair config: {rc_path}")
        repair_cfg = generate_repair_config_from_research(
            root=config.storage.root,
            novel_id=config.novel_id,
            logs_dir=config.storage.logs_dir,
            input_dir=config.storage.input_dir,
        )
        save_repair_config(rc_path, repair_cfg)

    def infer_range() -> tuple[int, int]:
        batch_files: list[Path]
        if filenames:
            batch_files = [config.storage.origin_dir / name for name in filenames]
        else:
            batch_files = _iter_origin_batch_files(config.storage.origin_dir)
        starts: list[int] = []
        ends: list[int] = []
        for path in batch_files:
            match = BATCH_FILENAME_PATTERN.match(path.name)
            if not match:
                continue
            starts.append(int(match.group(1)))
            ends.append(int(match.group(2)))
        if starts and ends:
            return min(starts), max(ends)
        # Fallback: infer from repair config itself (if origin batches are not available).
        candidates = list(repair_cfg.index_gaps) + list(repair_cfg.replacements.keys())
        if candidates:
            return min(candidates), max(candidates)
        raise ValueError("Unable to infer repair range (no origin batches and empty repair config)")

    if (from_chapter is None) ^ (to_chapter is None):
        raise ValueError("repair_crawled_content requires both from_chapter and to_chapter, or neither")
    if from_chapter is None and to_chapter is None:
        from_chapter, to_chapter = infer_range()
    assert from_chapter is not None and to_chapter is not None

    verify_report = verify_crawled_content(
        config,
        from_chapter=from_chapter,
        to_chapter=to_chapter,
        filenames=filenames,
        fix_stale_manifest=False,
        delete_empty_manifest=False,
    )

    missing_by_file: dict[Path, list[int]] = {}
    missing_in_range: set[int] = set()
    invalid_chapters: set[int] = set()
    duplicate_chapters: dict[Path, set[int]] = {}
    watermark_chapters: set[int] = set()
    metadata_noise_chapters: set[int] = set()
    paginated_title_chapters: set[int] = set()
    for issue in verify_report.issues:
        if issue.chapter_number is None:
            continue
        if issue.code == "missing_chapter_in_batch" and issue.path is not None:
            missing_by_file.setdefault(issue.path, []).append(int(issue.chapter_number))
            continue
        if issue.code == "missing_chapter_in_range":
            missing_in_range.add(int(issue.chapter_number))
            continue
        if issue.code == "duplicate_chapter" and issue.path is not None:
            duplicate_chapters.setdefault(issue.path, set()).add(int(issue.chapter_number))
            continue
        if issue.code == "watermark_content":
            watermark_chapters.add(int(issue.chapter_number))
            continue
        if issue.code == "metadata_content":
            metadata_noise_chapters.add(int(issue.chapter_number))
            continue
        if issue.code == "paginated_title":
            paginated_title_chapters.add(int(issue.chapter_number))
            continue
        if issue.code in {"invalid_chapter_content", "duplicated_content"}:
            invalid_chapters.add(int(issue.chapter_number))

    actions: list[RepairAction] = []
    modified_files: list[Path] = []
    handled_chapters: set[int] = set()

    now = datetime.now(timezone.utc).isoformat()

    # 0) Remove duplicated placeholder chapters for index gaps to keep repair idempotent.
    index_gap_set = set(repair_cfg.index_gaps)
    for batch_file, chapters in sorted(duplicate_chapters.items(), key=lambda item: item[0].name):
        if not batch_file.exists():
            continue
        to_remove = {ch for ch in chapters if ch in index_gap_set}
        if not to_remove:
            continue
        changed, removed = _remove_duplicate_chapters_in_batch(
            path=batch_file,
            chapters=to_remove,
            chapter_regex=config.translation.chapter_regex,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter in removed:
            actions.append(
                RepairAction(
                    action="dedup_applied",
                    chapter=int(chapter),
                    reason="index_gap",
                    target_file=batch_file.name,
                    timestamp=now,
                )
            )
            handled_chapters.add(int(chapter))

    # 1) Apply replacements (garbage fixes / manual override list) first.
    batch_files: list[Path]
    if filenames:
        batch_files = [config.storage.origin_dir / name for name in filenames]
    else:
        batch_files = [
            path
            for path in _iter_origin_batch_files(config.storage.origin_dir)
            if _batch_file_overlaps_range(path, from_chapter, to_chapter)
        ]
    for batch_file in batch_files:
        if not batch_file.exists() or not batch_file.is_file():
            continue
        # Rewrite any existing placeholder blocks for index gaps to keep placeholder content consistent.
        changed, rewritten = _rewrite_index_gap_placeholders_in_batch(
            path=batch_file,
            index_gaps=index_gap_set,
            chapter_regex=config.translation.chapter_regex,
            cfg=repair_cfg,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter in rewritten:
            actions.append(
                RepairAction(
                    action="replaced_from_source",
                    chapter=int(chapter),
                    reason="index_gap",
                    target_file=batch_file.name,
                    timestamp=now,
                    from_source_id="placeholder",
                    from_url="",
                )
            )
            handled_chapters.add(int(chapter))
        # Replace configured chapters if present in the file, especially if verify flagged invalid/duplicated.
        raw = batch_file.read_text(encoding="utf-8")
        present = {num for num, _, _ in _split_crawled_chapter_spans(raw, config.translation.chapter_regex)}
        for chapter, rule in sorted(repair_cfg.replacements.items(), key=lambda item: item[0]):
            if chapter < from_chapter or chapter > to_chapter:
                continue
            if chapter in handled_chapters:
                continue
            if chapter not in present:
                continue
            block, source_id, url, dedup_applied = _fetch_replacement_block(
                config=config,
                chapter=chapter,
                rule=rule,
                now=now,
                actions=actions,
                dedupe=repair_cfg.dedupe_repeated_blocks,
            )
            replaced = _replace_chapter_in_batch(
                path=batch_file,
                chapter=chapter,
                new_block=block,
                chapter_regex=config.translation.chapter_regex,
            )
            if replaced:
                modified_files.append(batch_file)
                handled_chapters.add(int(chapter))
                # Backfill target_file in the last action for this chapter.
                for idx in range(len(actions) - 1, -1, -1):
                    if actions[idx].chapter == chapter and actions[idx].target_file == "":
                        actions[idx] = RepairAction(
                            action=actions[idx].action,
                            chapter=actions[idx].chapter,
                            reason="garbage_detected" if chapter in invalid_chapters else actions[idx].reason,
                            target_file=batch_file.name,
                            timestamp=actions[idx].timestamp,
                            from_source_id=actions[idx].from_source_id,
                            from_url=actions[idx].from_url,
                )
                        break
                if dedup_applied:
                    actions.append(
                        RepairAction(
                            action="dedup_applied",
                            chapter=chapter,
                            reason="manual_override",
                            target_file=batch_file.name,
                            timestamp=now,
                            from_source_id=source_id,
                            from_url=url,
                        )
                    )

    # 1b) Strip watermark / promo lines from affected chapters.
    for batch_file in batch_files:
        if not batch_file.exists() or not batch_file.is_file():
            continue
        raw = batch_file.read_text(encoding="utf-8")
        present = {num for num, _, _ in _split_crawled_chapter_spans(raw, config.translation.chapter_regex)}
        targets = {chapter for chapter in watermark_chapters if from_chapter <= chapter <= to_chapter and chapter in present}
        if not targets:
            continue
        changed, cleaned = _remove_watermark_lines_in_batch(
            path=batch_file,
            chapters=targets,
            chapter_regex=config.translation.chapter_regex,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter in cleaned:
            handled_chapters.add(int(chapter))
            actions.append(
                RepairAction(
                    action="watermark_removed",
                    chapter=int(chapter),
                    reason="watermark_content",
                    target_file=batch_file.name,
                    timestamp=now,
                )
            )

    # 1bb) Normalize paginated chapter titles such as "(4/4)" suffixes.
    for batch_file in batch_files:
        if not batch_file.exists() or not batch_file.is_file():
            continue
        raw = batch_file.read_text(encoding="utf-8")
        present = {num for num, _, _ in _split_crawled_chapter_spans(raw, config.translation.chapter_regex)}
        targets = {
            chapter for chapter in paginated_title_chapters if from_chapter <= chapter <= to_chapter and chapter in present
        }
        if not targets:
            continue
        changed, cleaned = _normalize_paginated_titles_in_batch(
            path=batch_file,
            chapters=targets,
            chapter_regex=config.translation.chapter_regex,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter in cleaned:
            handled_chapters.add(int(chapter))
            actions.append(
                RepairAction(
                    action="title_normalized",
                    chapter=int(chapter),
                    reason="paginated_title",
                    target_file=batch_file.name,
                    timestamp=now,
                )
            )

    # 1c) Strip leading metadata/noise lines from affected chapters.
    for batch_file in batch_files:
        if not batch_file.exists() or not batch_file.is_file():
            continue
        raw = batch_file.read_text(encoding="utf-8")
        present = {num for num, _, _ in _split_crawled_chapter_spans(raw, config.translation.chapter_regex)}
        targets = {
            chapter
            for chapter in metadata_noise_chapters
            if from_chapter <= chapter <= to_chapter and chapter in present
        }
        if not targets:
            continue
        changed, cleaned = _remove_metadata_lines_in_batch(
            path=batch_file,
            chapters=targets,
            chapter_regex=config.translation.chapter_regex,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter in cleaned:
            handled_chapters.add(int(chapter))
            actions.append(
                RepairAction(
                    action="metadata_removed",
                    chapter=int(chapter),
                    reason="metadata_content",
                    target_file=batch_file.name,
                    timestamp=now,
                )
            )

    # 1d) Canonicalize chapter formatting in-range so titles/body separators stay stable
    # after repair passes and paginated duplicate title lines are removed.
    for batch_file in batch_files:
        if not batch_file.exists() or not batch_file.is_file():
            continue
        raw = batch_file.read_text(encoding="utf-8")
        present = {num for num, _, _ in _split_crawled_chapter_spans(raw, config.translation.chapter_regex)}
        targets = {chapter for chapter in present if from_chapter <= chapter <= to_chapter}
        if not targets:
            continue
        changed, cleaned = _canonicalize_chapter_blocks_in_batch(
            path=batch_file,
            chapters=targets,
            chapter_regex=config.translation.chapter_regex,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter in cleaned:
            handled_chapters.add(int(chapter))
            actions.append(
                RepairAction(
                    action="chapter_canonicalized",
                    chapter=int(chapter),
                    reason="format_cleanup",
                    target_file=batch_file.name,
                    timestamp=now,
                )
            )

    # 2) Insert placeholders for index gaps (batch-level missing).
    for batch_file, missing_chapters in sorted(missing_by_file.items(), key=lambda item: item[0].name):
        if not batch_file.exists():
            continue
        gaps = [chapter for chapter in missing_chapters if chapter in index_gap_set and chapter not in handled_chapters]
        if not gaps:
            continue
        changed, inserted = _insert_placeholders_into_batch(
            path=batch_file,
            missing_chapters=gaps,
            chapter_regex=config.translation.chapter_regex,
            cfg=repair_cfg,
        )
        if changed:
            modified_files.append(batch_file)
        for chapter_number, reason in inserted:
            handled_chapters.add(int(chapter_number))
            actions.append(
                RepairAction(
                    action="placeholder_added",
                    chapter=int(chapter_number),
                    reason="index_gap" if int(chapter_number) in set(repair_cfg.index_gaps) else reason,
                    target_file=batch_file.name,
                    timestamp=now,
                )
            )

    # 3) Fill remaining missing-in-range chapters using replacements if available; otherwise placeholder if index-gap.
    for missing in sorted(missing_in_range):
        if missing < from_chapter or missing > to_chapter:
            continue
        if missing in handled_chapters:
            continue
        if missing in set(repair_cfg.index_gaps):
            block = _placeholder_block(missing, repair_cfg)
            target = _insert_chapter_into_best_batch(origin_dir=config.storage.origin_dir, chapter=missing, block=block)
            modified_files.append(target)
            handled_chapters.add(int(missing))
            actions.append(
                RepairAction(
                    action="placeholder_added",
                    chapter=missing,
                    reason="index_gap",
                    target_file=target.name,
                    timestamp=now,
                )
            )
            continue
        rule = repair_cfg.replacements.get(missing)
        if not rule:
            continue
        block, source_id, url, dedup_applied = _fetch_replacement_block(
            config=config,
            chapter=missing,
            rule=rule,
            now=now,
            actions=actions,
            dedupe=repair_cfg.dedupe_repeated_blocks,
        )
        target = _insert_chapter_into_best_batch(origin_dir=config.storage.origin_dir, chapter=missing, block=block)
        modified_files.append(target)
        for idx in range(len(actions) - 1, -1, -1):
            if actions[idx].chapter == missing and actions[idx].target_file == "":
                actions[idx] = RepairAction(
                    action=actions[idx].action,
                    chapter=actions[idx].chapter,
                    reason="missing_entry",
                    target_file=target.name,
                    timestamp=actions[idx].timestamp,
                    from_source_id=actions[idx].from_source_id,
                    from_url=actions[idx].from_url,
                )
                break
        if dedup_applied:
            actions.append(
                RepairAction(
                    action="dedup_applied",
                    chapter=missing,
                    reason="missing_entry",
                    target_file=target.name,
                    timestamp=now,
                    from_source_id=source_id,
                    from_url=url,
                )
            )

    report = RepairReport(
        novel_id=config.novel_id,
        from_chapter=int(from_chapter),
        to_chapter=int(to_chapter),
        executed_at=executed_at,
        actions=sorted(actions, key=lambda a: (a.chapter, a.action, a.target_file)),
        log_path=log_path,
        modified_files=sorted(set(modified_files), key=lambda p: p.name),
    )
    _write_repair_report(report)
    return report


def _prune_failure_manifest_against_origin(config: NovelConfig, from_chapter: int, to_chapter: int) -> tuple[int, bool]:
    manifest_path = _failure_manifest_path(config)
    if not manifest_path.exists():
        return 0, False

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Unable to read failure manifest (skip prune) | path=%s", manifest_path)
        return 0, False

    failures = manifest.get("failures", {})
    if not isinstance(failures, dict) or not failures:
        return 0, False

    valid_chapters: set[int] = set()
    batch_files = [
        path
        for path in _iter_origin_batch_files(config.storage.origin_dir)
        if _batch_file_overlaps_range(path, from_chapter, to_chapter)
    ]
    for batch_file in batch_files:
        try:
            raw = batch_file.read_text(encoding="utf-8")
        except Exception:
            continue
        chapters = _split_crawled_chapters(raw, config.translation.chapter_regex)
        for chapter_number, title, body in chapters:
            if chapter_number < from_chapter or chapter_number > to_chapter:
                continue
            normalized_title = normalize_whitespace(title)
            normalized_body = normalize_whitespace(body)
            if not normalized_body:
                continue
            if _validate_chapter_content(normalized_title, normalized_body) is None and _detect_duplicated_content(normalized_body) is None:
                valid_chapters.add(chapter_number)

    removed = 0
    for raw_key in list(failures.keys()):
        try:
            chapter_number = int(raw_key)
        except (TypeError, ValueError):
            continue
        if chapter_number < from_chapter or chapter_number > to_chapter:
            continue
        if chapter_number in valid_chapters:
            del failures[raw_key]
            removed += 1

    if removed <= 0:
        return 0, False

    if not failures:
        manifest_path.unlink()
        return removed, True

    _save_failure_manifest(config, manifest)
    return removed, False


def _fetch_chapter(entry: ChapterEntry, config: NovelConfig, resolver, strategy_chain) -> tuple[str, int, dict[str, object]]:
    visited: set[str] = set()
    parts: list[str] = []
    effective_title = entry.title
    url = entry.url
    part_index = 1
    started_at = time.time()
    last_result = None
    while url and url not in visited:
        visited.add(url)
        parsed = None
        result = None
        max_attempts = max(1, config.crawl.max_fetch_retries)
        for attempt in range(1, max_attempts + 1):
            LOGGER.info(
                "Fetching chapter %s part %s from %s (attempt %s/%s)",
                entry.chapter_number,
                part_index,
                url,
                attempt,
                max_attempts,
            )
            try:
                result = strategy_chain.fetch(url, config.crawl.request_timeout_seconds)
                last_result = result
            except Exception:
                LOGGER.exception(
                    "Chapter fetch failed | novel=%s source=%s chapter=%s part=%s url=%s attempt=%s/%s",
                    config.novel_id,
                    config.crawl.site_id,
                    entry.chapter_number,
                    part_index,
                    url,
                    attempt,
                    max_attempts,
                )
                if attempt >= max_attempts:
                    raise
                backoff = config.crawl.retry_backoff_seconds * attempt
                LOGGER.warning("Retrying chapter fetch after %.1fs", backoff)
                time.sleep(backoff)
                continue

            if result.challenge_detected:
                reason = result.block_reason or "challenge"
                cooldown = (
                    config.crawl.rate_limit_cooldown_seconds
                    if reason == "rate_limited"
                    else config.crawl.retry_backoff_seconds * attempt
                )
                LOGGER.warning(
                    "Chapter blocked | novel=%s source=%s chapter=%s part=%s url=%s reason=%s attempt=%s/%s cooldown=%.1fs",
                    config.novel_id,
                    config.crawl.site_id,
                    entry.chapter_number,
                    part_index,
                    url,
                    reason,
                    attempt,
                    max_attempts,
                    cooldown,
                )
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"Blocked by {reason} while fetching chapter {entry.chapter_number} from {url}"
                    )
                time.sleep(cooldown)
                continue

            try:
                parsed = resolver.parse_chapter(result.html, entry.chapter_number, effective_title or result.title)
            except Exception:
                LOGGER.exception(
                    "Chapter parse failed | novel=%s source=%s chapter=%s part=%s url=%s final_url=%s attempt=%s/%s",
                    config.novel_id,
                    config.crawl.site_id,
                    entry.chapter_number,
                    part_index,
                    url,
                    result.final_url,
                    attempt,
                    max_attempts,
                )
                if attempt >= max_attempts:
                    raise
                backoff = config.crawl.retry_backoff_seconds * attempt
                LOGGER.warning("Retrying chapter parse after %.1fs", backoff)
                time.sleep(backoff)
                continue

            if parsed.content.strip():
                validation_error = _validate_chapter_content(parsed.title, parsed.content)
                if validation_error is None:
                    LOGGER.info(
                        "Chapter part valid | novel=%s source=%s chapter=%s part=%s chars=%s strategy=%s final_url=%s",
                        config.novel_id,
                        config.crawl.site_id,
                        entry.chapter_number,
                        part_index,
                        len(parsed.content),
                        result.strategy_name,
                        result.final_url,
                    )
                    parts.append(parsed.content)
                    effective_title = parsed.title
                    break
                LOGGER.warning(
                    "Invalid chapter content | novel=%s source=%s chapter=%s part=%s url=%s reason=%s attempt=%s/%s",
                    config.novel_id,
                    config.crawl.site_id,
                    entry.chapter_number,
                    part_index,
                    result.final_url,
                    validation_error,
                    attempt,
                    max_attempts,
                )
                if attempt >= max_attempts:
                    raise RuntimeError(
                        f"Invalid content ({validation_error}) while fetching chapter {entry.chapter_number} from {url}"
                    )
                backoff = config.crawl.retry_backoff_seconds * attempt
                LOGGER.warning("Retrying invalid chapter after %.1fs", backoff)
                time.sleep(backoff)
                continue

            LOGGER.warning(
                "Empty chapter content | novel=%s source=%s chapter=%s part=%s url=%s attempt=%s/%s",
                config.novel_id,
                config.crawl.site_id,
                entry.chapter_number,
                part_index,
                result.final_url if result else url,
                attempt,
                max_attempts,
            )
            if attempt >= max_attempts:
                raise RuntimeError(
                    f"Empty content while fetching chapter {entry.chapter_number} from {url}"
                )
            backoff = config.crawl.retry_backoff_seconds * attempt
            LOGGER.warning("Retrying empty chapter after %.1fs", backoff)
            time.sleep(backoff)

        if parsed is None:
            raise RuntimeError(f"Unable to fetch chapter {entry.chapter_number} part {part_index}")
        next_url = resolver.find_next_part_url(result.html, result.final_url, entry.chapter_number)
        if not next_url:
            break
        url = next_url
        part_index += 1
        time.sleep(config.crawl.delay_between_chapters_seconds)
    combined_content = "\n\n".join(parts).strip()
    duration_seconds = round(time.time() - started_at, 2)
    return (
        f"{effective_title}\n\n" + combined_content,
        entry.chapter_number,
        {
            "title": effective_title,
            "parts": len(parts),
            "chars": len(combined_content),
            "duration_seconds": duration_seconds,
            "final_url": last_result.final_url if last_result else entry.url,
            "strategy": last_result.strategy_name if last_result else "",
        },
    )


def resolve_directory_entries(
    config: NovelConfig,
    directory_url: str | None = None,
    *,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
    fetch_all_pages: bool = False,
    log_exceptions: bool = True,
    proxy_session_state: CrawlProxySessionState | None = None,
) -> dict[int, ChapterEntry]:
    registry = build_default_registry()
    resolver = registry.get(config.source.resolver_id)
    strategy_chain = build_strategy_chain(
        config.crawl,
        config.crawl.browser_debug,
        proxy_gateway=config.proxy_gateway,
        redis_cfg=config.queue.redis,
        proxy_session_state=proxy_session_state,
    )
    dir_url = directory_url or config.crawl.directory_url

    LOGGER.info(
        "Resolving directory entries | novel=%s source=%s directory=%s range=%s-%s fetch_all=%s",
        config.novel_id,
        config.crawl.site_id,
        dir_url,
        from_chapter,
        to_chapter,
        fetch_all_pages,
    )
    try:
        directory_result = strategy_chain.fetch(dir_url, config.crawl.request_timeout_seconds)
        entries = resolver.parse_directory(directory_result.html, directory_result.final_url)
        seen_directory_urls = {directory_result.final_url}
        pending_directory_urls = resolver.find_directory_page_urls(directory_result.html, directory_result.final_url)
        while pending_directory_urls:
            if (
                (not fetch_all_pages)
                and from_chapter is not None
                and to_chapter is not None
                and _has_requested_chapters(entries, from_chapter, to_chapter)
            ):
                break
            page_url = pending_directory_urls.pop(0)
            if page_url in seen_directory_urls:
                continue
            seen_directory_urls.add(page_url)
            page_result = strategy_chain.fetch(page_url, config.crawl.request_timeout_seconds)
            entries.update(resolver.parse_directory(page_result.html, page_result.final_url))
            for extra_url in resolver.find_directory_page_urls(page_result.html, page_result.final_url):
                if extra_url not in seen_directory_urls and extra_url not in pending_directory_urls:
                    pending_directory_urls.append(extra_url)
    except Exception:
        if log_exceptions:
            LOGGER.exception(
                "Directory crawl failed | novel=%s source=%s directory=%s",
                config.novel_id,
                config.crawl.site_id,
                dir_url,
            )
        else:
            LOGGER.warning(
                "Directory crawl failed | novel=%s source=%s directory=%s",
                config.novel_id,
                config.crawl.site_id,
                dir_url,
            )
        raise
    return entries


def config_with_source(config: NovelConfig, source_config: SourceConfig) -> NovelConfig:
    return replace(
        config,
        source_id=source_config.source_id,
        source=source_config,
        crawl=source_config.crawl,
    )


def discover_source_entries(
    config: NovelConfig,
    source_config: SourceConfig,
    *,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
    fetch_all_pages: bool = False,
    log_exceptions: bool = True,
    proxy_session_state: CrawlProxySessionState | None = None,
) -> SourceDiscoveryResult | None:
    source_bound_config = config_with_source(config, source_config)
    entries = resolve_directory_entries(
        source_bound_config,
        from_chapter=from_chapter,
        to_chapter=to_chapter,
        fetch_all_pages=fetch_all_pages,
        log_exceptions=log_exceptions,
        proxy_session_state=proxy_session_state,
    )
    latest_chapter = max((int(chapter) for chapter in entries), default=0)
    return SourceDiscoveryResult(
        source_config=source_config,
        entries=entries,
        latest_chapter=latest_chapter,
    )


def crawl_range(
    config: NovelConfig,
    from_chapter: int,
    to_chapter: int,
    directory_url: str | None = None,
    *,
    force: bool = False,
    prune_failure_manifest: bool = True,
    source_configs: list[SourceConfig] | None = None,
) -> list[Path]:
    run_started_at = time.time()
    manifest = _load_failure_manifest(config)
    registry = build_default_registry()
    proxy_session_state = CrawlProxySessionState()
    source_candidates = list(source_configs or [config.source])
    discovery_results: list[SourceDiscoveryResult] = []
    for source_cfg in source_candidates:
        try:
            result = discover_source_entries(
                config,
                source_cfg,
                from_chapter=from_chapter,
                to_chapter=to_chapter,
                fetch_all_pages=False,
                log_exceptions=True,
                proxy_session_state=proxy_session_state,
            )
        except Exception:
            continue
        if result is not None:
            discovery_results.append(result)
    if not discovery_results:
        raise RuntimeError(f"Unable to build chapter map for {config.novel_id}")
    discovery_results.sort(key=lambda item: item.latest_chapter, reverse=True)
    primary_discovery = discovery_results[0]
    active_config = config_with_source(config, primary_discovery.source_config)
    resolver = registry.get(active_config.source.resolver_id)
    dir_url = directory_url or active_config.crawl.directory_url
    existing_chapters = set()
    if not force:
        existing_chapters = _existing_origin_chapter_numbers(
            origin_dir=config.storage.origin_dir,
            chapter_regex=config.translation.chapter_regex,
            from_chapter=from_chapter,
            to_chapter=to_chapter,
        )
    LOGGER.info(
        "Starting crawl | novel=%s source=%s range=%s-%s directory=%s force=%s existing_in_range=%s",
        active_config.novel_id,
        active_config.crawl.site_id,
        from_chapter,
        to_chapter,
        dir_url,
        force,
        len(existing_chapters),
    )
    chapter_sources: dict[int, list[tuple[SourceConfig, ChapterEntry]]] = {}
    chapter_map: dict[int, ChapterEntry] = {}
    for result in discovery_results:
        for chapter_number, entry in sorted(result.entries.items()):
            chapter_sources.setdefault(chapter_number, []).append((result.source_config, entry))
            chapter_map.setdefault(chapter_number, entry)
    if chapter_map:
        LOGGER.info("Resolved %s chapters from %s source(s)", len(chapter_map), len(discovery_results))
    elif active_config.crawl.chapter_url_pattern:
        for chapter_number in range(from_chapter, to_chapter + 1):
            chapter_map[chapter_number] = ChapterEntry(
                chapter_number=chapter_number,
                title=f"第{chapter_number}章",
                url=active_config.crawl.chapter_url_pattern.format(chapter=chapter_number),
            )
        LOGGER.warning("Directory parser returned no entries, using chapter_url_pattern fallback")
    else:
        raise RuntimeError(f"Unable to build chapter map for {config.novel_id}")

    outputs: list[Path] = []
    total_success = 0
    total_failed = 0
    total_skipped = 0
    failed_chapters: list[int] = []
    batch_size = max(1, config.crawl.chapter_batch_size)
    aligned_start = ((from_chapter - 1) // batch_size) * batch_size + 1
    for batch_start in range(aligned_start, to_chapter + 1, batch_size):
        batch_end = batch_start + batch_size - 1
        fetch_start = max(batch_start, from_chapter)
        fetch_end = min(batch_end, to_chapter)
        blocks: list[str] = []
        fetched_numbers: list[int] = []
        batch_success = 0
        batch_failed = 0
        batch_skipped = 0
        LOGGER.info(
            "Batch start | novel=%s source=%s batch=%s-%s batch_size=%s",
            active_config.novel_id,
            active_config.crawl.site_id,
            batch_start,
            batch_end,
            batch_size,
        )
        for chapter_number in range(fetch_start, fetch_end + 1):
            if (not force) and chapter_number in existing_chapters:
                batch_skipped += 1
                total_skipped += 1
                LOGGER.info(
                    "Skipping existing chapter | novel=%s source=%s chapter=%s batch=%s-%s",
                    active_config.novel_id,
                    active_config.crawl.site_id,
                    chapter_number,
                    batch_start,
                    batch_end,
                )
                continue
            candidates = chapter_sources.get(chapter_number, [])
            if not candidates and chapter_number in chapter_map:
                candidates = [(active_config.source, chapter_map[chapter_number])]
            if not candidates:
                LOGGER.warning("Skipping chapter %s: missing entry", chapter_number)
                batch_failed += 1
                total_failed += 1
                failed_chapters.append(chapter_number)
                _record_failure(
                    active_config,
                    manifest,
                    chapter_number=chapter_number,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    url="",
                    reason="missing_entry",
                    details="Directory parser did not return an entry for this chapter",
                )
                continue
            block = ""
            parsed_number = chapter_number
            stats: dict[str, object] = {}
            last_exc: Exception | None = None
            source_used = active_config
            for candidate_source, entry in candidates:
                candidate_config = config_with_source(config, candidate_source)
                candidate_resolver = registry.get(candidate_config.source.resolver_id)
                candidate_strategy_chain = build_strategy_chain(
                    candidate_config.crawl,
                    candidate_config.crawl.browser_debug,
                    proxy_gateway=candidate_config.proxy_gateway,
                    redis_cfg=candidate_config.queue.redis,
                    proxy_session_state=proxy_session_state,
                )
                try:
                    block, parsed_number, stats = _fetch_chapter(
                        entry,
                        candidate_config,
                        candidate_resolver,
                        candidate_strategy_chain,
                    )
                    source_used = candidate_config
                    break
                except Exception as exc:
                    last_exc = exc
                    LOGGER.warning(
                        "Chapter crawl fallback | novel=%s chapter=%s source=%s failed=%s",
                        config.novel_id,
                        chapter_number,
                        candidate_source.source_id,
                        exc,
                    )
                    continue
            if not block.strip():
                exc = last_exc or RuntimeError("Unable to fetch chapter from all sources")
                batch_failed += 1
                total_failed += 1
                failed_chapters.append(chapter_number)
                _record_failure(
                    active_config,
                    manifest,
                    chapter_number=chapter_number,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    url=candidates[0][1].url if candidates else "",
                    reason=exc.__class__.__name__,
                    details=str(exc),
                )
                continue
            if block.strip():
                blocks.append(block.strip())
                fetched_numbers.append(parsed_number)
                batch_success += 1
                total_success += 1
                _clear_failure(active_config, manifest, parsed_number)
                LOGGER.info(
                    "Chapter completed | novel=%s source=%s chapter=%s title=%s chars=%s parts=%s duration=%.2fs strategy=%s final_url=%s",
                    source_used.novel_id,
                    source_used.crawl.site_id,
                    parsed_number,
                    stats["title"],
                    stats["chars"],
                    stats["parts"],
                    stats["duration_seconds"],
                    stats["strategy"],
                    stats["final_url"],
                )
            time.sleep(config.crawl.delay_between_chapters_seconds)
        if blocks:
            output_path = _write_merged_batch(
                config.storage.origin_dir,
                batch_start,
                batch_end,
                blocks,
                fetched_numbers,
                config.translation.chapter_regex,
            )
            LOGGER.info(
                "Batch wrote file | novel=%s source=%s batch=%s-%s output=%s chapters=%s success=%s failed=%s skipped=%s",
                config.novel_id,
                config.crawl.site_id,
                batch_start,
                batch_end,
                output_path,
                len(fetched_numbers),
                batch_success,
                batch_failed,
                batch_skipped,
            )
            outputs.append(output_path)
        else:
            LOGGER.warning(
                "Batch wrote no file | novel=%s source=%s batch=%s-%s success=%s failed=%s skipped=%s",
                config.novel_id,
                config.crawl.site_id,
                batch_start,
                batch_end,
                batch_success,
                batch_failed,
                batch_skipped,
            )
        LOGGER.info(
            "Batch finished | novel=%s source=%s batch=%s-%s success=%s failed=%s skipped=%s",
            config.novel_id,
            config.crawl.site_id,
            batch_start,
            batch_end,
            batch_success,
            batch_failed,
            batch_skipped,
        )
    LOGGER.info(
        "Crawl finished | novel=%s source=%s range=%s-%s success=%s failed=%s skipped=%s outputs=%s elapsed=%.2fs failure_manifest=%s failed_chapters=%s",
        config.novel_id,
        config.crawl.site_id,
        from_chapter,
        to_chapter,
        total_success,
        total_failed,
        total_skipped,
        len(outputs),
        time.time() - run_started_at,
        _failure_manifest_path(config),
        failed_chapters,
    )
    if prune_failure_manifest:
        removed, deleted = _prune_failure_manifest_against_origin(config, from_chapter, to_chapter)
        if removed:
            LOGGER.info(
                "Pruned stale failure manifest entries | novel=%s source=%s range=%s-%s removed=%s deleted=%s path=%s",
                config.novel_id,
                config.crawl.site_id,
                from_chapter,
                to_chapter,
                removed,
                deleted,
                _failure_manifest_path(config),
            )
    return outputs
