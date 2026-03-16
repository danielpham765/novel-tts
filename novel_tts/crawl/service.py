from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.common.text import normalize_whitespace
from novel_tts.config.models import NovelConfig

from .registry import build_default_registry
from .strategies import build_strategy_chain
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
MIN_CONTENT_CHARS = 80
BATCH_FILENAME_PATTERN = re.compile(r"chuong_(\d+)-(\d+)\.txt$")


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

    @property
    def ok(self) -> bool:
        return not self.issues


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
    if len(normalized_content) < MIN_CONTENT_CHARS:
        return f"content_too_short:{len(normalized_content)}"
    for token in INVALID_CONTENT_TOKENS:
        if token in normalized_content:
            return f"invalid_content_token:{token}"
    return None


def _write_batch(origin_dir: Path, start_chapter: int, end_chapter: int, blocks: list[str]) -> Path:
    origin_dir.mkdir(parents=True, exist_ok=True)
    output_path = origin_dir / f"chuong_{start_chapter}-{end_chapter}.txt"
    output_path.write_text("\n\n\n".join(blocks).strip() + "\n", encoding="utf-8")
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


def verify_crawled_content(
    config: NovelConfig,
    from_chapter: int | None = None,
    to_chapter: int | None = None,
    filenames: list[str] | None = None,
) -> CrawlVerifyReport:
    checked_files: list[Path] = []
    checked_chapters: list[int] = []
    issues: list[CrawlVerifyIssue] = []
    seen_chapters: dict[int, Path] = {}
    valid_chapters: set[int] = set()

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
        return CrawlVerifyReport(checked_files=checked_files, checked_chapters=checked_chapters, issues=issues)

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
            for missing in sorted(expected_numbers - actual_numbers):
                issues.append(
                    CrawlVerifyIssue(
                        code="missing_chapter_in_batch",
                        message=f"File {batch_file.name} thieu chuong {missing}",
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

    manifest = _load_failure_manifest(config)
    failures = manifest.get("failures", {})
    if isinstance(failures, dict):
        for raw_key, payload in sorted(failures.items(), key=lambda item: int(item[0])):
            chapter_number = int(raw_key)
            if from_chapter is not None and chapter_number < from_chapter:
                continue
            if to_chapter is not None and chapter_number > to_chapter:
                continue
            reason = payload.get("reason", "unknown") if isinstance(payload, dict) else "unknown"
            details = payload.get("details", "") if isinstance(payload, dict) else ""
            code = "stale_manifest" if chapter_number in valid_chapters else "failure_manifest_entry"
            issues.append(
                CrawlVerifyIssue(
                    code=code,
                    message=f"Manifest bao loi chuong {chapter_number}: {reason} {details}".strip(),
                    chapter_number=chapter_number,
                    path=_failure_manifest_path(config),
                )
            )

    checked_chapters = sorted(set(checked_chapters))
    issues.sort(key=lambda issue: (issue.chapter_number is None, issue.chapter_number or 0, issue.code, issue.message))
    return CrawlVerifyReport(checked_files=checked_files, checked_chapters=checked_chapters, issues=issues)


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


def crawl_range(config: NovelConfig, from_chapter: int, to_chapter: int, directory_url: str | None = None) -> list[Path]:
    run_started_at = time.time()
    manifest = _load_failure_manifest(config)
    registry = build_default_registry()
    resolver = registry.get(config.source.resolver_id)
    strategy_chain = build_strategy_chain(config.crawl, config.browser_debug)
    dir_url = directory_url or config.crawl.directory_url
    LOGGER.info(
        "Starting crawl | novel=%s source=%s range=%s-%s directory=%s",
        config.novel_id,
        config.crawl.site_id,
        from_chapter,
        to_chapter,
        dir_url,
    )
    try:
        directory_result = strategy_chain.fetch(dir_url, config.crawl.request_timeout_seconds)
        entries = resolver.parse_directory(directory_result.html, directory_result.final_url)
        seen_directory_urls = {directory_result.final_url}
        pending_directory_urls = resolver.find_directory_page_urls(directory_result.html, directory_result.final_url)
        while pending_directory_urls and not _has_requested_chapters(entries, from_chapter, to_chapter):
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
        LOGGER.exception(
            "Directory crawl failed | novel=%s source=%s directory=%s",
            config.novel_id,
            config.crawl.site_id,
            dir_url,
        )
        raise

    chapter_map: dict[int, ChapterEntry] = {}
    if entries:
        chapter_map.update(entries)
        LOGGER.info("Resolved %s chapters from directory", len(chapter_map))
    elif config.crawl.chapter_url_pattern:
        for chapter_number in range(from_chapter, to_chapter + 1):
            chapter_map[chapter_number] = ChapterEntry(
                chapter_number=chapter_number,
                title=f"第{chapter_number}章",
                url=config.crawl.chapter_url_pattern.format(chapter=chapter_number),
            )
        LOGGER.warning("Directory parser returned no entries, using chapter_url_pattern fallback")
    else:
        raise RuntimeError(f"Unable to build chapter map for {config.novel_id}")

    outputs: list[Path] = []
    total_success = 0
    total_failed = 0
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
        LOGGER.info(
            "Batch start | novel=%s source=%s batch=%s-%s batch_size=%s",
            config.novel_id,
            config.crawl.site_id,
            batch_start,
            batch_end,
            batch_size,
        )
        for chapter_number in range(fetch_start, fetch_end + 1):
            entry = chapter_map.get(chapter_number)
            if not entry:
                LOGGER.warning("Skipping chapter %s: missing entry", chapter_number)
                batch_failed += 1
                total_failed += 1
                failed_chapters.append(chapter_number)
                _record_failure(
                    config,
                    manifest,
                    chapter_number=chapter_number,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    url="",
                    reason="missing_entry",
                    details="Directory parser did not return an entry for this chapter",
                )
                continue
            try:
                block, parsed_number, stats = _fetch_chapter(entry, config, resolver, strategy_chain)
            except Exception as exc:
                LOGGER.exception(
                    "Chapter crawl failed | novel=%s source=%s chapter=%s batch=%s-%s",
                    config.novel_id,
                    config.crawl.site_id,
                    chapter_number,
                    batch_start,
                    batch_end,
                )
                batch_failed += 1
                total_failed += 1
                failed_chapters.append(chapter_number)
                _record_failure(
                    config,
                    manifest,
                    chapter_number=chapter_number,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    url=entry.url,
                    reason=exc.__class__.__name__,
                    details=str(exc),
                )
                continue
            if block.strip():
                blocks.append(block.strip())
                fetched_numbers.append(parsed_number)
                batch_success += 1
                total_success += 1
                _clear_failure(config, manifest, parsed_number)
                LOGGER.info(
                    "Chapter completed | novel=%s source=%s chapter=%s title=%s chars=%s parts=%s duration=%.2fs strategy=%s final_url=%s",
                    config.novel_id,
                    config.crawl.site_id,
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
            output_path = _write_batch(config.storage.origin_dir, batch_start, batch_end, blocks)
            LOGGER.info(
                "Batch wrote file | novel=%s source=%s batch=%s-%s output=%s chapters=%s success=%s failed=%s",
                config.novel_id,
                config.crawl.site_id,
                batch_start,
                batch_end,
                output_path,
                len(fetched_numbers),
                batch_success,
                batch_failed,
            )
            outputs.append(output_path)
        else:
            LOGGER.warning(
                "Batch wrote no file | novel=%s source=%s batch=%s-%s success=%s failed=%s",
                config.novel_id,
                config.crawl.site_id,
                batch_start,
                batch_end,
                batch_success,
                batch_failed,
            )
        LOGGER.info(
            "Batch finished | novel=%s source=%s batch=%s-%s success=%s failed=%s",
            config.novel_id,
            config.crawl.site_id,
            batch_start,
            batch_end,
            batch_success,
            batch_failed,
        )
    LOGGER.info(
        "Crawl finished | novel=%s source=%s range=%s-%s success=%s failed=%s outputs=%s elapsed=%.2fs failure_manifest=%s failed_chapters=%s",
        config.novel_id,
        config.crawl.site_id,
        from_chapter,
        to_chapter,
        total_success,
        total_failed,
        len(outputs),
        time.time() - run_started_at,
        _failure_manifest_path(config),
        failed_chapters,
    )
    return outputs
