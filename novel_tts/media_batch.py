from __future__ import annotations

from dataclasses import dataclass

from novel_tts.common.text import parse_range
from novel_tts.config.models import MediaBatchConfig, NovelConfig


@dataclass(frozen=True)
class MediaBatchResolvedRule:
    start: int
    end: int
    chapter_batch_size: int


@dataclass(frozen=True)
class MediaBatchRange:
    start: int
    end: int
    range_key: str
    episode_index: int
    chapter_batch_size: int


def media_range_key(start: int, end: int) -> str:
    return f"chuong_{int(start)}-{int(end)}"


def resolve_media_batch_rules(config: NovelConfig | MediaBatchConfig) -> list[MediaBatchResolvedRule]:
    media_batch = config.media.media_batch if isinstance(config, NovelConfig) else config
    rules: list[MediaBatchResolvedRule] = []
    for raw_rule in media_batch.chapter_batch_overrides:
        start, end = parse_range(raw_rule.range)
        rules.append(
            MediaBatchResolvedRule(
                start=start,
                end=end,
                chapter_batch_size=max(1, int(raw_rule.chapter_batch_size)),
            )
        )
    rules.sort(key=lambda item: (item.start, item.end))
    return rules


def find_media_batch_size(config: NovelConfig | MediaBatchConfig, chapter: int) -> int:
    safe_chapter = int(chapter)
    if safe_chapter < 1:
        raise ValueError("chapter must be >= 1")
    media_batch = config.media.media_batch if isinstance(config, NovelConfig) else config
    for rule in resolve_media_batch_rules(media_batch):
        if rule.start <= safe_chapter <= rule.end:
            return rule.chapter_batch_size
    return max(1, int(media_batch.default_chapter_batch_size))


def get_media_batch_range(config: NovelConfig | MediaBatchConfig, chapter: int) -> tuple[int, int]:
    safe_chapter = int(chapter)
    if safe_chapter < 1:
        raise ValueError("chapter must be >= 1")
    for rule in resolve_media_batch_rules(config):
        if rule.start <= safe_chapter <= rule.end:
            size = rule.chapter_batch_size
            start = ((safe_chapter - rule.start) // size) * size + rule.start
            end = min(rule.end, start + size - 1)
            return start, end
    size = find_media_batch_size(config, safe_chapter)
    start = ((safe_chapter - 1) // size) * size + 1
    end = start + size - 1
    return start, end


def collect_media_batch_ranges(config: NovelConfig | MediaBatchConfig, start: int, end: int) -> list[MediaBatchRange]:
    safe_start = int(start)
    safe_end = int(end)
    if safe_start > safe_end:
        safe_start, safe_end = safe_end, safe_start
    ranges: list[MediaBatchRange] = []
    seen: set[tuple[int, int]] = set()
    chapter = safe_start
    while chapter <= safe_end:
        batch_start, batch_end = get_media_batch_range(config, chapter)
        key = (batch_start, batch_end)
        if key not in seen:
            seen.add(key)
            ranges.append(
                MediaBatchRange(
                    start=batch_start,
                    end=batch_end,
                    range_key=media_range_key(batch_start, batch_end),
                    episode_index=0,
                    chapter_batch_size=batch_end - batch_start + 1,
                )
            )
        chapter = batch_end + 1
    return _assign_episode_indexes(config, ranges)


def _assign_episode_indexes(
    config: NovelConfig | MediaBatchConfig,
    ranges: list[MediaBatchRange],
) -> list[MediaBatchRange]:
    if not ranges:
        return []
    indexed: list[MediaBatchRange] = []
    for item in ranges:
        episode_index = count_media_batches_before(config, item.start) + 1
        indexed.append(
            MediaBatchRange(
                start=item.start,
                end=item.end,
                range_key=item.range_key,
                episode_index=episode_index,
                chapter_batch_size=item.chapter_batch_size,
            )
        )
    return indexed


def count_media_batches_before(config: NovelConfig | MediaBatchConfig, chapter: int) -> int:
    safe_chapter = int(chapter)
    if safe_chapter <= 1:
        return 0
    media_batch = config.media.media_batch if isinstance(config, NovelConfig) else config
    default_size = max(1, int(media_batch.default_chapter_batch_size))
    rules = resolve_media_batch_rules(media_batch)
    total = 0
    previous_end = 0
    for rule in rules:
        if rule.start > previous_end + 1:
            gap_start = previous_end + 1
            gap_end = rule.start - 1
            total += _count_batches_in_segment(gap_start, gap_end, default_size, safe_chapter)
        total += _count_batches_in_segment(rule.start, rule.end, rule.chapter_batch_size, safe_chapter)
        previous_end = max(previous_end, rule.end)
    total += _count_batches_in_segment(previous_end + 1, safe_chapter - 1, default_size, safe_chapter)
    return total


def _count_batches_in_segment(segment_start: int, segment_end: int, size: int, limit_chapter: int) -> int:
    if segment_start > segment_end or segment_start >= limit_chapter:
        return 0
    effective_end = min(segment_end, limit_chapter - 1)
    if effective_end < segment_start:
        return 0
    return ((effective_end - segment_start) // size) + 1


def find_media_range_by_episode(config: NovelConfig | MediaBatchConfig, episode_index: int) -> MediaBatchRange | None:
    safe_index = int(episode_index)
    if safe_index < 1:
        return None
    media_batch = config.media.media_batch if isinstance(config, NovelConfig) else config
    default_size = max(1, int(media_batch.default_chapter_batch_size))
    rules = resolve_media_batch_rules(media_batch)
    episode_cursor = 0
    previous_end = 0
    for rule in rules:
        if rule.start > previous_end + 1:
            gap = _find_batch_in_segment(previous_end + 1, rule.start - 1, default_size, safe_index, episode_cursor)
            if gap is not None:
                return gap
            episode_cursor += _count_batches_in_segment(previous_end + 1, rule.start - 1, default_size, 10**18)
        segment = _find_batch_in_segment(rule.start, rule.end, rule.chapter_batch_size, safe_index, episode_cursor)
        if segment is not None:
            return segment
        episode_cursor += _count_batches_in_segment(rule.start, rule.end, rule.chapter_batch_size, 10**18)
        previous_end = max(previous_end, rule.end)
    return _find_batch_in_segment(previous_end + 1, None, default_size, safe_index, episode_cursor)


def _find_batch_in_segment(
    segment_start: int,
    segment_end: int | None,
    size: int,
    target_episode: int,
    episode_cursor: int,
) -> MediaBatchRange | None:
    if segment_end is not None and segment_start > segment_end:
        return None
    if size < 1:
        raise ValueError("chapter_batch_size must be >= 1")
    if segment_end is None:
        batches_available = target_episode - episode_cursor
    else:
        batches_available = ((segment_end - segment_start) // size) + 1
    if target_episode > episode_cursor + batches_available:
        return None
    offset = target_episode - episode_cursor - 1
    start = segment_start + offset * size
    end = start + size - 1 if segment_end is None else min(segment_end, start + size - 1)
    return MediaBatchRange(
        start=start,
        end=end,
        range_key=media_range_key(start, end),
        episode_index=target_episode,
        chapter_batch_size=end - start + 1,
    )
