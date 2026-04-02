from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config import NovelConfig, load_novel_config, load_novel_source_configs
from novel_tts.config.loader import _load_app_config
from novel_tts.crawl import crawl_range
from novel_tts.crawl.service import config_with_source, discover_source_entries
from novel_tts.media import create_video, generate_visual
from novel_tts.media_batch import collect_media_batch_ranges, media_range_key
from novel_tts.queue import add_jobs_to_queue, launch_queue_stack, wait_for_range_completion
from novel_tts.translate import polish_translations
from novel_tts.translate.repair import enqueue_repair_jobs, find_repair_jobs_in_range
from novel_tts.tts import run_tts
from novel_tts.upload import run_uploads
from novel_tts.config.models import SourceConfig

LOGGER = get_logger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _discover_configured_novel_ids(repo_root: Path) -> list[str]:
    try:
        app_raw = _load_app_config()
    except Exception:
        app_raw = {}
    pipeline_raw = app_raw.get("pipeline", {}) or {}
    watch_raw = pipeline_raw.get("watch", {}) or {}
    novels_raw = watch_raw.get("novels", []) or []
    configured = sorted({str(item).strip() for item in novels_raw if str(item).strip()})
    if configured:
        return configured

    novels_dir = repo_root / "configs" / "novels"
    if not novels_dir.exists():
        return []
    return sorted(path.stem for path in novels_dir.glob("*.yaml") if path.is_file())


def _watch_state_path(config: NovelConfig) -> Path:
    return config.storage.progress_dir / "watch_pipeline_state.json"


def _load_watch_state(config: NovelConfig) -> dict[str, object]:
    path = _watch_state_path(config)
    if not path.exists():
        return {"media_completed_ranges": {}, "updated_at": ""}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"media_completed_ranges": {}, "updated_at": ""}
    if not isinstance(raw, dict):
        return {"media_completed_ranges": {}, "updated_at": ""}
    media_completed = raw.get("media_completed_ranges", {})
    if not isinstance(media_completed, dict):
        media_completed = {}
    return {
        "media_completed_ranges": media_completed,
        "updated_at": str(raw.get("updated_at", "") or ""),
    }


def _save_watch_state(config: NovelConfig, state: dict[str, object]) -> None:
    path = _watch_state_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "media_completed_ranges": state.get("media_completed_ranges", {}),
        "updated_at": _utc_now_iso(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _batch_range_for_chapter(chapter: int, batch_size: int) -> tuple[int, int]:
    safe_batch_size = max(1, int(batch_size))
    start = ((int(chapter) - 1) // safe_batch_size) * safe_batch_size + 1
    end = start + safe_batch_size - 1
    return start, end


def _range_key(start: int, end: int) -> str:
    return media_range_key(start, end)


def _translated_range_path(config: NovelConfig, start: int, end: int) -> Path:
    return config.storage.translated_dir / f"{_range_key(start, end)}.txt"


def _audio_parts_count(config: NovelConfig, start: int, end: int) -> int:
    parts_dir = config.storage.audio_dir / _range_key(start, end) / ".parts"
    if not parts_dir.exists():
        return 0
    return sum(1 for path in parts_dir.glob("chapter_*.wav") if path.is_file())


def _count_crawled_chapters(config: NovelConfig, path: Path) -> int:
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return 0
    count = 0
    for match in re.finditer(config.translation.chapter_regex, raw, flags=re.M):
        try:
            count = max(count, int(match.group(1)))
        except Exception:
            continue
    return count


def _discover_local_latest_chapter(config: NovelConfig) -> int:
    if not config.storage.origin_dir.exists():
        return 0
    latest = 0
    for path in sorted(config.storage.origin_dir.glob("chuong_*.txt")):
        if not path.is_file():
            continue
        latest = max(latest, _count_crawled_chapters(config, path))
    return latest


def _discover_best_remote_source(
    config: NovelConfig,
    source_configs: list[SourceConfig],
) -> tuple[int | None, SourceConfig | None]:
    best_latest: int | None = None
    best_source: SourceConfig | None = None
    summary_rows: list[dict[str, object]] = []
    for source_config in source_configs:
        try:
            result = discover_source_entries(
                config,
                source_config,
                fetch_all_pages=True,
                log_exceptions=False,
            )
        except Exception as exc:
            LOGGER.warning(
                "Watch remote discovery skipped | novel=%s source=%s reason=%s",
                config.novel_id,
                source_config.source_id,
                exc,
            )
            summary_rows.append(
                {
                    "source": source_config.source_id,
                    "latest": "-",
                    "entries": "-",
                    "status": f"error:{exc}",
                }
            )
            continue
        if result is None or not result.entries:
            LOGGER.warning(
                "Watch remote discovery returned no chapter entries | novel=%s source=%s",
                config.novel_id,
                source_config.source_id,
            )
            summary_rows.append(
                {
                    "source": source_config.source_id,
                    "latest": "-",
                    "entries": 0,
                    "status": "no-entries",
                }
            )
            continue
        LOGGER.info(
            "Watch remote discovery | novel=%s source=%s latest=%s entries=%s",
            config.novel_id,
            source_config.source_id,
            result.latest_chapter,
            len(result.entries),
        )
        summary_rows.append(
            {
                "source": source_config.source_id,
                "latest": result.latest_chapter,
                "entries": len(result.entries),
                "status": "ok",
            }
        )
        if best_latest is None or result.latest_chapter > best_latest:
            best_latest = result.latest_chapter
            best_source = source_config
    if summary_rows:
        lines = []
        for row in summary_rows:
            selected = "yes" if best_source is not None and row["source"] == best_source.source_id else "no"
            lines.append(
                "source={source} latest={latest} entries={entries} selected={selected} status={status}".format(
                    source=row["source"],
                    latest=row["latest"],
                    entries=row["entries"],
                    selected=selected,
                    status=row["status"],
                )
            )
        LOGGER.info("Watch source summary | novel=%s\n%s", config.novel_id, "\n".join(lines))
    return best_latest, best_source


def _collect_ranges_for_span(start: int, end: int, batch_size: int) -> list[tuple[int, int]]:
    if start > end:
        start, end = end, start
    ranges: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for chapter in range(start, end + 1):
        item = _batch_range_for_chapter(chapter, batch_size)
        if item in seen:
            continue
        seen.add(item)
        ranges.append(item)
    return ranges


def _polish_range(config: NovelConfig, start: int, end: int) -> tuple[int, int]:
    filenames: list[str] = []
    for batch_start, batch_end in _collect_ranges_for_span(start, end, config.crawl.chapter_batch_size):
        translated_path = _translated_range_path(config, batch_start, batch_end)
        if translated_path.exists():
            filenames.append(translated_path.name)
    if not filenames:
        return 0, 0
    return polish_translations(config, filenames=filenames)


def _run_media_if_ready(
    config: NovelConfig,
    *,
    start: int,
    end: int,
    state: dict[str, object],
    upload_platform: str,
    skip_visual: bool,
    skip_video: bool,
    skip_upload: bool,
) -> bool:
    expected_parts = max(1, end - start + 1)
    available_parts = _audio_parts_count(config, start, end)
    range_key = _range_key(start, end)
    if available_parts < expected_parts:
        LOGGER.info(
            "Watch media gated | novel=%s range=%s parts=%s/%s",
            config.novel_id,
            range_key,
            available_parts,
            expected_parts,
        )
        return False

    if skip_visual and skip_video and skip_upload:
        LOGGER.info("Watch media skipped by flags | novel=%s range=%s", config.novel_id, range_key)
        return False

    media_completed = state.setdefault("media_completed_ranges", {})
    if not isinstance(media_completed, dict):
        media_completed = {}
        state["media_completed_ranges"] = media_completed
    already_completed = str(media_completed.get(range_key, "")).strip()
    if already_completed and not (skip_visual or skip_video or skip_upload):
        LOGGER.info("Watch media already completed | novel=%s range=%s", config.novel_id, range_key)
        return False

    if not skip_visual:
        generate_visual(config, start, end)
    if not skip_video:
        create_video(config, start, end)
    if not skip_upload:
        run_uploads(config, [(start, end)], platform=upload_platform, dry_run=False)
    if not (skip_visual or skip_video or skip_upload):
        media_completed[range_key] = _utc_now_iso()
        _save_watch_state(config, state)
    LOGGER.info("Watch media completed | novel=%s range=%s", config.novel_id, range_key)
    return True


def _process_novel(
    config: NovelConfig,
    *,
    upload_platform_override: str | None,
    restart_queue: bool | None,
    bootstrap_from: int | None,
    skip_crawl: bool,
    skip_translate: bool,
    skip_repair: bool,
    skip_polish: bool,
    skip_tts: bool,
    skip_create_menu: bool,
    skip_visual: bool,
    skip_video: bool,
    skip_upload: bool,
) -> bool:
    state = _load_watch_state(config)
    source_configs = load_novel_source_configs(config.novel_id)
    remote_latest, selected_source = _discover_best_remote_source(config, source_configs)
    if remote_latest is None or selected_source is None:
        LOGGER.warning("Watch novel skipped | novel=%s reason=remote-discovery-unavailable", config.novel_id)
        return False
    source_bound_config = config_with_source(config, selected_source)
    LOGGER.info(
        "Watch selected source | novel=%s selected_source=%s remote_latest=%s",
        config.novel_id,
        selected_source.source_id,
        remote_latest,
    )
    local_latest = _discover_local_latest_chapter(config)
    batch_size = max(1, int(config.crawl.chapter_batch_size))
    watch_cfg = config.pipeline.watch
    upload_platform = str(
        upload_platform_override
        or getattr(watch_cfg, "upload_platform", "")
        or getattr(config.upload, "default_platform", "youtube")
        or "youtube"
    )
    effective_restart_queue = (
        bool(restart_queue) if restart_queue is not None else bool(getattr(watch_cfg, "restart_queue", False))
    )
    effective_bootstrap_from = bootstrap_from
    if effective_bootstrap_from is None:
        configured_bootstrap = int(getattr(watch_cfg, "bootstrap_from", 0) or 0)
        effective_bootstrap_from = configured_bootstrap if configured_bootstrap > 0 else None
    changed = False

    LOGGER.info(
        "Watch scan | novel=%s local_latest=%s remote_latest=%s batch_size=%s",
        config.novel_id,
        local_latest,
        remote_latest,
        batch_size,
    )

    crawl_start: int | None = None
    crawl_end: int | None = None
    if (not skip_crawl) and remote_latest > local_latest:
        if local_latest <= 0 and effective_bootstrap_from is None:
            LOGGER.warning(
                "Watch bootstrap skipped | novel=%s remote_latest=%s reason=no-local-chapters",
                config.novel_id,
                remote_latest,
            )
        else:
            crawl_start = (
                int(effective_bootstrap_from)
                if local_latest <= 0 and effective_bootstrap_from is not None
                else (local_latest + 1)
            )
            crawl_end = remote_latest

    tts_ranges: list[tuple[int, int]] = []
    if crawl_start is not None and crawl_end is not None and crawl_start <= crawl_end:
        LOGGER.info("Watch crawl start | novel=%s range=%s-%s", config.novel_id, crawl_start, crawl_end)
        crawl_range(source_bound_config, crawl_start, crawl_end, source_configs=source_configs)
        if not skip_translate:
            launch_queue_stack(source_bound_config, restart=effective_restart_queue, add_queue=False)
            add_jobs_to_queue(source_bound_config, crawl_start, crawl_end)
            wait_for_range_completion(source_bound_config, crawl_start, crawl_end)
        if not skip_repair:
            repair_jobs = find_repair_jobs_in_range(source_bound_config, crawl_start, crawl_end)
            if repair_jobs:
                enqueue_repair_jobs(source_bound_config, repair_jobs, label="watch repair")
                wait_for_range_completion(source_bound_config, crawl_start, crawl_end)

        if not skip_polish:
            _polish_range(source_bound_config, crawl_start, crawl_end)
        tts_ranges.extend([(item.start, item.end) for item in collect_media_batch_ranges(config, crawl_start, crawl_end)])
        changed = True

    latest_known = max(local_latest, remote_latest)
    if latest_known > 0:
        latest_item = collect_media_batch_ranges(config, latest_known, latest_known)[0]
        latest_range = (latest_item.start, latest_item.end)
        if latest_range not in tts_ranges:
            tts_ranges.append(latest_range)

    for start, end in tts_ranges:
        translated_path = _translated_range_path(config, start, end)
        if not translated_path.exists():
            LOGGER.info(
                "Watch tts skipped | novel=%s range=%s missing=%s",
                config.novel_id,
                _range_key(start, end),
                translated_path,
            )
            continue
        if not skip_tts:
            run_tts(source_bound_config, start, end, range_key=_range_key(start, end))
            changed = True
        if not skip_create_menu:
            from novel_tts.tts import create_menu
            create_menu(source_bound_config, start, end, range_key=_range_key(start, end))
            changed = True
        media_changed = _run_media_if_ready(
            source_bound_config,
            start=start,
            end=end,
            state=state,
            upload_platform=upload_platform,
            skip_visual=skip_visual,
            skip_video=skip_video,
            skip_upload=skip_upload,
        )
        changed = media_changed or changed

    if tts_ranges:
        _save_watch_state(config, state)
    return changed


def run_watch_pipeline(
    *,
    repo_root: Path,
    novel_ids: list[str],
    watch_all: bool,
    interval_seconds: float | None,
    once: bool,
    upload_platform_override: str | None,
    restart_queue: bool | None,
    bootstrap_from: int | None,
    skip_crawl: bool,
    skip_translate: bool,
    skip_repair: bool,
    skip_polish: bool,
    skip_tts: bool,
    skip_create_menu: bool,
    skip_visual: bool,
    skip_video: bool,
    skip_upload: bool,
) -> int:
    resolved_ids = sorted({item.strip() for item in novel_ids if item and item.strip()})
    if watch_all:
        resolved_ids = _discover_configured_novel_ids(repo_root)
    if not resolved_ids:
        raise ValueError("pipeline watch requires at least one novel id or --all")

    sleep_seconds: float | None = None
    if interval_seconds is not None:
        sleep_seconds = max(5.0, float(interval_seconds))
    had_error = False

    while True:
        cycle_changed = False
        for novel_id in resolved_ids:
            try:
                config = load_novel_config(novel_id)
                if sleep_seconds is None:
                    sleep_seconds = max(5.0, float(config.pipeline.watch.interval_seconds or 300.0))
                cycle_changed = _process_novel(
                    config,
                    upload_platform_override=upload_platform_override,
                    restart_queue=restart_queue,
                    bootstrap_from=bootstrap_from,
                    skip_crawl=skip_crawl,
                    skip_translate=skip_translate,
                    skip_repair=skip_repair,
                    skip_polish=skip_polish,
                    skip_tts=skip_tts,
                    skip_create_menu=skip_create_menu,
                    skip_visual=skip_visual,
                    skip_video=skip_video,
                    skip_upload=skip_upload,
                ) or cycle_changed
            except Exception:
                had_error = True
                LOGGER.exception("Watch cycle failed | novel=%s", novel_id)

        if once:
            return 1 if had_error else 0

        LOGGER.info(
            "Watch cycle complete | novels=%s changed=%s next_poll_in=%.1fs",
            ",".join(resolved_ids),
            cycle_changed,
            sleep_seconds or 300.0,
        )
        time.sleep(sleep_seconds or 300.0)
