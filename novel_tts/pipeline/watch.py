from __future__ import annotations

import json
import re
import shlex
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from novel_tts.common.logging import get_logger
from novel_tts.config import NovelConfig, load_novel_config, load_novel_source_configs
from novel_tts.config.loader import _load_app_config
from novel_tts.crawl import crawl_range
from novel_tts.crawl.service import config_with_source, discover_source_entries, repair_crawled_content
from novel_tts.media import create_video, generate_visual
from novel_tts.media_batch import collect_media_batch_ranges, media_range_key
from novel_tts.queue import add_chapters_to_queue, launch_queue_stack
from novel_tts.queue.translation_queue import (
    _chapter_needs_work,
    _client,
    _extract_novel_id,
    _inflight_key,
    _novel_key,
    _pending_delayed_key,
    _pending_key,
    _pending_priority_key,
    _queued_key,
    drain_novel_from_queue,
)
from novel_tts.config.loader import load_queue_config
from novel_tts.translate.novel import load_source_chapters
from novel_tts.translate import polish_translations
from novel_tts.translate.repair import enqueue_repair_jobs, find_repair_jobs_in_range
from novel_tts.tts import run_tts
from novel_tts.upload import run_uploads
from novel_tts.config.models import SourceConfig

LOGGER = get_logger(__name__)
QUEUE_PENDING_WAIT_SECONDS = 3600.0
QUEUE_PENDING_MAX_WAITS = 3
QUEUE_REPAIR_CHECK_SECONDS = 300.0
ACTIVE_MEDIA_POLL_SECONDS = 300.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now_ts() -> float:
    return time.time()


def _parse_iso_ts(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return 0.0


def _iso_from_ts(timestamp: float) -> str:
    return datetime.fromtimestamp(float(timestamp), timezone.utc).isoformat()


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
    default_state = {
        "media_completed_ranges": {},
        "queue_generation": 0,
        "active_queue_generation": 0,
        "active_queue_range": {"start": 0, "end": 0},
        "queue_wait_attempts": 0,
        "queue_wait_next_retry_at": "",
        "queue_repair_completed_generation": 0,
        "polish_completed_generation": 0,
        "updated_at": "",
    }
    if not path.exists():
        return dict(default_state)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default_state)
    if not isinstance(raw, dict):
        return dict(default_state)
    media_completed = raw.get("media_completed_ranges", {})
    if not isinstance(media_completed, dict):
        media_completed = {}
    active_queue_range = raw.get("active_queue_range", {})
    if not isinstance(active_queue_range, dict):
        active_queue_range = {}
    return {
        "media_completed_ranges": media_completed,
        "queue_generation": int(raw.get("queue_generation", 0) or 0),
        "active_queue_generation": int(raw.get("active_queue_generation", 0) or 0),
        "active_queue_range": {
            "start": int(active_queue_range.get("start", 0) or 0),
            "end": int(active_queue_range.get("end", 0) or 0),
        },
        "queue_wait_attempts": int(raw.get("queue_wait_attempts", 0) or 0),
        "queue_wait_next_retry_at": str(raw.get("queue_wait_next_retry_at", "") or ""),
        "queue_repair_completed_generation": int(raw.get("queue_repair_completed_generation", 0) or 0),
        "polish_completed_generation": int(raw.get("polish_completed_generation", 0) or 0),
        "updated_at": str(raw.get("updated_at", "") or ""),
    }


def _save_watch_state(config: NovelConfig, state: dict[str, object]) -> None:
    path = _watch_state_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "media_completed_ranges": state.get("media_completed_ranges", {}),
        "queue_generation": int(state.get("queue_generation", 0) or 0),
        "active_queue_generation": int(state.get("active_queue_generation", 0) or 0),
        "active_queue_range": state.get("active_queue_range", {"start": 0, "end": 0}),
        "queue_wait_attempts": int(state.get("queue_wait_attempts", 0) or 0),
        "queue_wait_next_retry_at": str(state.get("queue_wait_next_retry_at", "") or ""),
        "queue_repair_completed_generation": int(state.get("queue_repair_completed_generation", 0) or 0),
        "polish_completed_generation": int(state.get("polish_completed_generation", 0) or 0),
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


def _merged_audio_path(config: NovelConfig, start: int, end: int) -> Path:
    range_key = _range_key(start, end)
    return config.storage.audio_dir / range_key / f"{range_key}.aac"


def _menu_path(config: NovelConfig, start: int, end: int) -> Path:
    return config.storage.subtitle_dir / f"{_range_key(start, end)}_menu.txt"


def _visual_path(config: NovelConfig, start: int, end: int) -> Path:
    return config.storage.visual_dir / f"{_range_key(start, end)}.mp4"


def _video_path(config: NovelConfig, start: int, end: int) -> Path:
    return config.storage.video_dir / f"{_range_key(start, end)}.mp4"


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


def _translated_ranges(config: NovelConfig, latest_chapter: int) -> list[tuple[int, int]]:
    if latest_chapter <= 0:
        return []
    ranges: list[tuple[int, int]] = []
    for item in collect_media_batch_ranges(config, 1, latest_chapter):
        if _translated_range_path(config, item.start, item.end).exists():
            ranges.append((item.start, item.end))
    return ranges


def _collect_untranslated_chapters(
    config: NovelConfig,
    *,
    upto_chapter: int,
) -> list[int]:
    if upto_chapter <= 0 or not config.storage.origin_dir.exists():
        return []
    chapters: list[int] = []
    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num_str, chapter_text in load_source_chapters(config, source_path):
            try:
                chapter_num = int(str(chapter_num_str))
            except Exception:
                continue
            if chapter_num < 1 or chapter_num > upto_chapter:
                continue
            if _chapter_needs_work(config, source_path, str(chapter_num), chapter_text=chapter_text):
                chapters.append(chapter_num)
    return sorted(set(chapters))


def _count_novel_queue_jobs(config: NovelConfig) -> dict[str, int]:
    client = _client(config)

    def _filter_job_ids(values: list[str]) -> set[str]:
        return {job_id for job_id in values if _extract_novel_id(job_id) == config.novel_id}

    pending_priority = _filter_job_ids(client.lrange(_pending_priority_key(config), 0, -1) or [])
    pending_normal = _filter_job_ids(client.lrange(_pending_key(config), 0, -1) or [])
    pending_delayed = _filter_job_ids(client.zrange(_pending_delayed_key(config), 0, -1) or [])
    inflight = _filter_job_ids(list((client.hkeys(_inflight_key(config)) or [])))
    queued = _filter_job_ids(list((client.smembers(_queued_key(config)) or [])))
    done = int(client.hlen(_novel_key(config, config.novel_id, "done")) or 0)
    waiting = (pending_priority | pending_normal | pending_delayed) - inflight
    return {
        "waiting": len(waiting),
        "queued": len(queued),
        "inflight": len(inflight),
        "done": done,
    }


def _begin_queue_generation(
    state: dict[str, object],
    *,
    untranslated_chapters: list[int],
) -> int:
    queue_generation = int(state.get("queue_generation", 0) or 0)
    active_generation = int(state.get("active_queue_generation", 0) or 0)
    polish_completed_generation = int(state.get("polish_completed_generation", 0) or 0)
    if active_generation <= 0 or polish_completed_generation >= active_generation:
        queue_generation += 1
        active_generation = queue_generation
        state["queue_generation"] = queue_generation
        state["active_queue_generation"] = active_generation
        state["queue_repair_completed_generation"] = min(
            int(state.get("queue_repair_completed_generation", 0) or 0),
            active_generation - 1,
        )
        state["polish_completed_generation"] = min(
            int(state.get("polish_completed_generation", 0) or 0),
            active_generation - 1,
        )
        state["queue_wait_attempts"] = 0
        state["queue_wait_next_retry_at"] = ""

    active_range = state.get("active_queue_range", {})
    if not isinstance(active_range, dict):
        active_range = {}
    start = int(active_range.get("start", 0) or 0)
    end = int(active_range.get("end", 0) or 0)
    if untranslated_chapters:
        wanted_start = min(untranslated_chapters)
        wanted_end = max(untranslated_chapters)
        if start <= 0 or end <= 0:
            start, end = wanted_start, wanted_end
        else:
            start = min(start, wanted_start)
            end = max(end, wanted_end)
    state["active_queue_range"] = {"start": start, "end": end}
    return active_generation


def _active_queue_span(state: dict[str, object], fallback_end: int) -> tuple[int, int]:
    active_range = state.get("active_queue_range", {})
    if not isinstance(active_range, dict):
        active_range = {}
    start = int(active_range.get("start", 0) or 0)
    end = int(active_range.get("end", 0) or 0)
    if start > 0 and end > 0:
        return start, end
    if fallback_end > 0:
        return 1, fallback_end
    return 0, 0


def _run_ps_ax() -> str:
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout or ""


def _top_level_command(argv: list[str]) -> str:
    commands = {"crawl", "translate", "queue", "tts", "create-menu", "visual", "video", "upload", "pipeline"}
    for token in argv:
        if token in commands:
            return token
    return ""


def _has_stage_process(novel_id: str, stage: str) -> bool:
    for raw_line in _run_ps_ax().splitlines():
        line = raw_line.strip()
        if not line or novel_id not in line:
            continue
        try:
            argv = shlex.split(line)
        except Exception:
            argv = line.split()
        if _top_level_command(argv) != stage:
            continue
        if novel_id not in argv:
            continue
        return True
    return False


def _run_queue_repair_rounds(
    config: NovelConfig,
    *,
    start: int,
    end: int,
) -> bool:
    if start <= 0 or end <= 0:
        return False
    changed = False
    for round_index in range(1, 4):
        jobs = find_repair_jobs_in_range(config, start, end)
        if not jobs:
            LOGGER.info(
                "Watch queue repair clean | novel=%s range=%s-%s round=%s",
                config.novel_id,
                start,
                end,
                round_index,
            )
            break
        LOGGER.info(
            "Watch queue repair start | novel=%s range=%s-%s round=%s jobs=%s",
            config.novel_id,
            start,
            end,
            round_index,
            len(jobs),
        )
        enqueue_repair_jobs(config, jobs, label=f"watch repair round {round_index}")
        changed = True
        while True:
            remaining = _collect_untranslated_chapters(config, upto_chapter=end)
            queue_counts = _count_novel_queue_jobs(config)
            remaining_in_span = [chapter for chapter in remaining if start <= chapter <= end]
            if not remaining_in_span and queue_counts["waiting"] == 0 and queue_counts["inflight"] == 0:
                break
            LOGGER.info(
                "Watch queue repair waiting | novel=%s range=%s-%s round=%s remaining=%s waiting=%s inflight=%s",
                config.novel_id,
                start,
                end,
                round_index,
                len(remaining_in_span),
                queue_counts["waiting"],
                queue_counts["inflight"],
            )
            time.sleep(5.0)
    return changed


def _run_polish_rounds(config: NovelConfig, *, start: int, end: int) -> bool:
    if start <= 0 or end <= 0:
        return False
    changed = False
    for round_index in range(1, 6):
        polished_files, polished_lines = _polish_range(config, start, end)
        LOGGER.info(
            "Watch polish round | novel=%s range=%s-%s round=%s files=%s lines=%s",
            config.novel_id,
            start,
            end,
            round_index,
            polished_files,
            polished_lines,
        )
        changed = changed or polished_files > 0 or polished_lines > 0
    return changed


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
) -> tuple[bool, float | None]:
    state = _load_watch_state(config)
    source_configs = load_novel_source_configs(config.novel_id)
    remote_latest, selected_source = _discover_best_remote_source(config, source_configs)
    if remote_latest is None or selected_source is None:
        LOGGER.warning("Watch novel skipped | novel=%s reason=remote-discovery-unavailable", config.novel_id)
        return False, None
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
    next_delay = max(5.0, float(getattr(watch_cfg, "interval_seconds", 300.0) or 300.0))

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
        if not skip_repair:
            LOGGER.info("Watch crawl repair start | novel=%s range=%s-%s", config.novel_id, crawl_start, crawl_end)
            repair_crawled_content(
                source_bound_config,
                crawl_start,
                crawl_end,
                generate_repair_config_if_missing=True,
            )
        tts_ranges.extend([(item.start, item.end) for item in collect_media_batch_ranges(config, crawl_start, crawl_end)])
        changed = True
        local_latest = max(local_latest, crawl_end)

    latest_known = max(local_latest, remote_latest)
    untranslated_chapters = _collect_untranslated_chapters(source_bound_config, upto_chapter=latest_known)
    if untranslated_chapters and not skip_translate:
        active_generation = _begin_queue_generation(state, untranslated_chapters=untranslated_chapters)
        queue_counts = _count_novel_queue_jobs(source_bound_config)
        LOGGER.info(
            "Watch queue scan | novel=%s generation=%s untranslated=%s waiting=%s inflight=%s queued=%s",
            config.novel_id,
            active_generation,
            len(untranslated_chapters),
            queue_counts["waiting"],
            queue_counts["inflight"],
            queue_counts["queued"],
        )
        if queue_counts["waiting"] > 0:
            now_ts = _utc_now_ts()
            retry_at_ts = _parse_iso_ts(state.get("queue_wait_next_retry_at"))
            if retry_at_ts > now_ts:
                remaining = max(5.0, retry_at_ts - now_ts)
                LOGGER.info(
                    "Watch queue wait active | novel=%s generation=%s attempts=%s retry_in=%.1fs",
                    config.novel_id,
                    active_generation,
                    int(state.get("queue_wait_attempts", 0) or 0),
                    remaining,
                )
                next_delay = min(next_delay, remaining)
            else:
                wait_attempts = int(state.get("queue_wait_attempts", 0) or 0) + 1
                state["queue_wait_attempts"] = wait_attempts
                if wait_attempts >= QUEUE_PENDING_MAX_WAITS:
                    LOGGER.warning(
                        "Watch queue drain pending jobs | novel=%s generation=%s waiting=%s attempts=%s",
                        config.novel_id,
                        active_generation,
                        queue_counts["waiting"],
                        wait_attempts,
                    )
                    drain_novel_from_queue(source_bound_config)
                    launch_queue_stack(load_queue_config(), restart=effective_restart_queue, add_queue=False)
                    add_chapters_to_queue(source_bound_config, untranslated_chapters, force=False)
                    state["queue_wait_attempts"] = 0
                    state["queue_wait_next_retry_at"] = ""
                    changed = True
                    next_delay = min(next_delay, QUEUE_REPAIR_CHECK_SECONDS)
                else:
                    retry_at = now_ts + QUEUE_PENDING_WAIT_SECONDS
                    state["queue_wait_next_retry_at"] = _iso_from_ts(retry_at)
                    LOGGER.info(
                        "Watch queue wait scheduled | novel=%s generation=%s attempts=%s retry_at=%s",
                        config.novel_id,
                        active_generation,
                        wait_attempts,
                        state["queue_wait_next_retry_at"],
                    )
                    next_delay = min(next_delay, QUEUE_PENDING_WAIT_SECONDS)
        else:
            launch_queue_stack(load_queue_config(), restart=effective_restart_queue, add_queue=False)
            add_chapters_to_queue(source_bound_config, untranslated_chapters, force=False)
            state["queue_wait_attempts"] = 0
            state["queue_wait_next_retry_at"] = ""
            changed = True
            next_delay = min(next_delay, QUEUE_REPAIR_CHECK_SECONDS)

    active_generation = int(state.get("active_queue_generation", 0) or 0)
    if (
        active_generation > 0
        and int(state.get("queue_repair_completed_generation", 0) or 0) < active_generation
        and not skip_repair
    ):
        queue_counts = _count_novel_queue_jobs(source_bound_config)
        untranslated_chapters = _collect_untranslated_chapters(source_bound_config, upto_chapter=latest_known)
        if untranslated_chapters or queue_counts["waiting"] > 0 or queue_counts["inflight"] > 0:
            LOGGER.info(
                "Watch queue repair gated | novel=%s generation=%s untranslated=%s waiting=%s inflight=%s",
                config.novel_id,
                active_generation,
                len(untranslated_chapters),
                queue_counts["waiting"],
                queue_counts["inflight"],
            )
            next_delay = min(next_delay, QUEUE_REPAIR_CHECK_SECONDS)
        else:
            repair_start, repair_end = _active_queue_span(state, latest_known)
            changed = _run_queue_repair_rounds(source_bound_config, start=repair_start, end=repair_end) or changed
            state["queue_repair_completed_generation"] = active_generation
            next_delay = min(next_delay, QUEUE_REPAIR_CHECK_SECONDS)

    if (
        active_generation > 0
        and int(state.get("queue_repair_completed_generation", 0) or 0) >= active_generation
        and int(state.get("polish_completed_generation", 0) or 0) < active_generation
        and not skip_polish
    ):
        polish_start, polish_end = _active_queue_span(state, latest_known)
        changed = _run_polish_rounds(source_bound_config, start=polish_start, end=polish_end) or changed
        state["polish_completed_generation"] = active_generation
        next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)

    if latest_known > 0:
        for item in _translated_ranges(source_bound_config, latest_known):
            if item not in tts_ranges:
                tts_ranges.append(item)

    tts_busy = _has_stage_process(config.novel_id, "tts")
    menu_busy = _has_stage_process(config.novel_id, "create-menu")
    visual_busy = _has_stage_process(config.novel_id, "visual")
    video_busy = _has_stage_process(config.novel_id, "video")

    for start, end in sorted(set(tts_ranges)):
        translated_path = _translated_range_path(config, start, end)
        if not translated_path.exists():
            LOGGER.info(
                "Watch tts skipped | novel=%s range=%s missing=%s",
                config.novel_id,
                _range_key(start, end),
                translated_path,
            )
            continue
        if not skip_tts and not tts_busy:
            try:
                run_tts(source_bound_config, start, end, range_key=_range_key(start, end))
                changed = True
            except Exception:
                LOGGER.exception(
                    "Watch tts failed | novel=%s range=%s",
                    config.novel_id,
                    _range_key(start, end),
                )
                next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
                continue
        elif not skip_tts and tts_busy:
            LOGGER.info("Watch tts busy | novel=%s range=%s", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
        can_create_menu = _menu_path(source_bound_config, start, end).exists() or _audio_parts_count(source_bound_config, start, end) >= max(1, end - start + 1)
        if not skip_create_menu and not menu_busy and can_create_menu:
            from novel_tts.tts import create_menu
            try:
                create_menu(source_bound_config, start, end, range_key=_range_key(start, end))
                changed = True
            except Exception:
                LOGGER.exception(
                    "Watch create-menu failed | novel=%s range=%s",
                    config.novel_id,
                    _range_key(start, end),
                )
                next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
        elif not skip_create_menu and menu_busy:
            LOGGER.info("Watch create-menu busy | novel=%s range=%s", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
        elif not skip_create_menu and not can_create_menu:
            LOGGER.info("Watch create-menu gated | novel=%s range=%s reason=missing-audio", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)

        media_skip_visual = skip_visual or visual_busy
        media_skip_video = skip_video or video_busy
        if visual_busy and not skip_visual:
            LOGGER.info("Watch visual busy | novel=%s range=%s", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
        if video_busy and not skip_video:
            LOGGER.info("Watch video busy | novel=%s range=%s", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)

        if not media_skip_visual and not _merged_audio_path(source_bound_config, start, end).exists():
            LOGGER.info("Watch visual gated | novel=%s range=%s reason=missing-audio", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
        elif not media_skip_video and not _visual_path(source_bound_config, start, end).exists():
            LOGGER.info("Watch video gated | novel=%s range=%s reason=missing-visual", config.novel_id, _range_key(start, end))
            next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)
        else:
            try:
                media_changed = _run_media_if_ready(
                    source_bound_config,
                    start=start,
                    end=end,
                    state=state,
                    upload_platform=upload_platform,
                    skip_visual=media_skip_visual,
                    skip_video=media_skip_video,
                    skip_upload=skip_upload,
                )
                changed = media_changed or changed
            except Exception:
                LOGGER.exception(
                    "Watch media failed | novel=%s range=%s",
                    config.novel_id,
                    _range_key(start, end),
                )
                next_delay = min(next_delay, ACTIVE_MEDIA_POLL_SECONDS)

    _save_watch_state(config, state)
    return changed, next_delay


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

    configured_sleep_seconds: float | None = None
    if interval_seconds is not None:
        configured_sleep_seconds = max(5.0, float(interval_seconds))
    had_error = False

    while True:
        cycle_changed = False
        sleep_seconds = configured_sleep_seconds
        for novel_id in resolved_ids:
            try:
                config = load_novel_config(novel_id)
                if sleep_seconds is None:
                    sleep_seconds = max(5.0, float(config.pipeline.watch.interval_seconds or 300.0))
                novel_changed, novel_next_delay = _process_novel(
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
                )
                cycle_changed = novel_changed or cycle_changed
                if novel_next_delay is not None:
                    sleep_seconds = min(float(sleep_seconds or novel_next_delay), float(novel_next_delay))
            except Exception:
                had_error = True
                LOGGER.exception("Watch cycle failed | novel=%s", novel_id)
                sleep_seconds = min(float(sleep_seconds or ACTIVE_MEDIA_POLL_SECONDS), ACTIVE_MEDIA_POLL_SECONDS)

        if once:
            return 1 if had_error else 0

        LOGGER.info(
            "Watch cycle complete | novels=%s changed=%s next_poll_in=%.1fs",
            ",".join(resolved_ids),
            cycle_changed,
            sleep_seconds or 300.0,
        )
        time.sleep(sleep_seconds or 300.0)
