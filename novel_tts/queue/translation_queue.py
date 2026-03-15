from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from novel_tts.common.logging import get_logger, get_novel_log_path
from novel_tts.config.models import NovelConfig
from novel_tts.translate.novel import chapter_part_path, is_glossary_pending, load_source_chapters

LOGGER = get_logger(__name__)
CAPTIONS_JOB_ID = "captions"

_RATE_LIMIT_TOKENS = (
    "429",
    "rate limit",
    "too many requests",
    "resource_exhausted",
)

_QUOTA_WAIT_TOKENS = (
    "quota is exhausted",
    "paused because model quota is exhausted",
    "blocked_model=",
)

_OUT_OF_QUOTA_TOKENS = (
    "worker entering out-of-quota cooldown",
    "out_of_quota",
)


def _tail_lines(path: str, max_bytes: int = 16384, max_lines: int = 80) -> list[str]:
    """Return the last N lines of a text file without reading it all."""
    if not path:
        return []
    try:
        with open(path, "rb") as fh:
            try:
                fh.seek(0, os.SEEK_END)
                end = fh.tell()
                start = max(0, end - max_bytes)
                fh.seek(start, os.SEEK_SET)
                data = fh.read()
            except OSError:
                data = fh.read()
    except OSError:
        return []

    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return []
    lines = text.splitlines()
    return lines[-max_lines:] if len(lines) > max_lines else lines


def _parse_log_timestamp(line: str) -> datetime | None:
    # Format: "YYYY-MM-DD HH:MM:SS,mmm | LEVEL | ..."
    if not line:
        return None
    prefix = line.split("|", 1)[0].strip()
    try:
        return datetime.strptime(prefix, "%Y-%m-%d %H:%M:%S,%f")
    except Exception:
        return None


def _waiting_expired(line: str, *, now: datetime, grace_seconds: float = 2.0) -> tuple[bool, float | None]:
    """Return (expired, wait_seconds) for known waiting/sleep lines, or (False, None) if unknown."""
    if not line:
        return False, None
    ts = _parse_log_timestamp(line)
    if ts is None:
        return False, None

    lowered = line.lower()
    sleep_match = re.search(r"sleeping for ([0-9]+(?:\.[0-9]+)?)s", lowered)
    if sleep_match:
        wait_s = float(sleep_match.group(1))
        return (now >= (ts + timedelta(seconds=wait_s + grace_seconds))), wait_s

    quota_match = re.search(r"wait_seconds=([0-9]+(?:\.[0-9]+)?)", lowered)
    if quota_match:
        wait_s = float(quota_match.group(1))
        return (now >= (ts + timedelta(seconds=wait_s + grace_seconds))), wait_s

    return False, None


def _waiting_countdown_seconds(line: str, *, now: datetime, grace_seconds: float = 2.0) -> float | None:
    """Return remaining seconds for a waiting line, or None if not a waiting line."""
    if not line:
        return None
    ts = _parse_log_timestamp(line)
    if ts is None:
        return None
    expired, wait_s = _waiting_expired(line, now=now, grace_seconds=grace_seconds)
    if wait_s is None:
        return None
    if expired:
        return 0.0
    end = ts + timedelta(seconds=float(wait_s) + float(grace_seconds))
    return max(0.0, (end - now).total_seconds())


def _format_countdown(seconds: float | None) -> str:
    """Human-friendly countdown format for queue ps tables.

    Rules:
    - Base format: hh:mm:ss with units (e.g. 1h:23m:39s ; 43m:24s ; 47s)
    - If countdown > 3 minutes, only show hh:mm (e.g. 1h:32m ; 34m)
    """
    if seconds is None:
        return ""
    try:
        value = float(seconds)
    except Exception:
        return ""
    if value <= 0:
        return ""

    total_s = int(math.ceil(value))
    if total_s <= 0:
        return ""

    if total_s > 180:
        total_m = total_s // 60
        hours = total_m // 60
        minutes = total_m % 60
        if hours > 0:
            return f"{hours}h:{minutes}m"
        return f"{minutes}m"

    hours = total_s // 3600
    rem = total_s % 3600
    minutes = rem // 60
    secs = rem % 60

    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return ":".join(parts)


def _format_target(file_arg: str, chapter_arg: str) -> str:
    file_arg = (file_arg or "").strip()
    chapter_arg = (chapter_arg or "").strip()
    if not file_arg or not chapter_arg:
        return ""
    base = os.path.basename(file_arg)
    if chapter_arg.isdigit():
        chapter_arg = f"{int(chapter_arg):04d}"
    return f"{base}:{chapter_arg}"


def _extract_target_from_argv(argv: list[str]) -> str:
    file_arg = ""
    chapter_arg = ""
    for idx, token in enumerate(argv):
        if token == "--file" and idx + 1 < len(argv):
            file_arg = argv[idx + 1]
        elif token == "--chapter" and idx + 1 < len(argv):
            chapter_arg = argv[idx + 1]
    return _format_target(file_arg, chapter_arg)


def _classify_process_state(role: str, *, is_busy: bool, log_file: str) -> tuple[str, float | None]:
    """Heuristic state classifier for ps output."""
    lines = _tail_lines(log_file)
    tail = "\n".join(lines[-40:]).lower()

    # Hard errors (exceptions, crashes).
    if "traceback" in tail or "command failed" in tail:
        return "error", None

    if role == "worker":
        # Prefer process-tree truth when available.
        if is_busy:
            return "busy", None

        # When idle (no translate-chapter child), classify based on the most recent relevant log event.
        # We scan from the end so older 429s don't permanently label the worker.
        now = datetime.now()
        for raw in reversed(lines[-200:]):
            line = (raw or "").lower()
            if not line:
                continue
            if "traceback" in line or "command failed" in line:
                return "error", None
            if any(token in line for token in _OUT_OF_QUOTA_TOKENS):
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "out-of-quota", remaining
            if any(token in line for token in _QUOTA_WAIT_TOKENS):
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-quota", remaining
            # Match the specific "rate limit hit + sleeping" style so we don't trigger on unrelated "429" text.
            if ("rate limit" in line or "429" in line or "too many requests" in line) and "sleep" in line:
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-429", remaining
            if "worker done:" in line or "rebuilt file:" in line or "translated chapter part:" in line:
                return "idle", None
        return "idle", None

    if role == "translate-chapter":
        # This process exists only while doing work, but it may be sleeping on rate limits/quota.
        now = datetime.now()
        phase: str | None = None
        for raw in reversed(lines[-200:]):
            lowered = (raw or "").lower()
            if not lowered:
                continue
            if any(token in lowered for token in _QUOTA_WAIT_TOKENS):
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-quota", remaining
            if ("rate limit" in lowered or "429" in lowered or "too many requests" in lowered) and "sleep" in lowered:
                remaining = _waiting_countdown_seconds(raw or "", now=now)
                if remaining is not None and remaining > 0:
                    return "waiting-429", remaining
            if "traceback" in lowered or "command failed" in lowered:
                return "error", None
            if "queue_phase glossary" in lowered:
                phase = "glossary"
                break
            if "queue_phase repair" in lowered:
                phase = "repair"
                break
            if "queue_phase translate" in lowered:
                phase = "translate"
                break
            # If we hit a completion marker in this log stream, treat the child as busy only if it's actually running.
            if "worker done:" in lowered or "rebuilt file:" in lowered or "translated chapter part:" in lowered:
                break
        return phase or "busy", None
    return "running", None


def _combine_worker_child_states(child_states: list[str]) -> str | None:
    """Pick a representative state for a worker based on its translate-chapter children."""
    if not child_states:
        return None
    # Priority: if any child is waiting/error, the worker is effectively waiting/error too.
    priority = {
        "error": 4,
        "out-of-quota": 3,
        "waiting-quota": 3,
        "waiting-429": 2,
        "glossary": 2,
        "repair": 2,
        "translate": 1,
        "busy": 1,
    }
    best = None
    best_score = 0
    for state in child_states:
        score = priority.get(state or "", 0)
        if score > best_score:
            best_score = score
            best = state
    return best


def _combine_worker_child_states_with_countdown(children: list[dict[str, str]]) -> tuple[str | None, float | None]:
    if not children:
        return None, None
    # Reuse the same priority as the string-only combiner.
    priority = {
        "error": 4,
        "out-of-quota": 3,
        "waiting-quota": 3,
        "waiting-429": 2,
        "glossary": 2,
        "repair": 2,
        "translate": 1,
        "busy": 1,
    }
    best_state: str | None = None
    best_score = 0
    for child in children:
        state = (child.get("state") or "").strip()
        score = priority.get(state, 0)
        if score > best_score:
            best_score = score
            best_state = state
    if not best_state:
        return None, None
    if best_state.startswith("waiting-"):
        remaining: float | None = None
        for child in children:
            if (child.get("state") or "").strip() != best_state:
                continue
            try:
                value = float(child.get("countdown") or 0.0)
            except Exception:
                continue
            remaining = value if remaining is None else max(remaining, value)
        return best_state, remaining
    return best_state, None


def _client(config: NovelConfig):
    import redis

    return redis.Redis(
        host=config.queue.redis.host,
        port=config.queue.redis.port,
        db=config.queue.redis.database,
        decode_responses=True,
    )


def _key(config: NovelConfig, suffix: str) -> str:
    return f"{config.queue.redis.prefix}:{config.novel_id}:{suffix}"


def _key_file(config: NovelConfig) -> Path:
    return config.storage.root / ".secrets" / "gemini-keys.txt"


def _load_keys(config: NovelConfig) -> list[str]:
    key_file = _key_file(config)
    if not key_file.exists():
        raise FileNotFoundError(f"Missing key file: {key_file}")
    keys = [line.strip() for line in key_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not keys:
        raise RuntimeError(f"No Gemini keys found in {key_file}")
    return keys


def _needs_translation(config: NovelConfig, path: Path) -> bool:
    target = config.storage.translated_dir / path.name
    if not target.exists():
        return True
    return path.stat().st_mtime > target.stat().st_mtime


def _job_id(file_name: str, chapter_num: str) -> str:
    return f"{file_name}::{int(chapter_num):04d}"


def _is_captions_job(job_id: str) -> bool:
    return (job_id or "").strip().lower() == CAPTIONS_JOB_ID


def _parse_job_id(job_id: str) -> tuple[str, str]:
    # Backward-compatible chapter job id: "<file_name>::<chapter_num>"
    file_name, chapter_num = job_id.split("::", 1)
    return file_name, str(int(chapter_num))


def _chapter_needs_translation(config: NovelConfig, source_path: Path, chapter_num: str) -> bool:
    part_path = chapter_part_path(config, source_path, chapter_num)
    if not part_path.exists():
        return True
    if _needs_translation(config, source_path) is False:
        return False
    return part_path.stat().st_mtime < source_path.stat().st_mtime


def _chapter_needs_work(config: NovelConfig, source_path: Path, chapter_num: str) -> bool:
    if is_glossary_pending(config, source_path, chapter_num):
        return True
    return _chapter_needs_translation(config, source_path, chapter_num)


def _captions_needs_translation(config: NovelConfig) -> bool:
    input_path = config.storage.caption_dir / config.captions.input_file
    output_path = config.storage.caption_dir / config.captions.output_file
    if not input_path.exists():
        return False
    if not output_path.exists():
        return True
    return input_path.stat().st_mtime > output_path.stat().st_mtime


def _chapter_jobs_for_file(config: NovelConfig, source_path: Path) -> list[str]:
    jobs: list[str] = []
    for chapter_num, _chapter_text in load_source_chapters(config, source_path):
        if _chapter_needs_work(config, source_path, chapter_num):
            jobs.append(_job_id(source_path.name, chapter_num))
    return jobs


def _retry_count(config: NovelConfig, client, job_id: str) -> int:
    value = client.hget(_key(config, "retries"), job_id)
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _has_exhausted_retries(config: NovelConfig, client, job_id: str) -> bool:
    return _retry_count(config, client, job_id) >= config.queue.max_retries


def _enqueue_needed_jobs(config: NovelConfig, client) -> None:
    for path in sorted(config.storage.origin_dir.glob("*.txt")):
        for job_id in _chapter_jobs_for_file(config, path):
            if client.hexists(_key(config, "inflight"), job_id):
                continue
            if _has_exhausted_retries(config, client, job_id):
                continue
            if client.sadd(_key(config, "queued"), job_id):
                client.rpush(_key(config, "pending"), job_id)
    if _captions_needs_translation(config):
        job_id = CAPTIONS_JOB_ID
        if not client.hexists(_key(config, "inflight"), job_id) and not _has_exhausted_retries(config, client, job_id):
            if client.sadd(_key(config, "queued"), job_id):
                client.rpush(_key(config, "pending"), job_id)


def add_jobs_to_queue(config: NovelConfig, from_chapter: int, to_chapter: int, *, force: bool = False) -> int:
    """Enqueue a specific chapter range for translation.

    - Range is chapter-number based, independent of origin batch boundaries.
    - When force=True, jobs are marked so workers will translate even if parts are up-to-date.
    """
    if from_chapter > to_chapter:
        from_chapter, to_chapter = to_chapter, from_chapter

    client = _client(config)
    added = 0
    skipped_done = 0
    skipped_exhausted = 0

    for source_path in sorted(config.storage.origin_dir.glob("*.txt")):
        for chapter_num, _chapter_text in load_source_chapters(config, source_path):
            try:
                chap = int(str(chapter_num))
            except Exception:
                continue
            if chap < from_chapter or chap > to_chapter:
                continue
            job_id = _job_id(source_path.name, str(chap))
            if client.hexists(_key(config, "inflight"), job_id):
                continue
            if not force:
                if _has_exhausted_retries(config, client, job_id):
                    skipped_exhausted += 1
                    continue
                if not _chapter_needs_work(config, source_path, str(chap)):
                    skipped_done += 1
                    continue

            if force:
                # Mark as force so workers won't skip due to up-to-date parts.
                client.hset(_key(config, "force"), job_id, str(int(time.time())))
                # Clear retries so a force enqueue gets a full retry budget again.
                client.hdel(_key(config, "retries"), job_id)

            if client.sadd(_key(config, "queued"), job_id):
                client.rpush(_key(config, "pending"), job_id)
                added += 1

    LOGGER.info(
        "Queue add | novel=%s range=%s-%s force=%s added=%s skipped_done=%s skipped_exhausted=%s",
        config.novel_id,
        from_chapter,
        to_chapter,
        force,
        added,
        skipped_done,
        skipped_exhausted,
    )
    print(
        f"Queued {added} job(s) for novel {config.novel_id} chapters {from_chapter}-{to_chapter}"
        f"{' (force)' if force else ''}. Skipped already-done={skipped_done}, exhausted-retries={skipped_exhausted}."
    )
    return 0


def add_job_ids_to_queue(
    config: NovelConfig,
    job_ids: list[str],
    *,
    force: bool = True,
    label: str = "queue add",
) -> int:
    """Enqueue an explicit list of job IDs (file::chapter)."""
    if not job_ids:
        print("Queued 0 job(s).")
        return 0

    client = _client(config)
    added = 0
    skipped_inflight = 0
    skipped_exhausted = 0
    missing_origin = 0

    for job_id in job_ids:
        if _is_captions_job(job_id):
            if client.hexists(_key(config, "inflight"), job_id):
                skipped_inflight += 1
                continue
            if not force:
                if _has_exhausted_retries(config, client, job_id):
                    skipped_exhausted += 1
                    continue
                if not _captions_needs_translation(config):
                    continue
            if client.sadd(_key(config, "queued"), job_id):
                client.rpush(_key(config, "pending"), job_id)
                added += 1
            continue
        try:
            file_name, chapter_num = _parse_job_id(job_id)
        except Exception:
            LOGGER.warning("Skipping invalid job_id: %r", job_id)
            continue
        source_path = config.storage.origin_dir / file_name
        if not source_path.exists():
            missing_origin += 1
            continue

        if client.hexists(_key(config, "inflight"), job_id):
            skipped_inflight += 1
            continue

        if not force:
            if _has_exhausted_retries(config, client, job_id):
                skipped_exhausted += 1
                continue
            if not _chapter_needs_work(config, source_path, chapter_num):
                continue

        if force:
            client.hset(_key(config, "force"), job_id, str(int(time.time())))
            client.hdel(_key(config, "retries"), job_id)

        if client.sadd(_key(config, "queued"), job_id):
            client.rpush(_key(config, "pending"), job_id)
            added += 1

    LOGGER.info(
        "%s | novel=%s job_ids=%s force=%s added=%s skipped_inflight=%s skipped_exhausted=%s missing_origin=%s",
        label,
        config.novel_id,
        len(job_ids),
        force,
        added,
        skipped_inflight,
        skipped_exhausted,
        missing_origin,
    )
    print(
        f"Queued {added} job(s) for novel {config.novel_id} ({label}). "
        f"Skipped inflight={skipped_inflight}, exhausted={skipped_exhausted}, missing_origin={missing_origin}."
    )
    return 0


def _requeue_stale_inflight(config: NovelConfig, client) -> None:
    now = time.time()
    for job_id, payload in client.hgetall(_key(config, "inflight")).items():
        meta = json.loads(payload)
        started_at = float(meta.get("started_at", 0))
        if now - started_at < config.queue.inflight_ttl_seconds:
            continue
        client.hdel(_key(config, "inflight"), job_id)
        if _has_exhausted_retries(config, client, job_id):
            continue
        if client.sadd(_key(config, "queued"), job_id):
            client.rpush(_key(config, "pending"), job_id)


def _count_origin_files(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.origin_dir.glob("*.txt"))


def _count_translated_files(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.translated_dir.glob("*.txt"))


def _count_parts(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.parts_dir.rglob("*.txt"))


def _count_checkpoints(config: NovelConfig) -> int:
    return sum(1 for _ in config.storage.progress_dir.glob("*.json"))


def _total_chapters(config: NovelConfig) -> int:
    total = 0
    chapter_regex = re.compile(config.translation.chapter_regex, flags=re.M)
    for path in config.storage.origin_dir.glob("*.txt"):
        total += len(chapter_regex.findall(path.read_text(encoding="utf-8")))
    return total


def _status_paths(config: NovelConfig) -> tuple[Path, Path]:
    return (
        get_novel_log_path(config.storage.logs_dir, config.novel_id, "queue/status.log"),
        get_novel_log_path(config.storage.logs_dir, config.novel_id, "queue/status.state.json"),
    )


def _decode_done_payload(value: str) -> dict[str, str]:
    try:
        payload = json.loads(value)
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {"finished_at": value}


def _parse_finished_at(payload: dict[str, object]) -> float | None:
    value = payload.get("finished_at")
    if value is None:
        return None
    try:
        return float(value)  # stored as time.time() or a numeric-ish string
    except (TypeError, ValueError):
        return None


def _write_status_line(
    config: NovelConfig,
    client,
    last_snapshot: dict[str, int] | None,
    *,
    append_log: bool = True,
) -> dict[str, int]:
    inflight_payloads = client.hgetall(_key(config, "inflight"))
    done_payloads = client.hgetall(_key(config, "done"))
    model_done = client.hgetall(_key(config, "model_done"))
    model_failed = client.hgetall(_key(config, "model_failed"))
    inflight_by_model: dict[str, int] = {}
    for payload in inflight_payloads.values():
        try:
            model = json.loads(payload).get("model", "unknown")
        except Exception:
            model = "unknown"
        inflight_by_model[model] = inflight_by_model.get(model, 0) + 1
    status_log, state_log = _status_paths(config)
    snapshot = {
        "ts": int(time.time()),
        "origin_files": _count_origin_files(config),
        "translated_files": _count_translated_files(config),
        "parts": _count_parts(config),
        "checkpoints": _count_checkpoints(config),
        "chapter_total": _total_chapters(config),
        "pending": client.llen(_key(config, "pending")),
        "queued": client.scard(_key(config, "queued")),
        "inflight": len(inflight_payloads),
        "retries": client.hlen(_key(config, "retries")),
        "done": len(done_payloads),
        "inflight_by_model": inflight_by_model,
        "done_by_model": {model: int(count) for model, count in model_done.items()},
        "failed_by_model": {model: int(count) for model, count in model_failed.items()},
    }
    files_per_min = 0.0
    parts_per_min = 0.0
    eta_files = "unknown"
    eta_parts = "unknown"
    eta_queue = "unknown"
    if last_snapshot:
        delta_s = snapshot["ts"] - last_snapshot["ts"]
        if delta_s > 0:
            # For both normal and force re-translate, the file/part *counts* may not change.
            # Instead, infer throughput from "done" jobs in the time window.
            # Each job corresponds to one chapter part; file throughput is measured as distinct batch files touched.
            window_start = float(last_snapshot["ts"])
            completed_jobs = 0
            touched_files: set[str] = set()
            for raw in done_payloads.values():
                payload = _decode_done_payload(raw)
                finished_at = _parse_finished_at(payload) or 0.0
                if finished_at < window_start:
                    continue
                completed_jobs += 1
                file_name = str(payload.get("file_name") or "").strip()
                if file_name:
                    touched_files.add(file_name)

            parts_per_min = completed_jobs * 60.0 / delta_s
            files_per_min = len(touched_files) * 60.0 / delta_s
            if files_per_min > 0 and snapshot["origin_files"] > snapshot["translated_files"]:
                minutes = (snapshot["origin_files"] - snapshot["translated_files"]) / files_per_min
                eta_files = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
            if parts_per_min > 0 and snapshot["chapter_total"] > snapshot["parts"]:
                minutes = (snapshot["chapter_total"] - snapshot["parts"]) / parts_per_min
                eta_parts = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
            queue_remaining = snapshot["pending"] + snapshot["queued"] + snapshot["inflight"]
            if parts_per_min > 0 and queue_remaining > 0:
                minutes = queue_remaining / parts_per_min
                eta_queue = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
    done_pct = (snapshot["translated_files"] / snapshot["origin_files"] * 100) if snapshot["origin_files"] else 0.0
    part_pct = (snapshot["parts"] / snapshot["chapter_total"] * 100) if snapshot["chapter_total"] else 0.0
    line = (
        f"translated={snapshot['translated_files']}/{snapshot['origin_files']} "
        f"| done={done_pct:.2f}% | parts={snapshot['parts']}/{snapshot['chapter_total']} "
        f"| part_done={part_pct:.2f}% | files/min={files_per_min:.2f} | parts/min={parts_per_min:.2f} "
        f"| ETA_files={eta_files} | ETA_parts={eta_parts} | ETA_queue={eta_queue} | checkpoints={snapshot['checkpoints']} "
        f"| retries={snapshot['retries']} | pending={snapshot['pending']} | queued={snapshot['queued']} "
        f"| inflight={snapshot['inflight']} | workers={snapshot['inflight']} "
        f"| inflight_by_model={snapshot['inflight_by_model']} "
        f"| done_by_model={snapshot['done_by_model']} | failed_by_model={snapshot['failed_by_model']}"
    )
    state_log.parent.mkdir(parents=True, exist_ok=True)
    if append_log:
        with status_log.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {line}\n")
    state_log.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot


def _daily_quota_key(config: NovelConfig, key_index: int, model: str) -> str:
    return f"{config.queue.redis.prefix}:{config.novel_id}:k{key_index}:{model}:quota:daily_reqs"


def _minute_quota_key(config: NovelConfig, key_index: int, model: str) -> str:
    return f"{config.queue.redis.prefix}:{config.novel_id}:k{key_index}:{model}:quota:reqs"


def _minute_token_key(config: NovelConfig, key_index: int, model: str) -> str:
    return f"{config.queue.redis.prefix}:{config.novel_id}:k{key_index}:{model}:quota:tokens"


def _estimate_tokens_from_chars(char_count: int) -> int:
    input_tokens = max(1, math.ceil(max(0, char_count) / 2.2))
    output_reserve = max(256, math.ceil(input_tokens * 0.8))
    return input_tokens + output_reserve


def _estimated_request_tokens_for_model(config: NovelConfig, model: str) -> int:
    model_cfg = config.queue.model_configs.get(model)
    chunk_max_len = model_cfg.chunk_max_len if model_cfg and model_cfg.chunk_max_len > 0 else 0
    if chunk_max_len <= 0:
        chunk_max_len = config.translation.chunk_max_len
    return _estimate_tokens_from_chars(chunk_max_len)


def _model_rpd_exhausted(config: NovelConfig, client, key_index: int, model: str) -> bool:
    model_cfg = config.queue.model_configs.get(model)
    if model_cfg is None or model_cfg.rpd_limit <= 0:
        return False
    now = time.time()
    day_window_start = now - 86400.0
    daily_key = _daily_quota_key(config, key_index, model)
    client.zremrangebyscore(daily_key, 0, day_window_start)
    return client.zcount(daily_key, day_window_start, "+inf") >= model_cfg.rpd_limit


def _model_short_quota_wait_seconds(config: NovelConfig, client, key_index: int, model: str) -> float:
    model_cfg = config.queue.model_configs.get(model)
    if model_cfg is None:
        return 0.0
    rpm_limit = max(0, int(model_cfg.rpm_limit))
    tpm_limit = max(0, int(model_cfg.tpm_limit))
    if rpm_limit <= 0 and tpm_limit <= 0:
        return 0.0

    now = time.time()
    window_start = now - 60.0
    req_key = _minute_quota_key(config, key_index, model)
    token_key = _minute_token_key(config, key_index, model)
    stale_members = client.zrangebyscore(req_key, 0, window_start)
    if stale_members:
        client.zrem(req_key, *stale_members)
        client.hdel(token_key, *stale_members)
    active_members = client.zrangebyscore(req_key, window_start, "+inf", withscores=True)
    token_map = client.hgetall(token_key)
    current_requests = len(active_members)
    current_tokens = 0
    for member, _score in active_members:
        try:
            current_tokens += int(token_map.get(member, "0"))
        except (TypeError, ValueError):
            continue

    estimated_tokens = _estimated_request_tokens_for_model(config, model)
    wait_rpm = 0.0
    if rpm_limit > 0 and current_requests >= rpm_limit and active_members:
        oldest_score = float(active_members[0][1])
        wait_rpm = max(0.25, 60.0 - (now - oldest_score) + 0.05)
    wait_tpm = 0.0
    if tpm_limit > 0 and (current_tokens + estimated_tokens) > tpm_limit and active_members:
        oldest_score = float(active_members[0][1])
        wait_tpm = max(0.25, 60.0 - (now - oldest_score) + 0.05)
    return max(wait_rpm, wait_tpm, 0.0)


def _worker_should_pause_for_quota(config: NovelConfig, client, key_index: int, model: str) -> tuple[bool, str, float]:
    if _model_rpd_exhausted(config, client, key_index, model):
        return True, model, 60.0
    short_wait = _model_short_quota_wait_seconds(config, client, key_index, model)
    if short_wait > 0:
        return True, model, short_wait
    model_cfg = config.queue.model_configs.get(model)
    repair_model = model_cfg.repair_model if model_cfg else ""
    if repair_model and _model_rpd_exhausted(config, client, key_index, repair_model):
        return True, repair_model, 60.0
    if repair_model:
        repair_wait = _model_short_quota_wait_seconds(config, client, key_index, repair_model)
        if repair_wait > 0:
            return True, repair_model, repair_wait
    return False, "", 0.0


def run_worker(config: NovelConfig, key_index: int, model: str) -> int:
    keys = _load_keys(config)
    if key_index < 1 or key_index > len(keys):
        raise ValueError(f"Invalid key index: {key_index}")
    api_key = keys[key_index - 1]
    client = _client(config)
    worker_id = f"{config.novel_id}:k{key_index}:{model}:{os.getpid()}"
    consecutive_rate_limit_releases = 0
    while True:
        should_pause, blocked_model, wait_seconds = _worker_should_pause_for_quota(config, client, key_index, model)
        if should_pause:
            LOGGER.warning(
                "Worker paused because model quota is exhausted | novel=%s key_index=%s model=%s blocked_model=%s wait_seconds=%.2f",
                config.novel_id,
                key_index,
                model,
                blocked_model,
                wait_seconds,
            )
            time.sleep(max(1.0, wait_seconds))
            continue
        item = client.blpop(_key(config, "pending"), timeout=5)
        if not item:
            continue
        job_id = item[1]
        client.srem(_key(config, "queued"), job_id)
        is_captions = _is_captions_job(job_id)
        is_force = bool(client.hexists(_key(config, "force"), job_id)) if not is_captions else False
        file_name = ""
        chapter_num = ""
        source_path: Path | None = None
        if is_captions:
            if not _captions_needs_translation(config):
                continue
        else:
            file_name, chapter_num = _parse_job_id(job_id)
            source_path = config.storage.origin_dir / file_name
            if not source_path.exists() or ((not is_force) and (not _chapter_needs_work(config, source_path, chapter_num))):
                continue
        client.hset(
            _key(config, "inflight"),
            job_id,
            json.dumps(
                {
                    "worker": worker_id,
                    "started_at": time.time(),
                    "model": model,
                    "job_type": "captions" if is_captions else "chapter",
                    "file_name": file_name,
                    "chapter_num": chapter_num,
                }
            ),
        )
        env = os.environ.copy()
        env["GEMINI_API_KEY"] = api_key
        env["GEMINI_MODEL"] = model
        env["NOVEL_TTS_QUOTA_MODE"] = "raise"
        env["NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS"] = "0"
        if not is_captions:
            env["NOVEL_TTS_GLOSSARY_STRICT"] = "1"
        env["GEMINI_RATE_LIMIT_KEY_PREFIX"] = f"{config.queue.redis.prefix}:{config.novel_id}:k{key_index}"
        env["GEMINI_REDIS_HOST"] = config.queue.redis.host
        env["GEMINI_REDIS_PORT"] = str(config.queue.redis.port)
        env["GEMINI_REDIS_DB"] = str(config.queue.redis.database)
        model_cfg = config.queue.model_configs.get(model)
        env["GEMINI_MODEL_CONFIGS_JSON"] = json.dumps(
            {
                model_name: {
                    "rpm_limit": cfg.rpm_limit,
                    "tpm_limit": cfg.tpm_limit,
                    "rpd_limit": cfg.rpd_limit,
                }
                for model_name, cfg in config.queue.model_configs.items()
            }
        )
        if model_cfg and model_cfg.repair_model:
            env["REPAIR_MODEL"] = model_cfg.repair_model
        if model_cfg and model_cfg.chunk_max_len > 0:
            env["CHUNK_MAX_LEN"] = str(model_cfg.chunk_max_len)
        if model_cfg and model_cfg.chunk_sleep_seconds > 0:
            env["CHUNK_SLEEP_SECONDS"] = str(model_cfg.chunk_sleep_seconds)
        # In queue mode, avoid spending a long time sleeping on 429 in a single worker.
        # Let the job be picked up by another worker/key instead.
        env.setdefault("NOVEL_TTS_RATE_LIMIT_MAX_ATTEMPTS", "4")
        import logging
        log_file_args = []
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                log_file_args = ["--log-file", handler.baseFilename]
                break

        cmd_args = [sys.executable, "-m", "novel_tts"] + log_file_args + ["translate"]
        if is_captions:
            cmd_args += ["captions", config.novel_id]
        else:
            cmd_args += ["chapter", config.novel_id]
            if is_force:
                cmd_args.append("--force")
            cmd_args += ["--file", file_name, "--chapter", chapter_num]

        proc = subprocess.run(
            cmd_args,
            cwd=str(config.storage.root),
            env=env,
            capture_output=True,
            text=True,
        )
        client.hdel(_key(config, "inflight"), job_id)
        # Special transient exit code from CLI when providers keep returning 429.
        if proc.returncode == 75:
            consecutive_rate_limit_releases += 1
            LOGGER.warning(
                "Worker releasing job due to rate limit | job=%s key_index=%s model=%s",
                job_id,
                key_index,
                model,
            )
            # Requeue without counting as a failure retry.
            if client.sadd(_key(config, "queued"), job_id):
                client.rpush(_key(config, "pending"), job_id)
            time.sleep(0.5)
            if consecutive_rate_limit_releases >= 2:
                cooldown_seconds = 3600.0
                LOGGER.warning(
                    "Worker entering out-of-quota cooldown | out_of_quota=1 novel=%s key_index=%s model=%s wait_seconds=%.2f",
                    config.novel_id,
                    key_index,
                    model,
                    cooldown_seconds,
                )
                time.sleep(cooldown_seconds)
                consecutive_rate_limit_releases = 0
            continue
        if proc.returncode == 0:
            consecutive_rate_limit_releases = 0
            client.hdel(_key(config, "retries"), job_id)
            if is_force:
                client.hdel(_key(config, "force"), job_id)
            client.hset(
                _key(config, "done"),
                job_id,
                json.dumps(
                    {
                        "finished_at": time.time(),
                        "model": model,
                        "worker": worker_id,
                        "file_name": file_name,
                        "chapter_num": chapter_num,
                        "force": bool(is_force),
                    }
                ),
            )
            client.hincrby(_key(config, "model_done"), model, 1)
            LOGGER.info("Worker done: %s", job_id)
            continue
        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        LOGGER.error(
            "Worker failed | job=%s key_index=%s model=%s returncode=%s stdout=%r stderr=%r",
            job_id,
            key_index,
            model,
            proc.returncode,
            stdout[-4000:],
            stderr[-4000:],
        )
        consecutive_rate_limit_releases = 0
        client.hincrby(_key(config, "model_failed"), model, 1)
        retries = client.hincrby(_key(config, "retries"), job_id, 1)
        if is_captions:
            needs_work = _captions_needs_translation(config)
        else:
            assert source_path is not None
            needs_work = _chapter_needs_work(config, source_path, chapter_num)
        if retries < config.queue.max_retries and (is_force or needs_work):
            if client.sadd(_key(config, "queued"), job_id):
                client.rpush(_key(config, "pending"), job_id)
        else:
            LOGGER.error("Worker gave up on %s after %s retries", job_id, retries)


def run_supervisor(config: NovelConfig) -> int:
    client = _client(config)
    while True:
        launched = _ensure_worker_processes(config)
        _enqueue_needed_jobs(config, client)
        _requeue_stale_inflight(config, client)
        LOGGER.info(
            "queue pending=%s queued=%s inflight=%s done=%s launched_workers=%s",
            client.llen(_key(config, "pending")),
            client.scard(_key(config, "queued")),
            client.hlen(_key(config, "inflight")),
            client.hlen(_key(config, "done")),
            launched,
        )
        time.sleep(config.queue.supervisor_interval_seconds)


def run_status_monitor(config: NovelConfig) -> int:
    client = _client(config)
    last_snapshot: dict[str, int] | None = None
    was_idle = False
    while True:
        # Consider the queue "idle" when there's nothing pending/queued/inflight.
        # We still update the state json (for ps/monitoring), but stop appending status.log to avoid noise.
        inflight = client.hlen(_key(config, "inflight"))
        pending = client.llen(_key(config, "pending"))
        queued = client.scard(_key(config, "queued"))
        is_idle = (pending == 0) and (queued == 0) and (inflight == 0)

        # Write one line on the transition into idle, then stay quiet until work resumes.
        append_log = (not is_idle) or (not was_idle)
        last_snapshot = _write_status_line(config, client, last_snapshot, append_log=append_log)
        was_idle = is_idle
        time.sleep(config.queue.status_interval_seconds)


def _spawn_process(cmd: list[str], log_path: Path, cwd: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as _:
        pass
    devnull = open(os.devnull, "w", encoding="utf-8")
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=devnull,
        stderr=devnull,
        start_new_session=True,
        text=True,
    )
    devnull.close()
    return process.pid


def _worker_log_path(config: NovelConfig, key_index: int, model: str, worker_idx: int) -> Path:
    safe_model = model.replace("-", "_")
    return get_novel_log_path(
        config.storage.logs_dir,
        config.novel_id,
        f"queue/workers/k{key_index}-{safe_model}-w{worker_idx}.log",
    )


def _worker_command(config: NovelConfig, key_index: int, model: str, worker_idx: int) -> tuple[list[str], Path]:
    worker_log = _worker_log_path(config, key_index, model, worker_idx)
    cmd = [
        sys.executable,
        "-m",
        "novel_tts",
        "--log-file",
        str(worker_log),
        "queue",
        "worker",
        config.novel_id,
        "--key-index",
        str(key_index),
        "--model",
        model,
    ]
    return cmd, worker_log


def _matching_worker_pids(config: NovelConfig, key_index: int, model: str) -> list[int]:
    pattern = (
        f"novel_tts --log-file .* queue worker {config.novel_id} "
        f"--key-index {key_index} --model {model}"
    )
    proc = subprocess.run(
        ["pgrep", "-f", pattern],
        cwd=str(config.storage.root),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return []
    pids: list[int] = []
    for line in (proc.stdout or "").splitlines():
        value = line.strip()
        if not value:
            continue
        try:
            pids.append(int(value))
        except ValueError:
            continue
    return pids


def _reap_unwanted_worker_processes(config: NovelConfig, *, max_key_index: int, worker_models: list[str]) -> int:
    """
    Supervisor reconciliation:
    - If the keys file shrinks, stop workers whose --key-index is now out of range.
    - If enabled models change or worker_count decreases, stop extra workers.

    This prevents "orphan" workers from continuing to run after config/keys updates.
    """
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=,command="],
            cwd=str(config.storage.root),
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return 0
    if proc.returncode != 0:
        return 0

    enabled_models = set(worker_models or [])
    by_group: dict[tuple[int, str], list[int]] = {}
    killed = 0

    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, cmd = line.split(None, 1)
        except ValueError:
            continue
        if "novel_tts" not in cmd or f"queue worker {config.novel_id}" not in cmd:
            continue
        try:
            pid = int(pid_str.strip())
        except ValueError:
            continue

        try:
            argv = shlex.split(cmd)
        except Exception:
            argv = cmd.split()

        key_index: int | None = None
        model = ""
        for idx, token in enumerate(argv):
            if token == "--key-index" and idx + 1 < len(argv):
                try:
                    key_index = int(str(argv[idx + 1]).strip())
                except Exception:
                    key_index = None
            elif token == "--model" and idx + 1 < len(argv):
                model = str(argv[idx + 1]).strip()

        if key_index is None or not model:
            continue

        if key_index > max_key_index:
            subprocess.run(["kill", str(pid)], cwd=str(config.storage.root), check=False)
            killed += 1
            continue

        if enabled_models and model not in enabled_models:
            subprocess.run(["kill", str(pid)], cwd=str(config.storage.root), check=False)
            killed += 1
            continue

        by_group.setdefault((key_index, model), []).append(pid)

    for (key_index, model), pids in by_group.items():
        model_cfg = config.queue.model_configs.get(model)
        desired = max(0, int(model_cfg.worker_count if model_cfg else 1))
        if len(pids) <= desired:
            continue
        for pid in sorted(pids)[desired:]:
            subprocess.run(["kill", str(pid)], cwd=str(config.storage.root), check=False)
            killed += 1

    return killed


def _ensure_worker_processes(config: NovelConfig) -> int:
    keys = _load_keys(config)
    worker_models = config.queue.enabled_models or ["gemma-3-27b-it", "gemma-3-12b-it"]
    _reap_unwanted_worker_processes(config, max_key_index=len(keys), worker_models=worker_models)
    launched = 0
    for key_index in range(1, len(keys) + 1):
        for model in worker_models:
            model_cfg = config.queue.model_configs.get(model)
            worker_count = max(0, int(model_cfg.worker_count if model_cfg else 1))
            running = len(_matching_worker_pids(config, key_index, model))
            for worker_idx in range(running + 1, worker_count + 1):
                cmd, worker_log = _worker_command(config, key_index, model, worker_idx)
                pid = _spawn_process(cmd, worker_log, config.storage.root)
                launched += 1
                LOGGER.info(
                    "Launched worker pid=%s key_index=%s model=%s worker_idx=%s log=%s",
                    pid,
                    key_index,
                    model,
                    worker_idx,
                    worker_log,
                )
    return launched


def launch_queue_stack(config: NovelConfig, restart: bool = False) -> int:
    keys = _load_keys(config)
    client = _client(config)
    if restart:
        patterns = [
            f"queue supervisor {config.novel_id}",
            f"queue monitor {config.novel_id}",
            f"queue worker {config.novel_id}",
            f"translate chapter {config.novel_id}",
        ]
        for pattern in patterns:
            subprocess.run(["pkill", "-f", pattern], cwd=str(config.storage.root), check=False)
        client.delete(
            _key(config, "pending"),
            _key(config, "queued"),
            _key(config, "inflight"),
            _key(config, "done"),
            _key(config, "retries"),
            _key(config, "force"),
            _key(config, "model_done"),
            _key(config, "model_failed"),
        )
        status_log, state_log = _status_paths(config)
        for path in (status_log, state_log):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                LOGGER.warning("Failed to remove status artifact on restart: %s", path)
        time.sleep(1)

    supervisor_log = get_novel_log_path(config.storage.logs_dir, config.novel_id, "queue/supervisor.log")
    supervisor_pid = _spawn_process(
        [
            sys.executable,
            "-m",
            "novel_tts",
            "--log-file",
            str(supervisor_log),
            "queue",
            "supervisor",
            config.novel_id,
        ],
        supervisor_log,
        config.storage.root,
    )
    LOGGER.info("Launched supervisor pid=%s log=%s", supervisor_pid, supervisor_log)
    status_log = get_novel_log_path(config.storage.logs_dir, config.novel_id, "queue/monitor.log")
    status_pid = _spawn_process(
        [
            sys.executable,
            "-m",
            "novel_tts",
            "--log-file",
            str(status_log),
            "queue",
            "monitor",
            config.novel_id,
        ],
        status_log,
        config.storage.root,
    )
    LOGGER.info("Launched status monitor pid=%s log=%s", status_pid, status_log)

    # Workers are spawned by the supervisor on its first loop iteration.
    # Do NOT call _ensure_worker_processes here to avoid a race condition
    # where both launch_queue_stack and the supervisor spawn workers simultaneously.
    LOGGER.info(
        "Queue stack launched | novel=%s keys=%s supervisor=%s monitor=%s (supervisor will spawn workers)",
        config.novel_id,
        len(keys),
        supervisor_pid,
        status_pid,
    )
    return 0


def list_queue_processes(config: NovelConfig, include_all: bool = False) -> int:
    """List queue-related processes for a novel in a pm2-like summary, plus progress."""
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=,ppid=,command="],
            cwd=str(config.storage.root),
            check=False,
            capture_output=True,
            text=True,
        )
    except PermissionError as exc:
        LOGGER.error("Unable to run ps to list processes: %s", exc)
        return 1
    if proc.returncode != 0:
        LOGGER.error("Unable to run ps ax to list processes")
        return 1

    lines = (proc.stdout or "").splitlines()
    rows: list[dict[str, str]] = []
    ppid_by_pid: dict[str, str] = {}
    worker_meta_by_pid: dict[str, dict[str, str]] = {}
    novel_token = f" {config.novel_id}"

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, ppid_str, cmd = line.split(None, 2)
        except ValueError:
            continue
        ppid_by_pid[pid_str.strip()] = ppid_str.strip()
        if "novel_tts" not in cmd or novel_token not in cmd:
            continue

        try:
            argv = shlex.split(cmd)
        except Exception:
            argv = cmd.split()

        role = ""
        key_index = ""
        model = ""
        log_file = ""

        # Queue commands for this novel.
        if "queue" in argv:
            q_idx = argv.index("queue")
            if q_idx + 2 < len(argv) and argv[q_idx + 2] == config.novel_id:
                subcmd = argv[q_idx + 1] if q_idx + 1 < len(argv) else ""
                if subcmd == "supervisor":
                    role = "supervisor"
                elif subcmd == "monitor":
                    role = "monitor"
                elif subcmd == "worker":
                    role = "worker"
                else:
                    role = subcmd or "queue"

        # Translate chapter subprocesses for this novel.
        if not role and "translate" in argv:
            t_idx = argv.index("translate")
            if t_idx + 2 < len(argv) and argv[t_idx + 1] == "chapter" and argv[t_idx + 2] == config.novel_id:
                role = "translate-chapter"

        if not role:
            continue

        for idx, token in enumerate(argv):
            if token == "--log-file" and idx + 1 < len(argv):
                log_file = argv[idx + 1]
            elif token == "--key-index" and idx + 1 < len(argv):
                key_index = argv[idx + 1]
            elif token == "--model" and idx + 1 < len(argv):
                model = argv[idx + 1]

        pid = pid_str.strip()
        rows.append(
            {
                "pid": pid,
                "ppid": ppid_str.strip(),
                "role": role,
                "key_index": key_index,
                "model": model,
                "log_file": log_file,
                "state": "",
                "countdown": "",
            }
        )
        if role == "worker":
            worker_meta_by_pid[pid] = {"key_index": key_index, "model": model}

    def _inherit_worker_meta(pid: str) -> dict[str, str] | None:
        cursor = pid
        for _ in range(6):
            if not cursor:
                break
            meta = worker_meta_by_pid.get(cursor)
            if meta and meta.get("key_index") and meta.get("model"):
                return meta
            cursor = ppid_by_pid.get(cursor, "")
        return None

    def _infer_from_log_path(path: str) -> dict[str, str] | None:
        # Example: .../queue/workers/k1-gemma_3_27b_it-w2.log
        base = os.path.basename(path or "")
        match = re.search(r"^k(?P<key>\d+)-(?P<model>.+?)(?:-w\d+)?\.log$", base)
        if not match:
            return None
        key = match.group("key")
        model_guess = match.group("model").replace("_", "-")
        return {"key_index": key, "model": model_guess}

    for row in rows:
        if row["role"] != "translate-chapter":
            continue
        if (not row["key_index"]) or (not row["model"]):
            inherited = _inherit_worker_meta(row.get("ppid", ""))
            if inherited:
                row["key_index"] = row["key_index"] or inherited.get("key_index", "")
                row["model"] = row["model"] or inherited.get("model", "")
        if (not row["key_index"]) or (not row["model"]):
            inferred = _infer_from_log_path(row.get("log_file", ""))
            if inferred:
                row["key_index"] = row["key_index"] or inferred.get("key_index", "")
                row["model"] = row["model"] or inferred.get("model", "")

    children_by_ppid: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        children_by_ppid.setdefault(row.get("ppid", ""), []).append(row)

    for row in rows:
        role = row.get("role", "")
        pid = row.get("pid", "")
        is_busy = False
        if role == "worker":
            for child in children_by_ppid.get(pid, []):
                if child.get("role") == "translate-chapter":
                    is_busy = True
                    break
        state, countdown = _classify_process_state(role, is_busy=is_busy, log_file=row.get("log_file", ""))
        row["state"] = state
        row["countdown"] = str(int(math.ceil(countdown))) if countdown is not None and countdown > 0 else ""

    # If a worker has translate-chapter children, align the worker's state with the child's effective state.
    for row in rows:
        if row.get("role") != "worker":
            continue
        pid = row.get("pid", "")
        children = [c for c in children_by_ppid.get(pid, []) if c.get("role") == "translate-chapter"]
        combined_state, combined_countdown = _combine_worker_child_states_with_countdown(children)
        if combined_state:
            row["state"] = combined_state
            row["countdown"] = (
                str(int(math.ceil(combined_countdown)))
                if combined_countdown is not None and combined_countdown > 0
                else ""
            )

    def _truncate_middle(value: str, max_len: int) -> str:
        value = value or ""
        if max_len <= 0 or len(value) <= max_len:
            return value
        head = max(1, (max_len - 3) // 2)
        tail = max(1, max_len - 3 - head)
        return value[:head] + "..." + value[-tail:]

    def _format_log_path(path: str) -> str:
        raw = path or ""
        if not raw:
            return ""
        try:
            root = str(config.storage.root)
            if raw.startswith(root + os.sep):
                return os.path.relpath(raw, root)
        except Exception:
            pass
        marker = f"{os.sep}.logs{os.sep}"
        idx = raw.find(marker)
        if idx >= 0:
            return raw[idx + 1 :]
        try:
            cwd = os.getcwd()
            if raw.startswith(cwd + os.sep):
                return os.path.relpath(raw, cwd)
        except Exception:
            pass
        return os.path.basename(raw)

    def _render_table(rows: list[dict[str, str]]) -> None:
        headers = ["PID", "ROLE", "KEY", "STATE", "COUNTDOWN", "MODEL", "LOG"]
        # Keep the table readable by truncating the log path in the rendered view.
        display_rows = []
        def _countdown_display(raw: str) -> str:
            raw = (raw or "").strip()
            if not raw:
                return ""
            try:
                return _format_countdown(float(raw))
            except Exception:
                return ""
        for r in rows:
            display_rows.append(
                {
                    "PID": r.get("pid", ""),
                    "ROLE": r.get("role", ""),
                    "KEY": r.get("key_index", "") or "",
                    "STATE": r.get("state", ""),
                    "COUNTDOWN": _countdown_display(r.get("countdown", "")),
                    "MODEL": r.get("model", "") or "",
                    "LOG": _truncate_middle(_format_log_path(r.get("log_file", "")), 110),
                }
            )

        widths: dict[str, int] = {h: len(h) for h in headers}
        for r in display_rows:
            for h in headers:
                widths[h] = max(widths[h], len(r.get(h, "")))

        def _hr() -> str:
            return "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"

        def _row(values: dict[str, str]) -> str:
            cells = []
            for h in headers:
                val = values.get(h, "")
                if h in {"PID", "KEY"}:
                    cells.append(val.rjust(widths[h]))
                else:
                    cells.append(val.ljust(widths[h]))
            return "| " + " | ".join(cells) + " |"

        print(_hr())
        print(_row({h: h for h in headers}))
        print(_hr())
        for r in display_rows:
            print(_row(r))
        print(_hr())

    if rows:
        def _role_rank(role: str) -> int:
            order = {
                "supervisor": 0,
                "monitor": 1,
                "worker": 2,
                "translate-chapter": 3,
            }
            return order.get(role or "", 9)

        def _safe_int(value: str, default: int = 10**12) -> int:
            try:
                return int(str(value).strip())
            except Exception:
                return default

        rows.sort(
            key=lambda r: (
                _role_rank(r.get("role", "")),
                _safe_int(r.get("key_index", ""), default=10**12),
                r.get("model", "") or "",
                _safe_int(r.get("pid", ""), default=10**12),
            )
        )
        render_rows = rows if include_all else [r for r in rows if r.get("role") != "translate-chapter"]
        _render_table(render_rows)
    else:
        print(f"No queue processes found for novel {config.novel_id}")

    # Try to show the latest progress snapshot, if available.
    _status_log, state_log = _status_paths(config)
    if state_log.exists():
        try:
            snapshot = json.loads(state_log.read_text(encoding="utf-8"))
        except Exception as exc:
            LOGGER.warning("Unable to read queue status state for %s: %s", config.novel_id, exc)
        else:
            origin_files = snapshot.get("origin_files", 0)
            translated_files = snapshot.get("translated_files", 0)
            parts = snapshot.get("parts", 0)
            chapter_total = snapshot.get("chapter_total", 0)
            pending = snapshot.get("pending", 0)
            queued = snapshot.get("queued", 0)
            inflight = snapshot.get("inflight", 0)
            retries = snapshot.get("retries", 0)
            done = snapshot.get("done", 0)
            done_pct = (translated_files / origin_files * 100.0) if origin_files else 0.0
            part_pct = (parts / chapter_total * 100.0) if chapter_total else 0.0
            eta_files = snapshot.get("eta_files") or ""
            eta_parts = snapshot.get("eta_parts") or ""

            print()
            print(f"Progress for novel {config.novel_id}:")
            print(
                f"  files: {translated_files}/{origin_files} ({done_pct:.2f}%)"
                f" | chapters: {parts}/{chapter_total} ({part_pct:.2f}%)"
            )
            print(
                f"  queue: pending={pending} queued={queued} inflight={inflight}"
                f" done={done} retries={retries}"
            )
            if eta_files or eta_parts:
                print(f"  ETA: files={eta_files or 'unknown'} chapters={eta_parts or 'unknown'}")

    return 0


def list_all_queue_processes(include_all: bool = False) -> int:
    """List queue-related processes for all novels, grouped by novel."""
    try:
        proc = subprocess.run(
            ["ps", "ax", "-o", "pid=,ppid=,command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except PermissionError as exc:
        LOGGER.error("Unable to run ps to list processes: %s", exc)
        return 1
    if proc.returncode != 0:
        LOGGER.error("Unable to run ps ax to list processes")
        return 1

    lines = (proc.stdout or "").splitlines()
    by_novel: dict[str, list[dict[str, str]]] = {}
    ppid_by_pid: dict[str, str] = {}
    worker_meta_by_pid: dict[str, dict[str, str]] = {}

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, ppid_str, cmd = line.split(None, 2)
        except ValueError:
            continue
        pid = pid_str.strip()
        ppid_by_pid[pid] = ppid_str.strip()
        if "novel_tts" not in cmd:
            continue

        try:
            argv = shlex.split(cmd)
        except Exception:
            argv = cmd.split()

        role = ""
        novel_id = ""

        # Queue commands: novel id is after "queue <subcmd> <novel_id>"
        if "queue" in argv:
            q_idx = argv.index("queue")
            if q_idx + 2 < len(argv):
                subcmd = argv[q_idx + 1]
                novel_id = argv[q_idx + 2]
                if subcmd in {"supervisor", "monitor", "worker", "launch"}:
                    if subcmd == "supervisor":
                        role = "supervisor"
                    elif subcmd == "monitor":
                        role = "monitor"
                    elif subcmd == "worker":
                        role = "worker"
                    elif subcmd == "launch":
                        role = "launcher"

        # Translate chapter subprocesses: novel id is after "translate chapter <novel_id>"
        if not role and "translate" in argv:
            t_idx = argv.index("translate")
            if t_idx + 2 < len(argv) and argv[t_idx + 1] == "chapter":
                novel_id = argv[t_idx + 2]
                role = "translate-chapter"

        if not role or not novel_id:
            continue

        key_index = ""
        model = ""
        log_file = ""
        target = ""
        for idx, token in enumerate(argv):
            if token == "--log-file" and idx + 1 < len(argv):
                log_file = argv[idx + 1]
            elif token == "--key-index" and idx + 1 < len(argv):
                key_index = argv[idx + 1]
            elif token == "--model" and idx + 1 < len(argv):
                model = argv[idx + 1]

        if role == "translate-chapter":
            target = _extract_target_from_argv(argv)

        by_novel.setdefault(novel_id, []).append(
            {
                "pid": pid,
                "ppid": ppid_str.strip(),
                "role": role,
                "key_index": key_index,
                "model": model,
                "log_file": log_file,
                "state": "",
                "countdown": "",
                "target": target,
            }
        )
        if role == "worker":
            worker_meta_by_pid[pid] = {"key_index": key_index, "model": model}

    if not by_novel:
        print("No queue processes found for any novel")
        return 0

    def _inherit_worker_meta(pid: str) -> dict[str, str] | None:
        cursor = pid
        for _ in range(6):
            if not cursor:
                break
            meta = worker_meta_by_pid.get(cursor)
            if meta and meta.get("key_index") and meta.get("model"):
                return meta
            cursor = ppid_by_pid.get(cursor, "")
        return None

    def _infer_from_log_path(path: str) -> dict[str, str] | None:
        base = os.path.basename(path or "")
        match = re.search(r"^k(?P<key>\d+)-(?P<model>.+?)(?:-w\d+)?\.log$", base)
        if not match:
            return None
        return {"key_index": match.group("key"), "model": match.group("model").replace("_", "-")}

    def _truncate_middle(value: str, max_len: int) -> str:
        value = value or ""
        if max_len <= 0 or len(value) <= max_len:
            return value
        head = max(1, (max_len - 3) // 2)
        tail = max(1, max_len - 3 - head)
        return value[:head] + "..." + value[-tail:]

    def _format_log_path(path: str) -> str:
        raw = path or ""
        if not raw:
            return ""
        marker = f"{os.sep}.logs{os.sep}"
        idx = raw.find(marker)
        if idx >= 0:
            return raw[idx + 1 :]
        try:
            cwd = os.getcwd()
            if raw.startswith(cwd + os.sep):
                return os.path.relpath(raw, cwd)
        except Exception:
            pass
        return os.path.basename(raw)

    def _render_table(rows: list[dict[str, str]]) -> None:
        headers = ["PID", "ROLE", "KEY", "STATE", "COUNTDOWN", "TARGET", "MODEL", "LOG"]
        display_rows = []
        def _countdown_display(raw: str) -> str:
            raw = (raw or "").strip()
            if not raw:
                return ""
            try:
                return _format_countdown(float(raw))
            except Exception:
                return ""
        for r in rows:
            display_rows.append(
                {
                    "PID": r.get("pid", ""),
                    "ROLE": r.get("role", ""),
                    "KEY": r.get("key_index", "") or "",
                    "STATE": r.get("state", ""),
                    "COUNTDOWN": _countdown_display(r.get("countdown", "")),
                    "TARGET": r.get("target", "") or "",
                    "MODEL": r.get("model", "") or "",
                    "LOG": _truncate_middle(_format_log_path(r.get("log_file", "")), 110),
                }
            )
        widths: dict[str, int] = {h: len(h) for h in headers}
        for r in display_rows:
            for h in headers:
                widths[h] = max(widths[h], len(r.get(h, "")))

        def _hr() -> str:
            return "+-" + "-+-".join("-" * widths[h] for h in headers) + "-+"

        def _row(values: dict[str, str]) -> str:
            cells = []
            for h in headers:
                val = values.get(h, "")
                if h in {"PID", "KEY"}:
                    cells.append(val.rjust(widths[h]))
                else:
                    cells.append(val.ljust(widths[h]))
            return "| " + " | ".join(cells) + " |"

        print(_hr())
        print(_row({h: h for h in headers}))
        print(_hr())
        for r in display_rows:
            print(_row(r))
        print(_hr())

    for novel_id, rows in sorted(by_novel.items(), key=lambda item: item[0]):
        for row in rows:
            if row["role"] != "translate-chapter":
                continue
            if (not row["key_index"]) or (not row["model"]):
                inherited = _inherit_worker_meta(row.get("ppid", ""))
                if inherited:
                    row["key_index"] = row["key_index"] or inherited.get("key_index", "")
                    row["model"] = row["model"] or inherited.get("model", "")
            if (not row["key_index"]) or (not row["model"]):
                inferred = _infer_from_log_path(row.get("log_file", ""))
                if inferred:
                    row["key_index"] = row["key_index"] or inferred.get("key_index", "")
                    row["model"] = row["model"] or inferred.get("model", "")

        children_by_ppid: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            children_by_ppid.setdefault(row.get("ppid", ""), []).append(row)

        for row in rows:
            role = row.get("role", "")
            pid = row.get("pid", "")
            is_busy = False
            if role == "worker":
                children = [c for c in children_by_ppid.get(pid, []) if c.get("role") == "translate-chapter"]
                if children:
                    is_busy = True
                    # Surface an active target on the worker even when children are hidden.
                    children.sort(key=lambda c: int(c.get("pid") or 10**12))
                    row["target"] = (children[0].get("target") or "").strip()
            state, countdown = _classify_process_state(role, is_busy=is_busy, log_file=row.get("log_file", ""))
            row["state"] = state
            row["countdown"] = str(int(math.ceil(countdown))) if countdown is not None and countdown > 0 else ""

        for row in rows:
            if row.get("role") != "worker":
                continue
            pid = row.get("pid", "")
            children = [c for c in children_by_ppid.get(pid, []) if c.get("role") == "translate-chapter"]
            combined_state, combined_countdown = _combine_worker_child_states_with_countdown(children)
            if combined_state:
                row["state"] = combined_state
                row["countdown"] = (
                    str(int(math.ceil(combined_countdown)))
                    if combined_countdown is not None and combined_countdown > 0
                    else ""
                )

        pending = queued = inflight = retries = done = 0
        loaded = False

        # Prefer live Redis counts so the line matches what workers are doing right now.
        try:
            from novel_tts.config.loader import load_novel_config

            config = load_novel_config(novel_id)
            client = _client(config)
            pending = int(client.llen(_key(config, "pending")) or 0)
            queued = int(client.scard(_key(config, "queued")) or 0)
            inflight = int(client.hlen(_key(config, "inflight")) or 0)
            retries = int(client.hlen(_key(config, "retries")) or 0)
            done = int(client.hlen(_key(config, "done")) or 0)
            loaded = True
        except Exception:
            loaded = False

        # Fallback: read the last monitor snapshot from disk if Redis/config isn't available.
        if not loaded:
            state_paths: list[Path] = []
            for r in rows:
                raw = r.get("log_file", "") or ""
                if not raw:
                    continue
                for marker in (f"{os.sep}.logs{os.sep}", f"{os.sep}logs{os.sep}"):
                    idx = raw.find(marker)
                    if idx < 0:
                        continue
                    logs_root = Path(raw[: idx + len(marker) - 1])
                    state_paths.append(logs_root / novel_id / "queue" / "status.state.json")
            # Fallback to cwd layout (repo root).
            try:
                state_paths.append(Path(os.getcwd()) / ".logs" / novel_id / "queue" / "status.state.json")
                state_paths.append(Path(os.getcwd()) / "logs" / novel_id / "queue" / "status.state.json")
            except Exception:
                pass

            for path in state_paths:
                try:
                    if not path.exists():
                        continue
                    snapshot = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                pending = int(snapshot.get("pending", 0) or 0)
                queued = int(snapshot.get("queued", 0) or 0)
                inflight = int(snapshot.get("inflight", 0) or 0)
                retries = int(snapshot.get("retries", 0) or 0)
                done = int(snapshot.get("done", 0) or 0)
                break

        print(
            f"\nNovel {novel_id}:"
            f" pending={pending} queued={queued} inflight={inflight} retries={retries} done={done}"
        )
        def _role_rank(role: str) -> int:
            order = {
                "supervisor": 0,
                "monitor": 1,
                "worker": 2,
                "translate-chapter": 3,
                "launcher": 4,
            }
            return order.get(role or "", 9)

        def _safe_int(value: str, default: int = 10**12) -> int:
            try:
                return int(str(value).strip())
            except Exception:
                return default

        rows.sort(
            key=lambda r: (
                _role_rank(r.get("role", "")),
                _safe_int(r.get("key_index", ""), default=10**12),
                r.get("model", "") or "",
                _safe_int(r.get("pid", ""), default=10**12),
            )
        )
        render_rows = rows if include_all else [r for r in rows if r.get("role") != "translate-chapter"]
        _render_table(render_rows)

    return 0


def stop_queue_processes(config: NovelConfig, pids: list[int] | None = None, roles: list[str] | None = None) -> int:
    """Stop queue-related processes for a novel.

    - If pids is provided, only those PIDs are stopped.
    - Otherwise, processes are stopped by role(s) (or all roles if roles is None/empty).
    """
    if pids:
        for pid in pids:
            subprocess.run(["kill", str(pid)], cwd=str(config.storage.root), check=False)
            LOGGER.info("Sent SIGTERM to pid=%s for novel=%s", pid, config.novel_id)
        return 0

    selected = {r.strip() for r in (roles or []) if r.strip()} or None

    patterns: list[str] = []
    if selected is None or "supervisor" in selected:
        patterns.append(f"queue supervisor {config.novel_id}")
    if selected is None or "monitor" in selected:
        patterns.append(f"queue monitor {config.novel_id}")
    if selected is None or "worker" in selected:
        patterns.append(f"queue worker {config.novel_id}")
    if selected is None or "translate-chapter" in selected:
        patterns.append(f"translate chapter {config.novel_id}")

    for pattern in patterns:
        subprocess.run(["pkill", "-f", pattern], cwd=str(config.storage.root), check=False)
    LOGGER.info("Stopped queue processes for novel=%s roles=%s", config.novel_id, ", ".join(sorted(selected or [])) or "all")
    return 0
