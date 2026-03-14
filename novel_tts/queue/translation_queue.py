from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from novel_tts.common.logging import get_logger, get_novel_log_path
from novel_tts.config.models import NovelConfig
from novel_tts.translate.novel import chapter_part_path, load_source_chapters

LOGGER = get_logger(__name__)


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


def _parse_job_id(job_id: str) -> tuple[str, str]:
    file_name, chapter_num = job_id.split("::", 1)
    return file_name, str(int(chapter_num))


def _chapter_needs_translation(config: NovelConfig, source_path: Path, chapter_num: str) -> bool:
    part_path = chapter_part_path(config, source_path, chapter_num)
    if not part_path.exists():
        return True
    if _needs_translation(config, source_path) is False:
        return False
    return part_path.stat().st_mtime < source_path.stat().st_mtime


def _chapter_jobs_for_file(config: NovelConfig, source_path: Path) -> list[str]:
    jobs: list[str] = []
    for chapter_num, _chapter_text in load_source_chapters(config, source_path):
        if _chapter_needs_translation(config, source_path, chapter_num):
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


def _write_status_line(config: NovelConfig, client, last_snapshot: dict[str, int] | None) -> dict[str, int]:
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
    if last_snapshot:
        delta_s = snapshot["ts"] - last_snapshot["ts"]
        if delta_s > 0:
            files_per_min = (snapshot["translated_files"] - last_snapshot["translated_files"]) * 60 / delta_s
            parts_per_min = (snapshot["parts"] - last_snapshot["parts"]) * 60 / delta_s
            if files_per_min > 0 and snapshot["origin_files"] > snapshot["translated_files"]:
                minutes = (snapshot["origin_files"] - snapshot["translated_files"]) / files_per_min
                eta_files = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
            if parts_per_min > 0 and snapshot["chapter_total"] > snapshot["parts"]:
                minutes = (snapshot["chapter_total"] - snapshot["parts"]) / parts_per_min
                eta_parts = datetime.fromtimestamp(time.time() + minutes * 60).strftime("%Y-%m-%d %H:%M:%S")
    done_pct = (snapshot["translated_files"] / snapshot["origin_files"] * 100) if snapshot["origin_files"] else 0.0
    part_pct = (snapshot["parts"] / snapshot["chapter_total"] * 100) if snapshot["chapter_total"] else 0.0
    line = (
        f"translated={snapshot['translated_files']}/{snapshot['origin_files']} "
        f"| done={done_pct:.2f}% | parts={snapshot['parts']}/{snapshot['chapter_total']} "
        f"| part_done={part_pct:.2f}% | files/min={files_per_min:.2f} | parts/min={parts_per_min:.2f} "
        f"| ETA_files={eta_files} | ETA_parts={eta_parts} | checkpoints={snapshot['checkpoints']} "
        f"| retries={snapshot['retries']} | pending={snapshot['pending']} | queued={snapshot['queued']} "
        f"| inflight={snapshot['inflight']} | workers={snapshot['inflight']} "
        f"| inflight_by_model={snapshot['inflight_by_model']} "
        f"| done_by_model={snapshot['done_by_model']} | failed_by_model={snapshot['failed_by_model']}"
    )
    status_log.parent.mkdir(parents=True, exist_ok=True)
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
        file_name, chapter_num = _parse_job_id(job_id)
        source_path = config.storage.origin_dir / file_name
        if not source_path.exists() or not _chapter_needs_translation(config, source_path, chapter_num):
            continue
        client.hset(
            _key(config, "inflight"),
            job_id,
            json.dumps(
                {
                    "worker": worker_id,
                    "started_at": time.time(),
                    "model": model,
                    "file_name": file_name,
                    "chapter_num": chapter_num,
                }
            ),
        )
        env = os.environ.copy()
        env["GEMINI_API_KEY"] = api_key
        env["GEMINI_MODEL"] = model
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
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "novel_tts",
                "translate",
                "chapter",
                config.novel_id,
                "--force",
                "--file",
                file_name,
                "--chapter",
                chapter_num,
            ],
            cwd=str(config.storage.root),
            env=env,
            capture_output=True,
            text=True,
        )
        client.hdel(_key(config, "inflight"), job_id)
        if proc.returncode == 0:
            client.hdel(_key(config, "retries"), job_id)
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
        client.hincrby(_key(config, "model_failed"), model, 1)
        retries = client.hincrby(_key(config, "retries"), job_id, 1)
        if retries < config.queue.max_retries and _chapter_needs_translation(config, source_path, chapter_num):
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
    while True:
        last_snapshot = _write_status_line(config, client, last_snapshot)
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


def _ensure_worker_processes(config: NovelConfig) -> int:
    keys = _load_keys(config)
    worker_models = config.queue.enabled_models or ["gemma-3-27b-it", "gemma-3-12b-it"]
    launched = 0
    for key_index in range(1, len(keys) + 1):
        for model in worker_models:
            model_cfg = config.queue.model_configs.get(model)
            worker_count = max(1, int(model_cfg.worker_count if model_cfg else 1))
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

    launched = 2
    launched += _ensure_worker_processes(config)
    LOGGER.info(
        "Queue stack launched | novel=%s keys=%s workers_per_key=%s total_processes=%s",
        config.novel_id,
        len(keys),
        len(config.queue.enabled_models or ["gemma-3-27b-it", "gemma-3-12b-it"]),
        launched,
    )
    return 0


def list_queue_processes(config: NovelConfig, include_all: bool = False) -> int:
    """List queue-related processes for a novel in a pm2-like summary, plus progress."""
    proc = subprocess.run(
        ["ps", "ax", "-o", "pid=,command="],
        cwd=str(config.storage.root),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        LOGGER.error("Unable to run ps ax to list processes")
        return 1

    lines = (proc.stdout or "").splitlines()
    rows: list[dict[str, str]] = []
    novel_token = f" {config.novel_id}"

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, cmd = line.split(" ", 1)
        except ValueError:
            continue
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

        if role == "translate-chapter" and not include_all:
            continue

        rows.append(
            {
                "pid": pid_str.strip(),
                "role": role,
                "key_index": key_index,
                "model": model,
                "log_file": log_file,
            }
        )

    # Simple fixed-width table for processes
    if rows:
        headers = ["PID", "ROLE", "KEY", "MODEL", "LOG"]
        print("{:>6}  {:>10}  {:>3}  {:>24}  {}".format(*headers))
        for row in rows:
            print(
                "{pid:>6}  {role:>10}  {key:>3}  {model:>24}  {log}".format(
                    pid=row["pid"],
                    role=row["role"],
                    key=row["key_index"],
                    model=row["model"],
                    log=row["log_file"],
                )
            )
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
    proc = subprocess.run(
        ["ps", "ax", "-o", "pid=,command="],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        LOGGER.error("Unable to run ps ax to list processes")
        return 1

    lines = (proc.stdout or "").splitlines()
    by_novel: dict[str, list[dict[str, str]]] = {}

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            pid_str, cmd = line.split(" ", 1)
        except ValueError:
            continue
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

        if role == "translate-chapter" and not include_all:
            continue

        key_index = ""
        model = ""
        log_file = ""
        for idx, token in enumerate(argv):
            if token == "--log-file" and idx + 1 < len(argv):
                log_file = argv[idx + 1]
            elif token == "--key-index" and idx + 1 < len(argv):
                key_index = argv[idx + 1]
            elif token == "--model" and idx + 1 < len(argv):
                model = argv[idx + 1]

        by_novel.setdefault(novel_id, []).append(
            {
                "pid": pid_str.strip(),
                "role": role,
                "key_index": key_index,
                "model": model,
                "log_file": log_file,
            }
        )

    if not by_novel:
        print("No queue processes found for any novel")
        return 0

    headers = ["PID", "ROLE", "KEY", "MODEL", "LOG"]
    for novel_id, rows in sorted(by_novel.items(), key=lambda item: item[0]):
        print(f"\nNovel {novel_id}:")
        print("{:>6}  {:>10}  {:>3}  {:>24}  {}".format(*headers))
        for row in rows:
            print(
                "{pid:>6}  {role:>10}  {key:>3}  {model:>24}  {log}".format(
                    pid=row["pid"],
                    role=row["role"],
                    key=row["key_index"],
                    model=row["model"],
                    log=row["log_file"],
                )
            )

    return 0


def stop_queue_processes(config: NovelConfig, pid: int | None = None, roles: list[str] | None = None) -> int:
    """Stop queue-related processes for a novel.

    - If pid is provided, only that PID is stopped.
    - Otherwise, processes are stopped by role(s) (or all roles if roles is None/empty).
    """
    if pid is not None:
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
