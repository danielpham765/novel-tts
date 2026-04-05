from __future__ import annotations

from types import SimpleNamespace

from novel_tts.config.loader import load_novel_config
from novel_tts.queue import translation_queue


class _Pipe:
    def __init__(self, client) -> None:
        self.client = client

    def srem(self, key: str, job_id: str) -> None:
        self.client.removed.append(("srem", key, job_id))

    def lrem(self, key: str, count: int, job_id: str) -> None:
        self.client.removed.append(("lrem", key, job_id))

    def zrem(self, key: str, job_id: str) -> None:
        self.client.removed.append(("zrem", key, job_id))

    def execute(self) -> None:
        return None


class _SharedQueueRedis:
    def __init__(self, queued: set[str], inflight: set[str]) -> None:
        self.queued = queued
        self.inflight = inflight
        self.removed: list[tuple[str, str, str]] = []

    def smembers(self, key: str):
        return self.queued

    def hkeys(self, key: str):
        return self.inflight

    def pipeline(self):
        return _Pipe(self)


class _NovelCountsRedis:
    def __init__(self, *, retry_len: int, done_len: int) -> None:
        self.retry_len = retry_len
        self.done_len = done_len
        self.seen_hlen_keys: list[str] = []
        self.seen_hgetall_keys: list[str] = []

    def llen(self, key: str) -> int:
        return 0

    def scard(self, key: str) -> int:
        return 0

    def hlen(self, key: str) -> int:
        self.seen_hlen_keys.append(key)
        if key.endswith(":retries"):
            return self.retry_len
        if key.endswith(":done"):
            return self.done_len
        if "inflight" in key:
            return 0
        return 0

    def hgetall(self, key: str) -> dict[str, str]:
        self.seen_hgetall_keys.append(key)
        if key.endswith(":retries"):
            return {"job-a": "1", "job-b": "3"}
        return {}


class _LaunchQueueRedis:
    def __init__(self, ack_raw: str) -> None:
        self.ack_raw = ack_raw
        self.requests: list[tuple[str, str]] = []

    def rpush(self, key: str, payload: str) -> None:
        self.requests.append((key, payload))

    def get(self, key: str):
        return self.ack_raw


def test_queue_remove_all_only_removes_selected_novel_jobs(monkeypatch, capsys) -> None:
    config = load_novel_config("tram-than")
    other_novel = "vo-cuc-thien-ton"
    same_novel_pending = f"{config.novel_id}::chuong_1-10.txt::0001"
    same_novel_inflight = f"{config.novel_id}::chuong_1-10.txt::0002"
    other_novel_pending = f"{other_novel}::chuong_1-10.txt::0003"
    client = _SharedQueueRedis(
        queued={same_novel_pending, same_novel_inflight, other_novel_pending},
        inflight={same_novel_inflight},
    )

    monkeypatch.setattr(translation_queue, "_client", lambda _config: client)

    rc = translation_queue.remove_jobs_from_queue(config, all_pending=True)

    assert rc == 0
    out = capsys.readouterr().out
    assert "Removed 1 job(s)" in out
    removed_job_ids = [job_id for _op, _key, job_id in client.removed]
    assert same_novel_pending in removed_job_ids
    assert same_novel_inflight not in removed_job_ids
    assert other_novel_pending not in removed_job_ids


def test_queue_counts_use_per_novel_shared_keys() -> None:
    config = load_novel_config("tram-than")
    client = _NovelCountsRedis(retry_len=2, done_len=7)

    pending, queued, inflight, retries, exhausted, done = translation_queue._queue_counts_from_redis(config, client)

    assert (pending, queued, inflight, retries, exhausted, done) == (0, 0, 0, 2, 1, 7)
    assert any(key.endswith(f":novel:{config.novel_id}:retries") for key in client.seen_hlen_keys)
    assert any(key.endswith(f":novel:{config.novel_id}:done") for key in client.seen_hlen_keys)
    assert any(key.endswith(f":novel:{config.novel_id}:retries") for key in client.seen_hgetall_keys)


def test_ps_all_prints_inflight_count_not_inflight_meta(monkeypatch, capsys) -> None:
    config = load_novel_config("tram-than")
    model = "gemma-3-27b-it"
    pid = 12345
    ps_out = "\n".join(
        [
            (
                f"{pid} 1 python -m novel_tts translate chapter {config.novel_id}"
                f" --key-index 1 --model {model} --file chuong_1-10.txt --chapter 0001 --log-file /tmp/x.log"
            ),
            "",
        ]
    )

    def fake_run(cmd, *args, **kwargs):
        if cmd[:3] == ["ps", "ax", "-o"]:
            return SimpleNamespace(returncode=0, stdout=ps_out, stderr="")
        raise AssertionError(f"unexpected subprocess.run: {cmd}")

    monkeypatch.setattr(translation_queue.subprocess, "run", fake_run)

    class _Redis:
        def llen(self, key: str) -> int:
            return 0

        def scard(self, key: str) -> int:
            return 0

        def hlen(self, key: str) -> int:
            return 4 if "inflight" in key else 0

        def get(self, key: str):
            return None

        def hget(self, key: str, field: str):
            return None

        def scan(self, cursor, match=None, count=None):
            return (0, [])

        def hgetall(self, key: str):
            return {}

    monkeypatch.setattr(translation_queue, "_client", lambda _config: _Redis())
    monkeypatch.setattr(translation_queue, "_apply_live_redis_overrides", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(translation_queue, "_classify_process_state", lambda *_args, **_kwargs: ("idle", None))

    rc = translation_queue.list_all_queue_processes(include_all=False)
    assert rc == 0
    out = capsys.readouterr().out
    assert "inflight=4" in out
    assert "inflight={'" not in out


def test_reap_workers_stops_out_of_range_and_excess(monkeypatch) -> None:
    config = load_novel_config("tram-than")
    model = "gemma-3-27b-it"
    config.queue.enabled_models = [model]
    config.queue.model_configs[model].worker_count = 1

    ps_out = "\n".join(
        [
            f"19625 python -m novel_tts --log-file /tmp/k8.log queue worker --key-index 8 --model {model}",
            f"19626 python -m novel_tts --log-file /tmp/k1a.log queue worker --key-index 1 --model {model}",
            f"19627 python -m novel_tts --log-file /tmp/k1b.log queue worker --key-index 1 --model {model}",
            "",
        ]
    )

    killed: list[str] = []

    def fake_run(cmd, *args, **kwargs):
        if cmd[:3] == ["ps", "ax", "-o"]:
            return SimpleNamespace(returncode=0, stdout=ps_out, stderr="")
        if cmd[:1] == ["kill"]:
            killed.append(cmd[1])
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"unexpected subprocess.run: {cmd}")

    monkeypatch.setattr(translation_queue.subprocess, "run", fake_run)

    killed_count = translation_queue._reap_unwanted_worker_processes(
        config,
        max_key_index=7,
        worker_models=[model],
    )

    assert killed_count == 2
    assert set(killed) == {"19625", "19627"}


def test_ps_all_renders_empty_table_when_no_processes(monkeypatch, capsys) -> None:
    def fake_run(cmd, *args, **kwargs):
        if cmd[:3] == ["ps", "ax", "-o"]:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        raise AssertionError(f"unexpected subprocess.run: {cmd}")

    monkeypatch.setattr(translation_queue.subprocess, "run", fake_run)
    monkeypatch.setattr(translation_queue, "_client", lambda _config: (_ for _ in ()).throw(RuntimeError("no redis")))

    rc = translation_queue.list_all_queue_processes(include_all=True)

    assert rc == 0
    out = capsys.readouterr().out
    assert "Queue: pending=0 queued=0 inflight=0" in out
    assert "No queue processes found" in out
    assert "PID" in out
    assert "ROLE" in out


def test_queue_launch_continues_when_logrotate_ack_fails(monkeypatch, capsys, tmp_path) -> None:
    config = load_novel_config("tram-than")
    client = _LaunchQueueRedis(
        ack_raw='{"ok": false, "cmd": "rotate_queue_logs", "novel_id": "", "rotated": 0, "request_id": "abc", "ts": 1.0}'
    )
    spawned: list[tuple[list[str], str]] = []
    rotated: list[tuple[str, str]] = []

    monkeypatch.setattr(translation_queue, "_client", lambda _config: client)
    monkeypatch.setattr(translation_queue, "_load_keys", lambda _config: ["k1"])
    monkeypatch.setattr(translation_queue, "_effective_worker_key_limit", lambda _config, total_keys: (1, ""))
    monkeypatch.setattr(translation_queue, "_clear_stopping", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(translation_queue, "_requeue_stale_inflight", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(translation_queue, "_shared_queue_log_dir", lambda: tmp_path / "_shared" / "queue")
    monkeypatch.setattr(
        translation_queue.logrotate,
        "rotate_log_file_to_today",
        lambda *, logs_root, src: rotated.append((str(logs_root), str(src))) or src,
    )
    monkeypatch.setattr(translation_queue.os, "walk", lambda _root: [(str(tmp_path / "_shared" / "queue"), [], ["supervisor.log"])])
    monkeypatch.setattr(
        translation_queue,
        "_spawn_process",
        lambda cmd, log_path, cwd: spawned.append((cmd, str(log_path))) or (1000 + len(spawned)),
    )

    rc = translation_queue.launch_queue_stack(config, restart=False)

    assert rc == 0
    assert len(spawned) == 2
    assert rotated
    err = capsys.readouterr().err
    assert "continuing with local fallback" in err
