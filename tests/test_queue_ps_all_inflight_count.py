from __future__ import annotations

import json
from types import SimpleNamespace

from novel_tts.config.loader import load_novel_config
from novel_tts.queue import translation_queue


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

    key_prefix = f"{config.queue.redis.prefix}:{config.novel_id}:k1"
    inflight_key = f"{key_prefix}:{model}:quota:alloc:inflight:{pid}"
    eta_key = f"{key_prefix}:{model}:quota:alloc:eta"

    class FakeRedis:
        def scard(self, key: str):
            return 0

        def hlen(self, key: str):
            if key == "inflight":
                return 4
            if key == "retries":
                return 1
            if key == "done":
                return 2
            return 0

        def get(self, key: str):
            if key == inflight_key:
                return json.dumps(
                    {
                        "request_id": "rid",
                        "reply_key": "novel_tts:tram-than:k1:gemma-3-27b-it:quota:alloc:reply:rid",
                        "pid": pid,
                    }
                )
            return None

        def hget(self, key: str, field: str):
            if key == eta_key and field == "rid":
                return "9999999999"
            return None

    monkeypatch.setattr(translation_queue, "_client", lambda _config: FakeRedis())
    monkeypatch.setattr(translation_queue, "_pending_total_len", lambda _config, _client: 0)
    monkeypatch.setattr(translation_queue, "_key", lambda _config, name: name)
    monkeypatch.setattr(translation_queue, "_get_rate_limit_cooldown_remaining_seconds", lambda _client, _key: 0.0)
    monkeypatch.setattr(translation_queue, "_classify_process_state", lambda *_args, **_kwargs: ("idle", None))

    rc = translation_queue.list_all_queue_processes(include_all=False)
    assert rc == 0
    out = capsys.readouterr().out
    assert f"inflight=4" in out
    assert "inflight={'" not in out

