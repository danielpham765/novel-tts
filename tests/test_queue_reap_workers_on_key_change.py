from __future__ import annotations

from types import SimpleNamespace

from novel_tts.config.loader import load_novel_config
from novel_tts.queue import translation_queue


def test_reap_workers_stops_out_of_range_and_excess(monkeypatch) -> None:
    config = load_novel_config("tram-than")
    model = "gemma-3-27b-it"
    config.queue.enabled_models = [model]
    config.queue.model_configs[model].worker_count = 1

    ps_out = "\n".join(
        [
            f"19625 python -m novel_tts --log-file /tmp/k8.log queue worker {config.novel_id} --key-index 8 --model {model}",
            f"19626 python -m novel_tts --log-file /tmp/k1a.log queue worker {config.novel_id} --key-index 1 --model {model}",
            f"19627 python -m novel_tts --log-file /tmp/k1b.log queue worker {config.novel_id} --key-index 1 --model {model}",
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
