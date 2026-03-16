from __future__ import annotations

from novel_tts.queue import translation_queue as tq


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_probe_gemini_429_true(monkeypatch) -> None:
    monkeypatch.setattr(tq.requests, "post", lambda *args, **kwargs: _Resp(429))
    assert tq._probe_gemini_429(api_key="k", model="m") is True


def test_probe_gemini_429_false(monkeypatch) -> None:
    monkeypatch.setattr(tq.requests, "post", lambda *args, **kwargs: _Resp(200))
    assert tq._probe_gemini_429(api_key="k", model="m") is False


def test_probe_gemini_429_unknown_on_exception(monkeypatch) -> None:
    def _boom(*args, **kwargs):
        raise RuntimeError("net")

    monkeypatch.setattr(tq.requests, "post", _boom)
    assert tq._probe_gemini_429(api_key="k", model="m") is None

