from __future__ import annotations

from types import SimpleNamespace

import pytest

from novel_tts.config.models import TtsConfig
from novel_tts.tts.providers import GradioTtsProvider, TtsModelConfig, TtsModelLoadError, TtsModelNotReadyError


class _FakeJob:
    def __init__(self, result):
        self._result = result
        self.communicator = SimpleNamespace(updates=None)

    def done(self) -> bool:
        return True

    def result(self):
        return self._result


class _FakeClient:
    def __init__(self, results):
        self.results = list(results)
        self.submit_calls = 0
        self.submit_args = []
        self.predict_calls = []

    def submit(self, *_args, **_kwargs):
        self.submit_args.append((_args, _kwargs))
        result = self.results[self.submit_calls]
        self.submit_calls += 1
        return _FakeJob(result)

    def predict(self, *args, **kwargs):
        self.predict_calls.append((args, kwargs))
        return self.results[0]


def _make_provider() -> GradioTtsProvider:
    provider = object.__new__(GradioTtsProvider)
    provider.config = SimpleNamespace(
        tts=TtsConfig(
            provider="gradio_vie_tts",
            voice="Doan",
            server_name="ttsCloud",
            model_name="gpu",
        )
    )
    provider.server_url = "http://127.0.0.1:17860"
    provider.model_config = TtsModelConfig(
        backbone="VieNeu-TTS (GPU)",
        codec="NeuCodec (Standard)",
        device="CUDA",
        use_lmdeploy=False,
        custom_model_id="",
        base_model="",
        hf_token="",
    )
    return provider


def test_synthesize_reloads_model_when_server_reports_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_provider()
    client = _FakeClient([(None, "⚠️  Vui lòng tải model trước!"), ("/tmp/audio.wav", None)])
    load_reasons: list[str] = []

    monkeypatch.setattr("novel_tts.tts.providers.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        provider,
        "_load_model_with_retry",
        lambda _client, reason=None: load_reasons.append(str(reason)),
    )

    result = provider.synthesize(client, "Xin chao")

    assert result == "/tmp/audio.wav"
    assert client.submit_calls == 2
    assert load_reasons == ["server-reported-model-not-ready"]
    first_submit_args, first_submit_kwargs = client.submit_args[0]
    assert first_submit_kwargs["api_name"] == "/synthesize_speech"
    assert first_submit_args[4] == "Standard (Một lần)"
    assert first_submit_args[5] is True
    assert first_submit_args[6] == 128
    assert first_submit_args[7] == 1.0
    assert first_submit_args[8] == 512


def test_synthesize_normalizes_legacy_generation_mode_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_provider()
    provider.config.tts.generation_mode = "preset_mode"
    client = _FakeClient([("/tmp/audio.wav", None)])

    monkeypatch.setattr("novel_tts.tts.providers.time.sleep", lambda _seconds: None)

    result = provider.synthesize(client, "Xin chao")

    assert result == "/tmp/audio.wav"
    first_submit_args, _first_submit_kwargs = client.submit_args[0]
    assert first_submit_args[4] == "Standard (Một lần)"
    assert first_submit_args[5] is True


def test_synthesize_raises_after_repeated_not_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _make_provider()
    client = _FakeClient([(None, "⚠️  Vui lòng tải model trước!")] * 3)
    load_reasons: list[str] = []

    monkeypatch.setattr("novel_tts.tts.providers.time.sleep", lambda _seconds: None)
    monkeypatch.setattr(
        provider,
        "_load_model_with_retry",
        lambda _client, reason=None: load_reasons.append(str(reason)),
    )

    with pytest.raises(TtsModelNotReadyError, match="still reported model not ready"):
        provider.synthesize(client, "Xin chao")

    assert client.submit_calls == 3
    assert load_reasons == [
        "server-reported-model-not-ready",
        "server-reported-model-not-ready",
    ]


def test_load_model_raises_when_server_reports_error() -> None:
    provider = _make_provider()
    client = _FakeClient([("❌ Lỗi khi tải model: CUDA unavailable", None, None)])

    with pytest.raises(TtsModelLoadError, match="CUDA unavailable"):
        provider.load_model(client)

    predict_args, predict_kwargs = client.predict_calls[0]
    assert predict_kwargs["api_name"] == "/load_model"
    assert predict_args == (
        "VieNeu-TTS (GPU)",
        "NeuCodec (Standard)",
        "CUDA",
        False,
        "",
        "",
        "",
    )
