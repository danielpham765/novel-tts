from __future__ import annotations

from pathlib import Path

import pytest

from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    ModelsConfig,
    NovelConfig,
    QueueConfig,
    QueueModelConfig,
    SourceConfig,
    StorageConfig,
    TtsConfig,
    TranslationConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.tts import service as tts_service


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    input_dir = root / "input"
    output_dir = root / "output"
    storage = StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=root / "image",
        logs_dir=root / "logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl, browser_debug=browser_debug)
    models = ModelsConfig(
        provider="dummy",
        enabled_models=["dummy"],
        model_configs={"dummy": QueueModelConfig(chunk_max_len=4000, chunk_sleep_seconds=0.0)},
    )
    translation = TranslationConfig(
        chapter_regex=r"^第(\d+)章([^\n]*)",
        base_rules="",
        auto_update_glossary=True,
        glossary_file="",
    )
    captions = CaptionConfig()
    queue = QueueConfig()
    return NovelConfig(
        novel_id="novel",
        title="Novel",
        slug="novel",
        source_language="zh",
        target_language="vi",
        source_id="test",
        source=source,
        storage=storage,
        crawl=crawl,
        browser_debug=browser_debug,
        models=models,
        translation=translation,
        captions=captions,
        queue=queue,
        tts=TtsConfig(provider="local", voice="test"),
        visual=VisualConfig(background_video=""),
        video=VideoConfig(),
    )


class _DummyProvider:
    def __init__(self, tmp_dir: Path) -> None:
        self.tmp_dir = tmp_dir
        self.calls = 0

    def connect(self):
        return object()

    def load_model(self, _client) -> None:
        return None

    def synthesize(self, _client, _text: str, progress_callback=None):
        if progress_callback is not None:
            progress_callback("ITERATING")
        self.calls += 1
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        source_audio = self.tmp_dir / f"source_{self.calls}.wav"
        source_audio.write_bytes(f"wav:{self.calls}".encode("utf-8"))
        return {"path": str(source_audio)}


def test_tts_cache_invalidates_on_text_change_and_force(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)

    translated_dir = config.storage.translated_dir
    translated_dir.mkdir(parents=True, exist_ok=True)
    translated_path = translated_dir / "chuong_1-1.txt"

    provider = _DummyProvider(config.storage.tmp_dir)
    monkeypatch.setattr(tts_service, "get_tts_provider", lambda _config: provider)
    monkeypatch.setattr(tts_service, "ffprobe_duration", lambda _path: 10.0)

    def _fake_run_ffmpeg(args: list[str]) -> None:
        # Output path is always the last argument.
        Path(args[-1]).write_bytes(b"mp3")

    monkeypatch.setattr(tts_service, "run_ffmpeg", _fake_run_ffmpeg)

    translated_path.write_text("Chương 1: A\nXin chào\n", encoding="utf-8")
    tts_service.run_tts(config, 1, 1)
    assert provider.calls == 1
    # Writes per-chapter assets into .parts and keeps the range folder clean.
    audio_dir = config.storage.audio_dir / "chuong_1-1"
    assert (audio_dir / ".parts" / "chapter_1.wav").exists()
    assert (audio_dir / ".parts" / "file-list.txt").exists()
    assert not (audio_dir / "file-list.txt").exists()

    # Cached (hash matches).
    tts_service.run_tts(config, 1, 1)
    assert provider.calls == 1

    # Change translated text => cache invalidated, re-synthesize.
    translated_path.write_text("Chương 1: A\nXin chào!!!\n", encoding="utf-8")
    tts_service.run_tts(config, 1, 1)
    assert provider.calls == 2

    # Force => always re-synthesize.
    tts_service.run_tts(config, 1, 1, force=True)
    assert provider.calls == 3
