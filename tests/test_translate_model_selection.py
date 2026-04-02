from __future__ import annotations

from pathlib import Path

from novel_tts.config.models import (
    BrowserDebugConfig,
    CaptionConfig,
    CrawlConfig,
    MediaConfig,
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
from novel_tts.translate.model import resolve_translation_model


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    storage = StorageConfig(
        root=root,
        input_dir=root / "input",
        output_dir=root / "output",
        image_dir=root / "image",
        logs_dir=root / "logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["first", "second"],
        model_configs={"first": QueueModelConfig(chunk_max_len=4000), "second": QueueModelConfig(chunk_max_len=4000)},
    )
    translation = TranslationConfig(
        chapter_regex=r"^第(\d+)章([^\n]*)",
        base_rules="",
        auto_update_glossary=True,
        glossary_file="",
    )
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
        models=models,
        translation=translation,
        captions=CaptionConfig(),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        media=MediaConfig(
            visual=VisualConfig(background_video=""),
            video=VideoConfig(),
        ),
    )


def test_resolve_translation_model_uses_new_env_and_ignores_alias(tmp_path, monkeypatch):
    config = _make_config(tmp_path)

    monkeypatch.delenv("NOVEL_TTS_TRANSLATION_MODEL", raising=False)
    monkeypatch.setenv("NOVEL_TTS_TRANSLATE_MODEL", "legacy-alias-model")
    monkeypatch.setenv("GEMINI_MODEL", "")

    assert resolve_translation_model(config) == "first"

    monkeypatch.setenv("NOVEL_TTS_TRANSLATION_MODEL", "new-primary-model")

    assert resolve_translation_model(config) == "new-primary-model"
