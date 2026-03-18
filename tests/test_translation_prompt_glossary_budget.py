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
from novel_tts.translate import novel as novel_translate


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
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl, browser_debug=browser_debug)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["dummy"],
        model_configs={"dummy": QueueModelConfig(chunk_max_len=80, chunk_sleep_seconds=0.0)},
    )
    translation = TranslationConfig(
        chapter_regex=r"^第(\d+)章([^\n]*)",
        base_rules="",
        line_token="QZXBRQ",
        auto_update_glossary=False,
        glossary_file="",
        glossary={},
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


def test_translate_unit_scopes_glossary_to_chunk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)

    # Create a large glossary so placeholder mapping is large across the unit.
    glossary: dict[str, str] = {}
    for i in range(200):
        key = f"名{i:04d}称"
        glossary[key] = f"Ten{i:04d}"
    config.translation.glossary = glossary

    # Build raw text that includes *all* glossary keys, one per line, so only a small subset fits in the first chunk.
    raw_text = "\n".join(glossary.keys()) + "\n"

    prompts: list[str] = []

    class _DummyProvider:
        def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
            prompts.append(prompt)
            return "OK"

    monkeypatch.setattr(novel_translate, "get_translation_provider", lambda _name, *, config=None: _DummyProvider())

    novel_translate.translate_unit(config, "unit", raw_text)

    assert prompts
    first = prompts[0]
    # A token from far later in the mapping should not be present in the first request if glossary is chunk-scoped.
    assert "ZXQ000QXZ" in first
    assert "ZXQ150QXZ" not in first
    # Keep per-request glossary compact (do not include the entire unit's mapping).
    assert first.count("- ZXQ") < 40
