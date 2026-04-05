from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

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
from novel_tts.translate import captions as captions_translate
from novel_tts.translate import novel as novel_translate
from novel_tts.translate.providers import ProxyGatewayConfig


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
        enabled_models=["gemma-3-27b-it"],
        model_configs={"gemma-3-27b-it": QueueModelConfig(chunk_max_len=4000, chunk_sleep_seconds=0.0)},
    )
    translation = TranslationConfig(
        chapter_regex=r"^第(\d+)章([^\n]*)",
        base_rules="",
        auto_update_glossary=True,
        glossary_file="configs/glossaries/captions-only.json",
        glossary={"阿伟": "A Vỹ"},
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
        captions=CaptionConfig(chunk_size=10),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        media=MediaConfig(
            visual=VisualConfig(background_video=""),
            video=VideoConfig(),
        ),
    )


def test_translate_captions_prompt_is_natural_and_updates_glossary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.storage.captions_dir.mkdir(parents=True, exist_ok=True)
    caption_path = config.storage.captions_dir / "caption_cn.srt"
    caption_path.write_text(
        "1\n00:00:01,000 --> 00:00:02,000\n阿伟来了\n",
        encoding="utf-8",
    )

    prompts: list[str] = []

    class _DummyProvider:
        def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
            del model, system_prompt
            prompts.append(prompt)
            if "Hãy trích xuất glossary thuật ngữ" in prompt:
                return json.dumps({"阿伟": "A Vỹ"}, ensure_ascii=False)
            return json.dumps({"translations": ["A Vỹ đến rồi!"]}, ensure_ascii=False)

    glossary_updates: list[tuple[str, str]] = []
    dummy_provider = _DummyProvider()
    monkeypatch.setattr(captions_translate, "get_translation_provider", lambda _name, *, config=None: dummy_provider)
    monkeypatch.setattr(novel_translate, "get_translation_provider", lambda _name, *, config=None: dummy_provider)
    monkeypatch.setattr(
        captions_translate,
        "update_glossary_from_chapter",
        lambda _cfg, source_text, translated_text: glossary_updates.append((source_text, translated_text)),
    )

    out_path = captions_translate.translate_captions(config)

    assert out_path == config.storage.captions_dir / "caption_vn.srt"
    assert prompts
    first_prompt = prompts[0]
    assert "Dịch tự nhiên theo phong cách phụ đề" in first_prompt
    assert "Tự động thêm dấu câu" in first_prompt
    assert "GLOSSARY:" in first_prompt
    assert "- 阿伟 = A Vỹ" in first_prompt

    assert glossary_updates == [("阿伟来了", "A Vỹ đến rồi!")]


def test_translate_captions_skips_glossary_update_in_queue_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _make_config(tmp_path)
    config.storage.captions_dir.mkdir(parents=True, exist_ok=True)
    caption_path = config.storage.captions_dir / "caption_cn.srt"
    caption_path.write_text(
        "1\n00:00:01,000 --> 00:00:02,000\n阿伟来了\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("NOVEL_TTS_QUOTA_MODE", "raise")
    monkeypatch.setenv("NOVEL_TTS_QUOTA_MAX_WAIT_SECONDS", "0")
    monkeypatch.setenv("NOVEL_TTS_CENTRAL_QUOTA", "1")
    monkeypatch.setenv("GEMINI_RATE_LIMIT_KEY_PREFIX", "novel_tts:novel:k1")

    class _DummyProvider:
        def generate(self, model: str, prompt: str, system_prompt: str = "") -> str:
            del model, prompt, system_prompt
            return json.dumps({"translations": ["A Vỹ đến rồi!"]}, ensure_ascii=False)

    dummy_provider = _DummyProvider()
    monkeypatch.setattr(captions_translate, "get_translation_provider", lambda _name, *, config=None: dummy_provider)
    monkeypatch.setattr(novel_translate, "get_translation_provider", lambda _name, *, config=None: dummy_provider)

    called: list[bool] = []

    def _fail_update(*args, **kwargs):
        del args, kwargs
        called.append(True)
        raise AssertionError("update_glossary_from_chapter should not run in queue mode")

    monkeypatch.setattr(captions_translate, "update_glossary_from_chapter", _fail_update)

    out_path = captions_translate.translate_captions(config)

    assert out_path == config.storage.captions_dir / "caption_vn.srt"
    assert not called
