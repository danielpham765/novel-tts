from __future__ import annotations

from pathlib import Path

from novel_tts.config.models import (
    CaptionConfig,
    CrawlConfig,
    MediaConfig,
    ModelsConfig,
    NovelConfig,
    ProxyGatewayConfig,
    QueueConfig,
    QueueModelConfig,
    SourceConfig,
    StorageConfig,
    TtsConfig,
    TranslationConfig,
    VideoConfig,
    VisualConfig,
)
from novel_tts.crawl.service import _fetch_chapter
from novel_tts.crawl.types import ChapterEntry, FetchResult, ParsedChapter


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    input_dir = root / "input"
    output_dir = root / "output"
    storage = StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=root / "image",
        logs_dir=root / ".logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(
        site_id="test",
        chapter_batch_size=10,
        chapter_regex=r"^第(\d+)章([^\n]*)",
        max_fetch_retries=1,
        delay_between_chapters_seconds=0.0,
    )
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["dummy"],
        model_configs={"dummy": QueueModelConfig(chunk_max_len=4000, chunk_sleep_seconds=0.0)},
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
        proxy_gateway=ProxyGatewayConfig(),
    )


class _ShortContentResolver:
    def parse_chapter(self, html: str, expected_chapter_number: int, fallback_title: str = "") -> ParsedChapter:
        del html
        return ParsedChapter(
            chapter_number=expected_chapter_number,
            title=fallback_title or f"第{expected_chapter_number}章 原标题",
            content="太短了",
        )

    def find_next_part_url(self, html: str, current_url: str, chapter_number: int) -> str | None:
        del html, current_url, chapter_number
        return None


class _SingleFetchStrategy:
    def fetch(self, url: str, timeout_seconds: int) -> FetchResult:
        del timeout_seconds
        return FetchResult(
            url=url,
            final_url=url,
            html="<html></html>",
            title="",
            strategy_name="http",
        )


def test_fetch_chapter_uses_placeholder_when_content_too_short(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    entry = ChapterEntry(
        chapter_number=1205,
        title="第1205章 原标题",
        url="https://example.com/1205",
    )

    block, parsed_number, stats = _fetch_chapter(
        entry,
        config,
        _ShortContentResolver(),
        _SingleFetchStrategy(),
    )

    assert parsed_number == 1205
    assert block == "第1205章 略过\n\n本章内容与主线剧情无关。"
    assert stats["title"] == "第1205章 略过"
    assert stats["parts"] == 1
    assert stats["chars"] == len("本章内容与主线剧情无关。")
    assert stats["strategy"] == "http"
