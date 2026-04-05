from __future__ import annotations

import importlib
from pathlib import Path

from novel_tts.cli.main import _build_parser, main
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
from novel_tts.crawl.service import SourceDiscoveryResult


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
    crawl = CrawlConfig(site_id="test", chapter_batch_size=10, chapter_regex=r"^第(\d+)章([^\n]*)")
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


def test_crawl_run_accepts_all_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["crawl", "run", "tro-lai-dai-hoc", "--all"])
    assert args.command == "crawl"
    assert args.crawl_command == "run"
    assert args.all is True


def test_crawl_run_all_resolves_latest_and_crawls_full_span(tmp_path: Path, monkeypatch) -> None:
    cli_main_module = importlib.import_module("novel_tts.cli.main")
    config = _make_config(tmp_path)
    monkeypatch.setattr(cli_main_module, "load_novel_config", lambda _novel_id: config)
    monkeypatch.setattr(cli_main_module, "configure_logging", lambda _path: None)
    monkeypatch.setattr(cli_main_module, "install_exception_logging", lambda _logger: None)

    monkeypatch.setattr(
        "novel_tts.crawl.service.discover_source_entries",
        lambda *args, **kwargs: SourceDiscoveryResult(
            source_config=config.source,
            entries={12: object()},
            latest_chapter=12,
        ),
    )

    captured: dict[str, object] = {}

    def _fake_crawl_range(cfg, start, end, directory_url=None, *, force=False, prune_failure_manifest=True, source_configs=None):
        captured["cfg"] = cfg
        captured["start"] = start
        captured["end"] = end
        captured["directory_url"] = directory_url
        captured["force"] = force
        captured["prune_failure_manifest"] = prune_failure_manifest
        captured["source_configs"] = source_configs
        return []

    monkeypatch.setattr("novel_tts.crawl.crawl_range", _fake_crawl_range)

    rc = main(["crawl", "run", "novel", "--all"])

    assert rc == 0
    assert captured["cfg"] is config
    assert captured["start"] == 1
    assert captured["end"] == 12
    assert captured["directory_url"] is None
    assert captured["force"] is False
    assert captured["prune_failure_manifest"] is True
    assert captured["source_configs"] is None
