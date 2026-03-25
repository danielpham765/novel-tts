from __future__ import annotations

import importlib
from pathlib import Path

cli_main = importlib.import_module("novel_tts.cli.main")
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
    UploadConfig,
    UploadTikTokConfig,
    UploadYouTubeConfig,
    VideoConfig,
    VisualConfig,
)


def _make_config(tmp_path: Path) -> NovelConfig:
    root = tmp_path
    storage = StorageConfig(
        root=root,
        input_dir=root / "input" / "novel",
        output_dir=root / "output" / "novel",
        image_dir=root / "image" / "novel",
        logs_dir=root / ".logs",
        tmp_dir=root / "tmp",
    )
    crawl = CrawlConfig(site_id="test")
    browser_debug = BrowserDebugConfig()
    source = SourceConfig(source_id="test", resolver_id="test", crawl=crawl, browser_debug=browser_debug)
    models = ModelsConfig(
        provider="gemini_http",
        enabled_models=["m1"],
        model_configs={"m1": QueueModelConfig(chunk_max_len=1000)},
    )
    translation = TranslationConfig(chapter_regex=r"^$", base_rules="", glossary_file="")
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
        captions=CaptionConfig(),
        queue=QueueConfig(),
        tts=TtsConfig(provider="local", voice="test"),
        visual=VisualConfig(background_video="bg.mp4"),
        video=VideoConfig(),
        upload=UploadConfig(
            default_platform="youtube",
            youtube=UploadYouTubeConfig(enabled=True),
            tiktok=UploadTikTokConfig(enabled=True, dry_run=True),
        ),
    )


def _patch_pipeline_deps(monkeypatch, uploads: list[tuple[int, int, str]]) -> None:
    from novel_tts import crawl, media, translate, tts, upload

    monkeypatch.setattr(crawl, "crawl_range", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(translate, "translate_novel", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(translate, "translate_captions", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(tts, "run_tts", lambda *_args, **_kwargs: Path("/tmp/dummy.mp3"))
    monkeypatch.setattr(media, "generate_visual", lambda *_args, **_kwargs: (Path("/tmp/v.mp4"), Path("/tmp/t.png")))
    monkeypatch.setattr(media, "create_video", lambda *_args, **_kwargs: Path("/tmp/out.mp4"))
    monkeypatch.setattr(
        upload,
        "run_uploads",
        lambda _cfg, ranges, *, platform, dry_run=False: [
            uploads.append((start, end, f"{platform}:{dry_run}")) or {} for start, end in ranges
        ],
    )


def _patch_pipeline_order_deps(monkeypatch, calls: list[str]) -> None:
    from novel_tts import crawl, media, translate, tts, upload

    monkeypatch.setattr(crawl, "crawl_range", lambda *_args, **_kwargs: calls.append("crawl") or [])
    monkeypatch.setattr(translate, "translate_novel", lambda *_args, **_kwargs: calls.append("translate") or [])
    monkeypatch.setattr(translate, "translate_captions", lambda *_args, **_kwargs: calls.append("captions") or None)
    monkeypatch.setattr(
        tts,
        "run_tts",
        lambda _cfg, start, end, range_key=None, **_kwargs: calls.append(f"tts:{start}-{end}:{range_key}") or Path("/tmp/dummy.mp3"),
    )
    monkeypatch.setattr(
        media,
        "generate_visual",
        lambda _cfg, start, end, **_kwargs: calls.append(f"visual:{start}-{end}") or (Path("/tmp/v.mp4"), Path("/tmp/t.png")),
    )
    monkeypatch.setattr(
        media,
        "create_video",
        lambda _cfg, start, end, **_kwargs: calls.append(f"video:{start}-{end}") or Path("/tmp/out.mp4"),
    )
    monkeypatch.setattr(
        upload,
        "run_uploads",
        lambda _cfg, ranges, *, platform, dry_run=False: [
            calls.append(f"upload:{start}-{end}:{platform}:{dry_run}") or {} for start, end in ranges
        ],
    )


def test_pipeline_runs_upload_with_default_platform(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    uploads: list[tuple[int, int, str]] = []
    monkeypatch.setattr(cli_main, "load_novel_config", lambda _novel_id: cfg)
    _patch_pipeline_deps(monkeypatch, uploads)

    rc = cli_main.main(
        [
            "pipeline",
            "run",
            "novel",
            "--range",
            "1-10",
            "--skip-crawl",
            "--skip-translate",
            "--skip-captions",
            "--skip-tts",
            "--skip-visual",
        ]
    )

    assert rc == 0
    assert uploads == [(1, 10, "youtube:False")]


def test_pipeline_skip_upload_flag(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    uploads: list[tuple[int, int, str]] = []
    monkeypatch.setattr(cli_main, "load_novel_config", lambda _novel_id: cfg)
    _patch_pipeline_deps(monkeypatch, uploads)

    rc = cli_main.main(
        [
            "pipeline",
            "run",
            "novel",
            "--range",
            "1-10",
            "--skip-crawl",
            "--skip-translate",
            "--skip-captions",
            "--skip-tts",
            "--skip-visual",
            "--skip-upload",
        ]
    )

    assert rc == 0
    assert uploads == []


def test_pipeline_upload_platform_override(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    uploads: list[tuple[int, int, str]] = []
    monkeypatch.setattr(cli_main, "load_novel_config", lambda _novel_id: cfg)
    _patch_pipeline_deps(monkeypatch, uploads)

    rc = cli_main.main(
        [
            "pipeline",
            "run",
            "novel",
            "--range",
            "1-10",
            "--skip-crawl",
            "--skip-translate",
            "--skip-captions",
            "--skip-tts",
            "--skip-visual",
            "--upload-platform",
            "tiktok",
        ]
    )

    assert rc == 0
    assert uploads == [(1, 10, "tiktok:False")]


def test_pipeline_per_video_mode_runs_media_steps_in_video_order(tmp_path: Path, monkeypatch) -> None:
    cfg = _make_config(tmp_path)
    calls: list[str] = []
    monkeypatch.setattr(cli_main, "load_novel_config", lambda _novel_id: cfg)
    monkeypatch.setattr(
        cli_main,
        "get_translated_ranges",
        lambda _cfg, _start, _end: [(1, 10, "chuong_1-10"), (11, 20, "chuong_11-20")],
    )
    _patch_pipeline_order_deps(monkeypatch, calls)

    rc = cli_main.main(["pipeline", "run", "novel", "--range", "1-20", "--mode", "per-video"])

    assert rc == 0
    assert calls == [
        "crawl",
        "translate",
        "captions",
        "tts:1-10:chuong_1-10",
        "visual:1-10",
        "video:1-10",
        "upload:1-10:youtube:False",
        "tts:11-20:chuong_11-20",
        "visual:11-20",
        "video:11-20",
        "upload:11-20:youtube:False",
    ]
