from __future__ import annotations

import importlib
import json

cli_main = importlib.import_module("novel_tts.cli.main")


def test_youtube_playlist_title_only_outputs_id_and_title(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "novel_tts.upload.list_youtube_playlists",
        lambda: [
            {"id": "PL1", "title": "Playlist 1", "description": "ignored"},
            {"id": "PL2", "title": "Playlist 2", "privacy_status": "private"},
        ],
    )

    rc = cli_main.main(["youtube", "playlist", "--title-only"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == [{"id": "PL1", "title": "Playlist 1"}, {"id": "PL2", "title": "Playlist 2"}]


def test_youtube_video_title_only_outputs_id_and_title(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "novel_tts.upload.list_youtube_videos",
        lambda: [
            {"id": "vid1", "title": "Video 1", "description": "ignored"},
            {"id": "vid2", "title": "Video 2", "privacy_status": "private"},
        ],
    )

    rc = cli_main.main(["youtube", "video", "--title-only"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == [{"id": "vid1", "title": "Video 1"}, {"id": "vid2", "title": "Video 2"}]


def test_youtube_video_id_returns_single_video(monkeypatch, capsys) -> None:
    monkeypatch.setattr("novel_tts.upload.get_youtube_video", lambda _video_id: {"id": "vid1", "title": "Video 1"})

    rc = cli_main.main(["youtube", "video", "--id", "vid1"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == {"id": "vid1", "title": "Video 1"}


def test_youtube_playlist_update_requires_confirmation(monkeypatch, capsys) -> None:
    current = {
        "id": "PL1",
        "title": "Old title",
        "description": "Old description",
        "privacy_status": "private",
    }
    updates: list[dict[str, object]] = []
    monkeypatch.setattr("novel_tts.upload.get_youtube_playlist", lambda _playlist_id: current)
    monkeypatch.setattr(
        "novel_tts.upload.update_youtube_playlist",
        lambda playlist_id, **kwargs: updates.append({"playlist_id": playlist_id, **kwargs}) or {},
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    rc = cli_main.main(["youtube", "playlist", "update", "--id", "PL1", "--title", "New title"])

    assert rc == 0
    assert updates == []
    output = capsys.readouterr().out
    assert "Current playlist metadata:" in output
    assert "Update playlist metadata:" in output
    assert '"title": "New title"' in output
    assert '"description": "Old description"' not in output.split("Update playlist metadata:")[-1]


def test_youtube_playlist_update_executes_touch_when_confirmed(monkeypatch, capsys) -> None:
    current = {
        "id": "PL1",
        "title": "Old title",
        "description": "Old description",
        "privacy_status": "private",
    }
    updates: list[dict[str, object]] = []
    monkeypatch.setattr("novel_tts.upload.get_youtube_playlist", lambda _playlist_id: current)
    monkeypatch.setattr(
        "novel_tts.upload.update_youtube_playlist",
        lambda playlist_id, **kwargs: updates.append({"playlist_id": playlist_id, **kwargs}) or {"id": "PL1"},
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    rc = cli_main.main(["youtube", "playlist", "update", "--id", "PL1"])

    assert rc == 0
    assert updates == [
        {
            "playlist_id": "PL1",
            "title": None,
            "description": None,
            "privacy_status": None,
        }
    ]
    output = capsys.readouterr().out
    assert "Update playlist metadata:" in output
    assert "Nothing changes." in output
    assert '"id": "PL1"' in output


def test_youtube_video_update_requires_confirmation(monkeypatch, capsys) -> None:
    current = {
        "id": "vid1",
        "title": "Old title",
        "description": "Old description",
        "privacy_status": "private",
        "made_for_kids": False,
        "playlist_position": 3,
    }
    updates: list[dict[str, object]] = []
    monkeypatch.setattr("novel_tts.upload.get_youtube_video", lambda _video_id: current)
    monkeypatch.setattr(
        "novel_tts.upload.update_youtube_video",
        lambda video_id, **kwargs: updates.append({"video_id": video_id, **kwargs}) or {},
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")

    rc = cli_main.main(["youtube", "video", "update", "--id", "vid1", "--title", "New title"])

    assert rc == 0
    assert updates == []
    output = capsys.readouterr().out
    assert "Current video metadata:" in output
    assert "Update video metadata:" in output
    assert '"title": "New title"' in output
    assert '"description": "Old description"' not in output.split("Update video metadata:")[-1]


def test_youtube_video_update_executes_touch_when_confirmed(monkeypatch, capsys) -> None:
    current = {
        "id": "vid1",
        "title": "Old title",
        "description": "Old description",
        "privacy_status": "private",
        "made_for_kids": False,
        "playlist_position": 3,
    }
    updates: list[dict[str, object]] = []
    monkeypatch.setattr("novel_tts.upload.get_youtube_video", lambda _video_id: current)
    monkeypatch.setattr(
        "novel_tts.upload.update_youtube_video",
        lambda video_id, **kwargs: updates.append({"video_id": video_id, **kwargs}) or {"id": "vid1"},
    )
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    rc = cli_main.main(["youtube", "video", "update", "--id", "vid1"])

    assert rc == 0
    assert updates == [
        {
            "video_id": "vid1",
            "title": None,
            "description": None,
            "privacy_status": None,
            "made_for_kids": None,
            "playlist_position": None,
        }
    ]
    output = capsys.readouterr().out
    assert "Update video metadata:" in output
    assert "Nothing changes." in output
    assert '"id": "vid1"' in output


def test_upload_update_playlist_index_runs_bulk_update(monkeypatch, tmp_path: Path, capsys) -> None:
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
    cfg = NovelConfig(
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
        models=ModelsConfig(provider="gemini_http", enabled_models=["m1"], model_configs={"m1": QueueModelConfig()}),
        translation=TranslationConfig(chapter_regex=r"^$", base_rules="", glossary_file=""),
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

    monkeypatch.setattr(cli_main, "load_novel_config", lambda _novel_id: cfg)
    monkeypatch.setattr(
        "novel_tts.upload.update_uploaded_youtube_playlist_index_descriptions",
        lambda _config, **_kwargs: [{"video_id": "vid1", "status": "updated"}],
    )

    rc = cli_main.main(["upload", "novel", "--platform", "youtube", "--update-playlist-index"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == [{"video_id": "vid1", "status": "updated"}]
