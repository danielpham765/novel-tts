from __future__ import annotations

import json

import pytest
import yaml

from novel_tts.config import loader


def test_load_novel_config_upload_merge(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    (tmp_path / "configs" / "novels").mkdir(parents=True)
    (tmp_path / "configs" / "sources").mkdir(parents=True)

    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly"},
        "upload": {
            "default_platform": "youtube",
            "youtube": {
                "enabled": True,
                "project": 2,
                "privacy_status": "public",
                "credentials_path": [".secrets/youtube/a_client.json", ".secrets/youtube/b_client.json"],
                "token_path": [".secrets/youtube/a_token.json", ".secrets/youtube/b_token.json"],
            },
        },
    }
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")
    source_cfg = {"crawl": {"site_id": "test", "browser_debug": {}}}
    (tmp_path / "configs" / "sources" / "s1.json").write_text(json.dumps(source_cfg), encoding="utf-8")
    novel_cfg = {
        "novel_id": "n1",
        "title": "N1",
        "slug": "n1",
        "storage": {
            "input_dir": "input/n1",
            "output_dir": "output/n1",
            "image_dir": "image/n1",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "crawl": {"sources": [{"source_id": "s1"}]},
        "translation": {"chapter": {"chapter_regex": "^$", "base_rules": "x"}},
        "models": {},
        "tts": {},
        "media": {"visual": {"background_video": "bg.mp4"}, "video": {}},
        "upload": {"default_platform": "tiktok", "tiktok": {"enabled": True, "dry_run": True}},
    }
    (tmp_path / "configs" / "novels" / "n1.yaml").write_text(
        yaml.safe_dump(novel_cfg, sort_keys=False),
        encoding="utf-8",
    )

    cfg = loader.load_novel_config("n1")
    assert cfg.upload.default_platform == "tiktok"
    assert cfg.upload.youtube.enabled is True
    assert cfg.upload.youtube.project == "2"
    assert cfg.upload.youtube.credentials_path == [
        ".secrets/youtube/a_client.json",
        ".secrets/youtube/b_client.json",
    ]
    assert cfg.upload.youtube.token_path == [
        ".secrets/youtube/a_token.json",
        ".secrets/youtube/b_token.json",
    ]
    assert cfg.upload.tiktok.enabled is True
    assert cfg.upload.tiktok.dry_run is True


def test_load_novel_config_upload_rejects_mismatched_youtube_account_lists(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    (tmp_path / "configs" / "novels").mkdir(parents=True)
    (tmp_path / "configs" / "sources").mkdir(parents=True)

    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly"},
        "upload": {
            "default_platform": "youtube",
            "youtube": {
                "enabled": True,
                "credentials_path": [".secrets/youtube/a_client.json", ".secrets/youtube/b_client.json"],
                "token_path": [".secrets/youtube/a_token.json"],
            },
        },
    }
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")
    source_cfg = {"crawl": {"site_id": "test", "browser_debug": {}}}
    (tmp_path / "configs" / "sources" / "s1.json").write_text(json.dumps(source_cfg), encoding="utf-8")
    novel_cfg = {
        "novel_id": "n1",
        "title": "N1",
        "slug": "n1",
        "storage": {
            "input_dir": "input/n1",
            "output_dir": "output/n1",
            "image_dir": "image/n1",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "crawl": {"sources": [{"source_id": "s1"}]},
        "translation": {"chapter": {"chapter_regex": "^$", "base_rules": "x"}},
        "models": {},
        "tts": {},
        "media": {"visual": {"background_video": "bg.mp4"}, "video": {}},
        "upload": {"default_platform": "youtube"},
    }
    (tmp_path / "configs" / "novels" / "n1.yaml").write_text(
        yaml.safe_dump(novel_cfg, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must have the same number of entries"):
        loader.load_novel_config("n1")


def test_load_novel_config_upload_rejects_invalid_youtube_project_selector(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)
    (tmp_path / "configs" / "novels").mkdir(parents=True)
    (tmp_path / "configs" / "sources").mkdir(parents=True)

    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly"},
        "upload": {
            "default_platform": "youtube",
            "youtube": {
                "enabled": True,
                "project": "abc",
                "credentials_path": [".secrets/youtube/a_client.json"],
                "token_path": [".secrets/youtube/a_token.json"],
            },
        },
    }
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")
    source_cfg = {"crawl": {"site_id": "test", "browser_debug": {}}}
    (tmp_path / "configs" / "sources" / "s1.json").write_text(json.dumps(source_cfg), encoding="utf-8")
    novel_cfg = {
        "novel_id": "n1",
        "title": "N1",
        "slug": "n1",
        "storage": {
            "input_dir": "input/n1",
            "output_dir": "output/n1",
            "image_dir": "image/n1",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "crawl": {"sources": [{"source_id": "s1"}]},
        "translation": {"chapter": {"chapter_regex": "^$", "base_rules": "x"}},
        "models": {},
        "tts": {},
        "media": {"visual": {"background_video": "bg.mp4"}, "video": {}},
        "upload": {"default_platform": "youtube"},
    }
    (tmp_path / "configs" / "novels" / "n1.yaml").write_text(
        yaml.safe_dump(novel_cfg, sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match='upload.youtube.project'):
        loader.load_novel_config("n1")
