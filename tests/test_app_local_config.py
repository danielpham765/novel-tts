from __future__ import annotations

import json

import yaml

from novel_tts.config import loader


def test_load_app_config_merges_app_local_before_novel(tmp_path, monkeypatch) -> None:
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
        "tts": {"provider": "gradio_vie_tts", "voice": "Ly", "server_name": "app-default"},
    }
    app_local_cfg = {
        "tts": {"server_name": "device-local", "model_name": "gpu"},
        "media": {"video": {"use_gpu": False}},
    }
    source_cfg = {"crawl": {"site_id": "test", "browser_debug": {}}}
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
        "tts": {"server_name": "novel-override"},
        "media": {
            "visual": {"background_video": "bg.mp4"},
            "video": {},
        },
    }

    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")
    (tmp_path / "configs" / "app.local.yaml").write_text(
        yaml.safe_dump(app_local_cfg, sort_keys=False),
        encoding="utf-8",
    )
    (tmp_path / "configs" / "sources" / "s1.json").write_text(json.dumps(source_cfg), encoding="utf-8")
    (tmp_path / "configs" / "novels" / "n1.yaml").write_text(
        yaml.safe_dump(novel_cfg, sort_keys=False),
        encoding="utf-8",
    )

    cfg = loader.load_novel_config("n1")

    assert cfg.tts.server_name == "novel-override"
    assert cfg.tts.model_name == "gpu"
    assert cfg.media.video.use_gpu is False
