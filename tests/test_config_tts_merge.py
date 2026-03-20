import json

import yaml

from novel_tts.config import loader


def test_tts_defaults_from_app_yaml_and_novel_overrides(tmp_path, monkeypatch):
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
        "tts": {"provider": "gradio_vie_tts", "voice": "Tuyen", "tempo": 1.1},
    }
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")

    source_cfg = {"crawl": {"site_id": "test"}, "browser_debug": {}}
    (tmp_path / "configs" / "sources" / "s1.json").write_text(json.dumps(source_cfg), encoding="utf-8")

    base_novel = {
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
        "visual": {"background_video": "bg.mp4"},
        "video": {},
    }

    (tmp_path / "configs" / "novels" / "n1.json").write_text(json.dumps(base_novel), encoding="utf-8")
    cfg = loader.load_novel_config("n1")
    assert cfg.tts.provider == "gradio_vie_tts"
    assert cfg.tts.voice == "Tuyen"
    assert cfg.tts.tempo == 1.1

    override_novel = dict(base_novel)
    override_novel["novel_id"] = "n2"
    override_novel["slug"] = "n2"
    override_novel["tts"] = {"voice": "Ly"}
    (tmp_path / "configs" / "novels" / "n2.json").write_text(json.dumps(override_novel), encoding="utf-8")
    cfg2 = loader.load_novel_config("n2")
    assert cfg2.tts.provider == "gradio_vie_tts"
    assert cfg2.tts.voice == "Ly"
    assert cfg2.tts.tempo == 1.1


def test_load_novel_config_allows_captions_only_novel(tmp_path, monkeypatch):
    monkeypatch.setattr(loader, "_root_dir", lambda: tmp_path)

    (tmp_path / "configs" / "novels").mkdir(parents=True)

    app_cfg = {
        "models": {
            "provider": "gemini_http",
            "enabled_models": ["m1"],
            "model_configs": {"m1": {"chunk_max_len": 100, "worker_count": 1, "rpm_limit": 1, "tpm_limit": 1}},
        },
        "queue": {"redis": {"host": "127.0.0.1", "port": 6379, "database": 1, "prefix": "novel_tts"}},
        "tts": {"provider": "gradio_vie_tts", "voice": "Tuyen"},
    }
    (tmp_path / "configs" / "app.yaml").write_text(yaml.safe_dump(app_cfg, sort_keys=False), encoding="utf-8")

    novel_cfg = {
        "novel_id": "captions-only",
        "title": "Captions Only",
        "slug": "captions-only",
        "storage": {
            "input_dir": "input/captions-only",
            "output_dir": "output/captions-only",
            "image_dir": "image/captions-only",
            "logs_dir": ".logs",
            "tmp_dir": "tmp",
        },
        "translation": {
            "chapter": {"chapter_regex": "^$", "base_rules": "x"},
            "captions": {"input_file": "caption_cn.srt", "output_file": "caption_vn.srt"},
        },
        "models": {},
        "tts": {},
    }
    (tmp_path / "configs" / "novels" / "captions-only.json").write_text(json.dumps(novel_cfg), encoding="utf-8")

    cfg = loader.load_novel_config("captions-only")
    assert cfg.source_id == ""
    assert cfg.source.source_id == ""
    assert cfg.source.resolver_id == ""
    assert cfg.captions.input_file == "caption_cn.srt"
    assert cfg.captions.output_file == "caption_vn.srt"
