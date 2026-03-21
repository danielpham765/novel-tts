from __future__ import annotations

from pathlib import Path

from novel_tts.tts.providers import _model_configs, _server_configs


def test_tts_provider_configs_accept_json_comments(tmp_path: Path) -> None:
    provider_dir = tmp_path / "configs" / "providers"
    provider_dir.mkdir(parents=True, exist_ok=True)

    (provider_dir / "tts_servers.json").write_text(
        '{\n'
        '  "local": "http://127.0.0.1:7860/",\n'
        "  // staging endpoint disabled\n"
        '  "onPremise": "https://tts.aquafox.io/"\n'
        "}\n",
        encoding="utf-8",
    )
    (provider_dir / "tts_models.json").write_text(
        '{\n'
        "  /* legacy short payload */\n"
        '  "cpu": ["vi", "cpu", true, false]\n'
        "}\n",
        encoding="utf-8",
    )

    assert _server_configs(tmp_path)["local"] == "http://127.0.0.1:7860/"
    assert _server_configs(tmp_path)["onPremise"] == "https://tts.aquafox.io/"
    assert _model_configs(tmp_path)["cpu"] == ["vi", "cpu", True, False]
