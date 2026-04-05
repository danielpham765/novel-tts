from __future__ import annotations

from pathlib import Path

from novel_tts.tts.providers import _model_configs, _server_configs


def test_tts_provider_configs_accept_yaml_comments(tmp_path: Path) -> None:
    provider_dir = tmp_path / "configs" / "providers"
    provider_dir.mkdir(parents=True, exist_ok=True)

    (provider_dir / "tts_servers.yaml").write_text(
        "# staging endpoint disabled\n"
        "local: http://127.0.0.1:7860/\n"
        "onPremise: https://tts.aquafox.io/\n",
        encoding="utf-8",
    )
    (provider_dir / "tts_models.yaml").write_text(
        "# legacy short payload still loads as raw YAML data\n"
        "cpu:\n"
        "  - vi\n"
        "  - cpu\n"
        "  - true\n"
        "  - false\n",
        encoding="utf-8",
    )

    assert _server_configs(tmp_path)["local"] == "http://127.0.0.1:7860/"
    assert _server_configs(tmp_path)["onPremise"] == "https://tts.aquafox.io/"
    assert _model_configs(tmp_path)["cpu"] == ["vi", "cpu", True, False]
