from __future__ import annotations

from pathlib import Path

import pytest

from novel_tts.config.loader import load_novel_config
from novel_tts.config.models import StorageConfig
from novel_tts.translate import novel as novel_mod
from novel_tts.translate.novel import PLACEHOLDER_TOKEN_RE, translate_unit


class _DummyProvider:
    def __init__(self) -> None:
        self.calls: int = 0

    def generate(self, model: str, prompt: str) -> str:
        self.calls += 1
        # 1) Initial translation returns Han residue but no placeholders after restoration.
        if self.calls == 1:
            return "ZXQ000QXZ 麻煩"
        # 2) A repair stage (e.g. per-line Han patch) re-emits a placeholder token.
        return "ZXQ000QXZ gặp rắc rối."


def _tmp_storage(tmp_path: Path) -> StorageConfig:
    root = tmp_path
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    image_dir = tmp_path / "image"
    logs_dir = tmp_path / ".logs"
    tmp_dir = tmp_path / "tmp"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    image_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    return StorageConfig(
        root=root,
        input_dir=input_dir,
        output_dir=output_dir,
        image_dir=image_dir,
        logs_dir=logs_dir,
        tmp_dir=tmp_dir,
    )


def test_translate_unit_final_placeholder_restore(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = load_novel_config("vo-cuc-thien-ton")
    cfg.storage = _tmp_storage(tmp_path)
    monkeypatch.setenv("CHUNK_SLEEP_SECONDS", "0")

    # Avoid filesystem refresh, keep glossary tiny and deterministic.
    cfg.translation.glossary_file = ""
    cfg.translation.glossary = {"陳天極": "Trần Thiên Kiệt"}
    cfg.translation.han_fallback_replacements = {}
    cfg.translation.post_replacements = {}
    cfg.translation.base_rules = (
        "Rules.\n"
        "- Phải giữ nguyên các token placeholder như ZXQ000QXZ và token xuống dòng QZXBRQ, không được sửa đổi.\n"
    )

    dummy = _DummyProvider()
    monkeypatch.setattr(novel_mod, "get_translation_provider", lambda _provider, *, config=None: dummy)

    out = translate_unit(cfg, unit_key="unit", raw_text="陳天極")

    assert "Trần Thiên Kiệt" in out
    assert not PLACEHOLDER_TOKEN_RE.search(out)
