from __future__ import annotations

import os

from novel_tts.config.models import NovelConfig


def _clean_model_name(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"none", "null"}:
        return ""
    return text


def resolve_translation_model(config: NovelConfig) -> str:
    model = _clean_model_name(os.environ.get("NOVEL_TTS_TRANSLATION_MODEL") or os.environ.get("GEMINI_MODEL") or "")
    if model:
        return model
    enabled = getattr(config, "models", None) and config.models.enabled_models
    if enabled:
        return _clean_model_name(enabled[0])
    enabled_queue = getattr(config, "queue", None) and config.queue.enabled_models
    if enabled_queue:
        return _clean_model_name(enabled_queue[0])
    raise KeyError("Missing models.enabled_models[0]")
