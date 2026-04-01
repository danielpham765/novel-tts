from __future__ import annotations

from functools import lru_cache
from pathlib import Path


def _prompts_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "configs" / "llm-prompts"


@lru_cache(maxsize=None)
def _load_raw(name: str) -> str:
    return (_prompts_dir() / name).read_text(encoding="utf-8")


def render_prompt(name: str, **kwargs: str) -> str:
    """Load a prompt template file and substitute {placeholder} markers.

    Uses sequential str.replace instead of str.format so that base_rules or
    glossary values containing literal braces do not cause KeyError.
    """
    text = _load_raw(name)
    for key, value in kwargs.items():
        text = text.replace("{" + key + "}", value)
    return text
