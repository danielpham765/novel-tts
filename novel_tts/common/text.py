from __future__ import annotations

import re


def normalize_whitespace(value: str) -> str:
    return (
        value.replace("\r", "")
        .replace("\u00a0", " ")
        .replace("\t", " ")
        .replace(" ", " ")
        .strip()
    )


def normalize_ellipsis(value: str) -> str:
    value = value.replace("…", "...")
    value = re.sub(r"\.\.\.(?:\s*\.\.\.)+", "...", value)
    value = re.sub(r"\.{4,}", "...", value)
    return value


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def parse_range(raw: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d+)-(\d+)", raw.strip())
    if not match:
        raise ValueError(f"Invalid range: {raw}")
    start, end = int(match.group(1)), int(match.group(2))
    if start > end:
        raise ValueError(f"Invalid range: {raw}")
    return start, end
