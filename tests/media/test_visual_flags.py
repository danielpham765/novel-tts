from __future__ import annotations

import pytest

from novel_tts.cli.main import _build_parser


def test_visual_accepts_range() -> None:
    parser = _build_parser()
    args = parser.parse_args(["visual", "vo-cuc-thien-ton", "--range", "1-10"])
    assert args.range == "1-10"
    assert args.chapter is None


def test_visual_accepts_chapter() -> None:
    parser = _build_parser()
    args = parser.parse_args(["visual", "vo-cuc-thien-ton", "--chapter", "1"])
    assert args.chapter == 1
    assert args.range is None


def test_visual_requires_exactly_one_of_range_or_chapter() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["visual", "vo-cuc-thien-ton"])
    with pytest.raises(SystemExit):
        parser.parse_args(["visual", "vo-cuc-thien-ton", "--range", "1-10", "--chapter", "1"])
