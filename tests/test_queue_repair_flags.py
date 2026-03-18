from __future__ import annotations

import pytest

from novel_tts.cli.main import _build_parser


def test_queue_repair_requires_range_or_all() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "repair", "vo-cuc-thien-ton"])


def test_queue_repair_accepts_all() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "repair", "vo-cuc-thien-ton", "--all"])
    assert args.command == "queue"
    assert args.queue_command == "repair"
    assert args.all is True
    assert args.range is None


def test_queue_repair_accepts_range() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "repair", "vo-cuc-thien-ton", "--range", "1-10"])
    assert args.command == "queue"
    assert args.queue_command == "repair"
    assert args.all is False
    assert args.range == "1-10"


def test_queue_repair_rejects_all_and_range_together() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "repair", "vo-cuc-thien-ton", "--all", "--range", "1-10"])

