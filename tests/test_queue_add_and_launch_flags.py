from __future__ import annotations

import pytest

from novel_tts.cli.main import _build_parser


def test_queue_launch_parser_accepts_add_queue_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "launch", "vo-cuc-thien-ton", "--add-queue"])
    assert args.command == "queue"
    assert args.queue_command == "launch"
    assert args.add_queue is True


def test_queue_add_requires_range_or_all() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "add", "vo-cuc-thien-ton"])


def test_queue_add_accepts_all() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "add", "vo-cuc-thien-ton", "--all"])
    assert args.command == "queue"
    assert args.queue_command == "add"
    assert args.all is True
    assert args.range is None


def test_queue_add_accepts_range() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "add", "vo-cuc-thien-ton", "--range", "1-10"])
    assert args.command == "queue"
    assert args.queue_command == "add"
    assert args.all is False
    assert args.range == "1-10"


def test_queue_add_rejects_all_and_range_together() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "add", "vo-cuc-thien-ton", "--all", "--range", "1-10"])

