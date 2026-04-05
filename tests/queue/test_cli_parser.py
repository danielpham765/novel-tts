from __future__ import annotations

import pytest

from novel_tts.cli.main import _build_parser


def test_queue_launch_parser_accepts_add_queue_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "launch", "--add-queue", "--novel", "vo-cuc-thien-ton"])
    assert args.command == "queue"
    assert args.queue_command == "launch"
    assert args.add_queue is True
    assert args.novel == ["vo-cuc-thien-ton"]


def test_queue_add_requires_a_selection_flag() -> None:
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


def test_queue_drain_accepts_novel_id() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "drain", "vo-cuc-thien-ton"])
    assert args.command == "queue"
    assert args.queue_command == "drain"
    assert args.novel_id == "vo-cuc-thien-ton"


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


def test_queue_reset_key_requires_key_or_all() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "reset-key"])


def test_queue_reset_key_accepts_all() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "reset-key", "--all"])
    assert args.command == "queue"
    assert args.queue_command == "reset-key"
    assert args.all is True
    assert args.key == []


def test_queue_reset_key_rejects_all_and_key_together() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "reset-key", "--all", "--key", "k1"])


def test_queue_ps_parser_accepts_follow_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "ps", "vo-cuc-thien-ton", "-f"])
    assert args.command == "queue"
    assert args.queue_command == "ps"
    assert args.follow is True
