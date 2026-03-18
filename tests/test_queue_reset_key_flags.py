from __future__ import annotations

import pytest

from novel_tts.cli.main import _build_parser


def test_queue_reset_key_requires_key_or_all() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "reset-key", "vo-cuc-thien-ton"])


def test_queue_reset_key_accepts_all() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "reset-key", "vo-cuc-thien-ton", "--all"])
    assert args.command == "queue"
    assert args.queue_command == "reset-key"
    assert args.all is True
    assert args.key == []


def test_queue_reset_key_rejects_all_and_key_together() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["queue", "reset-key", "vo-cuc-thien-ton", "--all", "--key", "k1"])

