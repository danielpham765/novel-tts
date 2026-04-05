from __future__ import annotations

from novel_tts.cli.main import _build_parser


def test_quota_supervisor_parser_accepts_daemon_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["quota-supervisor", "-d"])
    assert args.command == "quota-supervisor"
    assert args.daemon is True


def test_quota_supervisor_parser_accepts_stop_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["quota-supervisor", "--stop"])
    assert args.command == "quota-supervisor"
    assert args.stop is True


def test_quota_supervisor_parser_accepts_restart_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["quota-supervisor", "--restart"])
    assert args.command == "quota-supervisor"
    assert args.restart is True
