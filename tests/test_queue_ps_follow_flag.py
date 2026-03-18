from __future__ import annotations

from novel_tts.cli.main import _build_parser


def test_queue_ps_parser_accepts_follow_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["queue", "ps", "vo-cuc-thien-ton", "-f"])
    assert args.command == "queue"
    assert args.queue_command == "ps"
    assert args.follow is True

