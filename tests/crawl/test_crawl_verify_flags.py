from __future__ import annotations

from novel_tts.cli.main import _build_parser


def test_crawl_verify_accepts_sync_repair_config_flag() -> None:
    parser = _build_parser()
    args = parser.parse_args(["crawl", "verify", "tro-lai-dai-hoc", "--sync-repair-config"])
    assert args.command == "crawl"
    assert args.crawl_command == "verify"
    assert args.sync_repair_config is True
