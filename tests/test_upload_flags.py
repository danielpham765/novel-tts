from __future__ import annotations

import pytest

from novel_tts.cli.main import _build_parser


def test_upload_parser_accepts_platform_and_range() -> None:
    parser = _build_parser()
    args = parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "youtube", "--range", "1-10"])
    assert args.command == "upload"
    assert args.novel_id == "vo-cuc-thien-ton"
    assert args.platform == "youtube"
    assert args.range == "1-10"
    assert args.dry_run is False


def test_upload_parser_accepts_dry_run() -> None:
    parser = _build_parser()
    args = parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "tiktok", "--range", "1-10", "--dry-run"])
    assert args.command == "upload"
    assert args.platform == "tiktok"
    assert args.dry_run is True


def test_upload_parser_rejects_missing_platform() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["upload", "vo-cuc-thien-ton", "--range", "1-10"])


def test_upload_parser_rejects_missing_range() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "youtube"])


def test_pipeline_parser_accepts_upload_flags() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        ["pipeline", "run", "vo-cuc-thien-ton", "--range", "1-10", "--skip-upload", "--upload-platform", "tiktok"]
    )
    assert args.command == "pipeline"
    assert args.pipeline_command == "run"
    assert args.skip_upload is True
    assert args.upload_platform == "tiktok"
