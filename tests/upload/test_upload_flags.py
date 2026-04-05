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
    assert args.update_playlist_index is False


def test_upload_parser_accepts_dry_run() -> None:
    parser = _build_parser()
    args = parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "tiktok", "--range", "1-10", "--dry-run"])
    assert args.command == "upload"
    assert args.platform == "tiktok"
    assert args.dry_run is True


def test_upload_parser_accepts_force() -> None:
    parser = _build_parser()
    args = parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "youtube", "--range", "1-10", "--force"])
    assert args.command == "upload"
    assert args.platform == "youtube"
    assert args.force is True


def test_upload_parser_accepts_update_playlist_index_without_range() -> None:
    parser = _build_parser()
    args = parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "youtube", "--update-playlist-index"])

    assert args.command == "upload"
    assert args.platform == "youtube"
    assert args.range is None
    assert args.update_playlist_index is True


def test_upload_parser_rejects_missing_platform() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["upload", "vo-cuc-thien-ton", "--range", "1-10"])


def test_upload_parser_allows_missing_range_for_runtime_validation() -> None:
    parser = _build_parser()
    args = parser.parse_args(["upload", "vo-cuc-thien-ton", "--platform", "youtube"])
    assert args.range is None


def test_pipeline_parser_accepts_upload_flags() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        ["pipeline", "run", "vo-cuc-thien-ton", "--range", "1-10", "--skip-upload", "--upload-platform", "tiktok"]
    )
    assert args.command == "pipeline"
    assert args.pipeline_command == "run"
    assert args.skip_upload is True
    assert args.upload_platform == "tiktok"
    assert args.mode == "per-stage"


def test_pipeline_parser_accepts_mode_override() -> None:
    parser = _build_parser()
    args = parser.parse_args(["pipeline", "run", "vo-cuc-thien-ton", "--range", "1-10", "--mode", "per-video"])

    assert args.command == "pipeline"
    assert args.pipeline_command == "run"
    assert args.mode == "per-video"


def test_youtube_playlist_parser_accepts_optional_id() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "playlist", "--id", "PL1234567890"])

    assert args.command == "youtube"
    assert args.youtube_command == "playlist"
    assert args.id == "PL1234567890"


def test_youtube_playlist_parser_accepts_listing_without_id() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "playlist"])

    assert args.command == "youtube"
    assert args.youtube_command == "playlist"
    assert args.id is None


def test_youtube_playlist_parser_accepts_title_only() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "playlist", "--title-only"])

    assert args.command == "youtube"
    assert args.youtube_command == "playlist"
    assert args.title_only is True


def test_youtube_playlist_update_parser_accepts_update_fields() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "youtube",
            "playlist",
            "update",
            "--id",
            "PL1234567890",
            "--title",
            "New title",
            "--description",
            "New description",
            "--privacy-status",
            "private",
        ]
    )

    assert args.command == "youtube"
    assert args.youtube_command == "playlist"
    assert args.playlist_action == "update"
    assert args.id == "PL1234567890"
    assert args.title == "New title"
    assert args.description == "New description"
    assert args.privacy_status == "private"


def test_youtube_video_parser_accepts_optional_id() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "video", "--id", "vid123"])

    assert args.command == "youtube"
    assert args.youtube_command == "video"
    assert args.id == "vid123"


def test_youtube_video_parser_accepts_title_only() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "video", "--title-only"])

    assert args.command == "youtube"
    assert args.youtube_command == "video"
    assert args.title_only is True


def test_youtube_video_update_parser_accepts_update_fields() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "youtube",
            "video",
            "update",
            "--id",
            "vid123",
            "--title",
            "New title",
            "--description",
            "New description",
            "--privacy_status",
            "private",
            "--made_for_kids",
            "true",
            "--playlist_position",
            "7",
        ]
    )

    assert args.command == "youtube"
    assert args.youtube_command == "video"
    assert args.video_action == "update"
    assert args.id == "vid123"
    assert args.title == "New title"
    assert args.description == "New description"
    assert args.privacy_status == "private"
    assert args.made_for_kids is True
    assert args.playlist_position == 7
