from __future__ import annotations

import importlib
from pathlib import Path

from novel_tts.cli.main import _build_parser, _default_log_path

cli_main = importlib.import_module("novel_tts.cli.main")


def test_youtube_playlist_default_log_path_uses_shared_upload_log() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "playlist"])

    log_path = _default_log_path(args)

    assert log_path == Path.cwd() / ".logs" / "upload" / "youtube" / "playlist.log"


def test_youtube_video_default_log_path_uses_shared_upload_log() -> None:
    parser = _build_parser()
    args = parser.parse_args(["youtube", "video"])

    log_path = _default_log_path(args)

    assert log_path == Path.cwd() / ".logs" / "upload" / "youtube" / "video.log"


def test_youtube_video_logs_result_payload_to_file(monkeypatch, tmp_path: Path) -> None:
    log_path = tmp_path / "video.log"
    monkeypatch.setattr("novel_tts.upload.list_youtube_videos", lambda: [{"id": "vid1", "title": "Video 1"}])

    rc = cli_main.main(["--log-file", str(log_path), "youtube", "video"])

    assert rc == 0
    content = log_path.read_text(encoding="utf-8")
    assert "YouTube video result:" in content
    assert '"id": "vid1"' in content
