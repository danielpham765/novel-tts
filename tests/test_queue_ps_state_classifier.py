from __future__ import annotations

import tempfile

from novel_tts.queue.translation_queue import _classify_process_state


def test_worker_error_is_not_sticky_after_success() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | ERROR | x | Traceback (most recent call last):\n")
        fh.write("2026-03-16 06:00:01,000 | INFO | x | Worker done: chuong_1-10.txt::0001\n")
        fh.flush()
        state, countdown = _classify_process_state("worker", is_busy=False, log_file=fh.name)
        assert state == "idle"
        assert countdown is None


def test_rate_limit_requeue_delay_backoff() -> None:
    from novel_tts.queue.translation_queue import _rate_limit_requeue_delay_seconds

    assert _rate_limit_requeue_delay_seconds(1) == 3.0
    assert _rate_limit_requeue_delay_seconds(2) == 6.0
    assert _rate_limit_requeue_delay_seconds(3) == 12.0
    assert _rate_limit_requeue_delay_seconds(4) == 24.0
    assert _rate_limit_requeue_delay_seconds(5) == 48.0
    assert _rate_limit_requeue_delay_seconds(6) == 60.0
    assert _rate_limit_requeue_delay_seconds(20) == 60.0


def test_worker_error_is_held_briefly() -> None:
    # Use timestamps close to "now" so the 5s error-hold window triggers.
    import datetime as _dt

    now = _dt.datetime.now()
    stamp = now.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write(f"{stamp} | ERROR | x | Command failed\n")
        fh.flush()
        state, countdown = _classify_process_state("worker", is_busy=False, log_file=fh.name)
        assert state == "error"
        assert countdown is None


def test_translate_chapter_infers_translate_phase_from_chunk_logs() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | INFO | x | Translating chuong_1-10.txt::0001 chunk 2/9\n")
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "translate"
        assert countdown is None


def test_translate_chapter_infers_repair_phase_from_han_repair_logs() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | INFO | x | Han residue detected; aggressive repair | unit=u\n")
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "repair"
        assert countdown is None


def test_translate_chapter_infers_glossary_phase_from_glossary_logs() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write("2026-03-16 06:00:00,000 | INFO | x | Glossary extract chunked | unit=u windows=3\n")
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "glossary"
        assert countdown is None


def test_translate_chapter_infers_upstream_timeout_from_requests_log() -> None:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", delete=True) as fh:
        fh.write(
            "2026-03-16 06:00:00,000 | WARNING | novel_tts.translate.providers | "
            "Gemini API generation error (attempt 1/12): HTTPSConnectionPool(host='generativelanguage.googleapis.com', port=443): "
            "Read timed out. (read timeout=90)\n"
        )
        fh.flush()
        state, countdown = _classify_process_state("translate-chapter", is_busy=True, log_file=fh.name)
        assert state == "upstream-timeout"
        assert countdown is None
