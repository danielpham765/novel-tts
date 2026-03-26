from __future__ import annotations

import hashlib
import shutil
import time
from pathlib import Path

from novel_tts.common.ffmpeg import ffprobe_duration, run_ffmpeg
from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig

from .providers import get_tts_provider

LOGGER = get_logger(__name__)


def split_text_into_chunks(text: str) -> tuple[list[str], list[dict[str, object]]]:
    chunks: list[str] = []
    chapter_info: list[dict[str, object]] = []
    matches = list(__import__("re").finditer(r"Chương\s+\d+", text))
    if not matches:
        return [text], [{"number": 1, "title": ""}]
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        chunk = text[start:end].strip()
        if not chunk:
            continue
        chunks.append(chunk)
        first_line = chunk.splitlines()[0]
        title_match = __import__("re").match(r"Chương\s+(\d+)[:\-\s]+(.+)", first_line)
        if title_match:
            chapter_info.append({"number": int(title_match.group(1)), "title": title_match.group(2).strip()})
        else:
            num_match = __import__("re").match(r"Chương\s+(\d+)", first_line)
            chapter_info.append({"number": int(num_match.group(1)) if num_match else idx + 1, "title": ""})
    return chunks, chapter_info


def _range_key(start: int, end: int) -> str:
    return f"chuong_{start}-{end}"


def _translated_text_path(config: NovelConfig, start: int, end: int, range_key: str | None = None) -> Path:
    if range_key is None:
        range_key = _range_key(start, end)
        
    direct = config.storage.translated_dir / f"{range_key}.txt"
    if direct.exists():
        return direct
    raise FileNotFoundError(f"Translated range file not found: {direct}")


def _chunk_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _chapter_parts_dir(output_dir: Path) -> Path:
    return output_dir / ".parts"


def _legacy_chapter_audio_path(output_dir: Path, chapter_number: int) -> Path:
    return output_dir / f"chapter_{chapter_number}.wav"


def _chapter_audio_path(parts_dir: Path, chapter_number: int) -> Path:
    return parts_dir / f"chapter_{chapter_number}.wav"


def _chapter_hash_cache_dir(parts_dir: Path) -> Path:
    # Keep cache metadata out of the main audio folder to reduce clutter.
    return parts_dir / ".cache"


def _chapter_hash_path(parts_dir: Path, chapter_number: int) -> Path:
    return _chapter_hash_cache_dir(parts_dir) / f"chapter_{chapter_number}.sha256"


def _merged_audio_hash_path(parts_dir: Path) -> Path:
    return _chapter_hash_cache_dir(parts_dir) / "merged.sha256"


def _legacy_chapter_hash_path(output_dir: Path, chapter_number: int) -> Path:
    # Oldest layout: hashes lived next to chapter wavs.
    return output_dir / f"chapter_{chapter_number}.sha256"


def _legacy_merged_audio_hash_path(output_dir: Path) -> Path:
    return output_dir / "merged.sha256"


def _legacy_chapter_hash_cache_path(output_dir: Path, chapter_number: int) -> Path:
    # Previous layout (after refactor): hashes lived under output_dir/.cache
    return (output_dir / ".cache") / f"chapter_{chapter_number}.sha256"


def _legacy_merged_audio_hash_cache_path(output_dir: Path) -> Path:
    return (output_dir / ".cache") / "merged.sha256"


def _read_cached_hash(parts_dir: Path, *, output_dir: Path, chapter_number: int) -> str | None:
    """
    Read cached sha256 for a chapter.

    Supports legacy locations by migrating into `output_dir/.parts/.cache/`.
    """
    hash_path = _chapter_hash_path(parts_dir, chapter_number)
    if hash_path.exists():
        try:
            return hash_path.read_text(encoding="utf-8").strip() or None
        except Exception:
            return None

    legacy_paths = [
        _legacy_chapter_hash_cache_path(output_dir, chapter_number),
        _legacy_chapter_hash_path(output_dir, chapter_number),
    ]
    for legacy_path in legacy_paths:
        if not legacy_path.exists():
            continue
        try:
            value = legacy_path.read_text(encoding="utf-8").strip()
        except Exception:
            value = ""
        try:
            hash_path.parent.mkdir(parents=True, exist_ok=True)
            if value:
                hash_path.write_text(value, encoding="utf-8")
            legacy_path.unlink(missing_ok=True)
        except Exception:
            pass
        return value or None

    return None


def _write_cached_hash(parts_dir: Path, chapter_number: int, value: str) -> None:
    path = _chapter_hash_path(parts_dir, chapter_number)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _read_merged_cached_hash(parts_dir: Path, *, output_dir: Path) -> str | None:
    hash_path = _merged_audio_hash_path(parts_dir)
    if hash_path.exists():
        try:
            return hash_path.read_text(encoding="utf-8").strip() or None
        except Exception:
            return None

    legacy_paths = [
        _legacy_merged_audio_hash_cache_path(output_dir),
        _legacy_merged_audio_hash_path(output_dir),
    ]
    for legacy_path in legacy_paths:
        if not legacy_path.exists():
            continue
        try:
            value = legacy_path.read_text(encoding="utf-8").strip()
        except Exception:
            value = ""
        try:
            hash_path.parent.mkdir(parents=True, exist_ok=True)
            if value:
                hash_path.write_text(value, encoding="utf-8")
            legacy_path.unlink(missing_ok=True)
        except Exception:
            pass
        return value or None

    return None


def _write_merged_cached_hash(parts_dir: Path, value: str) -> None:
    path = _merged_audio_hash_path(parts_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def _merged_cache_value(
    chapter_hashes: list[tuple[int, str]],
    *,
    tempo: float,
    bitrate: str,
) -> str:
    payload = "\n".join(
        [
            f"tempo={tempo}",
            f"bitrate={bitrate}",
            *[f"chapter={chapter_number}:{chapter_hash}" for chapter_number, chapter_hash in chapter_hashes],
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _generate_menu(config: NovelConfig, files: list[Path], chapter_info: list[dict[str, object]], range_key: str) -> Path:
    subtitle_dir = config.storage.subtitle_dir
    subtitle_dir.mkdir(parents=True, exist_ok=True)
    menu_path = subtitle_dir / f"{range_key}_menu.txt"
    lines: list[str] = []
    current_time = 0.0
    for idx, file in enumerate(files):
        duration = ffprobe_duration(file) / config.tts.tempo
        hours = int(current_time // 3600)
        minutes = int((current_time % 3600) // 60)
        seconds = int(current_time % 60)
        timestamp = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        chapter = chapter_info[idx] if idx < len(chapter_info) else {"number": idx + 1, "title": ""}
        label = f"Chương {chapter['number']}"
        if chapter.get("title"):
            label += f" - {chapter['title']}"
        lines.append(f"{timestamp} {label}")
        current_time += duration
    menu_path.write_text("\n".join(lines), encoding="utf-8")
    return menu_path


def _remove_incomplete_merged_artifacts(output_dir: Path, parts_dir: Path, merged_path: Path) -> None:
    try:
        merged_path.unlink(missing_ok=True)
    except Exception:
        pass
    try:
        _merged_audio_hash_path(parts_dir).unlink(missing_ok=True)
    except Exception:
        pass
    try:
        _legacy_merged_audio_hash_cache_path(output_dir).unlink(missing_ok=True)
    except Exception:
        pass
    try:
        _legacy_merged_audio_hash_path(output_dir).unlink(missing_ok=True)
    except Exception:
        pass


def run_tts(config: NovelConfig, start: int, end: int, range_key: str | None = None, force: bool = False) -> Path:
    translated_range_key = range_key
    source_path = _translated_text_path(config, start, end, translated_range_key)
    text = source_path.read_text(encoding="utf-8")
    chunks, chapter_info = split_text_into_chunks(text)
    
    # Filter chunks cleanly based on the slice we want
    filtered_chunks = []
    filtered_chapter_info = []
    for chunk, info in zip(chunks, chapter_info):
        if start <= info["number"] <= end:
            filtered_chunks.append(chunk)
            filtered_chapter_info.append(info)
            
    chunks = filtered_chunks
    chapter_info = filtered_chapter_info
    if not chunks:
        raise ValueError(f"No chapters found for requested range: {start}-{end} (source={source_path})")
    
    output_range_key = _range_key(start, end)
        
    output_dir = config.storage.audio_dir / output_range_key
    output_dir.mkdir(parents=True, exist_ok=True)
    parts_dir = _chapter_parts_dir(output_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)
    legacy_file_list_path = output_dir / "file-list.txt"
    file_list_path = parts_dir / "file-list.txt"
    if legacy_file_list_path.exists() and (not file_list_path.exists()):
        # Best-effort migration; we'll overwrite with a fresh list at the end anyway.
        try:
            legacy_file_list_path.replace(file_list_path)
        except Exception:
            pass
    LOGGER.info(
        "TTS start | range=%s chapters=%s source=%s server=%s model=%s voice=%s",
        output_range_key,
        len(chunks),
        source_path,
        config.tts.server_name,
        config.tts.model_name,
        config.tts.voice,
    )
    provider = get_tts_provider(config)
    client = provider.connect()
    LOGGER.info("TTS load model | server=%s model=%s", config.tts.server_name, config.tts.model_name)
    provider.load_model(client)
    LOGGER.info("TTS model ready")
    audio_files: list[Path] = []
    chapter_hashes: list[tuple[int, str]] = []
    regenerated_any_audio = False
    for idx, chunk in enumerate(chunks):
        chapter = chapter_info[idx] if idx < len(chapter_info) else {"number": idx + 1, "title": ""}
        chapter_number = int(chapter.get("number") or (idx + 1))
        output_path = _chapter_audio_path(parts_dir, chapter_number)
        legacy_output_path = _legacy_chapter_audio_path(output_dir, chapter_number)
        if (not output_path.exists()) and legacy_output_path.exists():
            try:
                legacy_output_path.replace(output_path)
            except Exception:
                pass
        expected_hash = _chunk_hash(chunk)
        chapter_hashes.append((chapter_number, expected_hash))
        chapter_label = f"chapter={chapter.get('number', idx + 1)}"
        if chapter.get("title"):
            chapter_label += f" title={chapter['title']}"
        cached_hash = _read_cached_hash(parts_dir, output_dir=output_dir, chapter_number=chapter_number)
        if (
            (not force)
            and output_path.exists()
            and output_path.stat().st_size > 0
            and cached_hash is not None
            and cached_hash == expected_hash
        ):
            LOGGER.info(
                "TTS chapter cached | %s index=%s/%s path=%s size_bytes=%s",
                chapter_label,
                idx + 1,
                len(chunks),
                output_path,
                output_path.stat().st_size,
            )
            audio_files.append(output_path)
            continue
        if not force:
            reason = "hash-mismatch"
            if not output_path.exists() or output_path.stat().st_size <= 0:
                reason = "missing-audio"
            elif cached_hash is None:
                reason = "missing-hash"
            LOGGER.info(
                "TTS chapter cache miss | %s index=%s/%s reason=%s",
                chapter_label,
                idx + 1,
                len(chunks),
                reason,
            )
        LOGGER.info(
            "TTS chapter start | %s index=%s/%s chars=%s output=%s",
            chapter_label,
            idx + 1,
            len(chunks),
            len(chunk),
            output_path,
        )
        chapter_started_at = time.monotonic()
        
        # State for throttling logs
        last_iterating_time = [0.0]

        def _throttled_progress(message: str, cl=chapter_label, i=idx, t=len(chunks)):
            if message == "ITERATING":
                now = time.monotonic()
                if now - last_iterating_time[0] >= 15.0:
                    last_iterating_time[0] = now
                    LOGGER.info("TTS chapter progress | %s index=%s/%s %s", cl, i + 1, t, message)
            else:
                LOGGER.info("TTS chapter progress | %s index=%s/%s %s", cl, i + 1, t, message)

        result = provider.synthesize(
            client,
            chunk,
            progress_callback=_throttled_progress,
        )
        audio_result = provider.materialize_output_audio(client, result)
        try:
            shutil.copyfile(audio_result.local_path, output_path)
        finally:
            try:
                if audio_result.local_path.is_file() and audio_result.local_path.parent == (config.storage.tmp_dir / "tts_downloads"):
                    audio_result.local_path.unlink(missing_ok=True)
            except Exception:
                pass
        provider.cleanup_output_audio(client, audio_result.cleanup_target)
        _write_cached_hash(parts_dir, chapter_number, expected_hash)
        elapsed = time.monotonic() - chapter_started_at
        LOGGER.info(
            "TTS chapter done | %s index=%s/%s elapsed=%.1fs path=%s size_bytes=%s",
            chapter_label,
            idx + 1,
            len(chunks),
            elapsed,
            output_path,
            output_path.stat().st_size,
        )
        regenerated_any_audio = True
        audio_files.append(output_path)

    file_list_path.write_text("\n".join(f"file '{path.resolve()}'" for path in audio_files), encoding="utf-8")
    # Keep the range folder clean: remove any legacy file-list in the old location.
    try:
        legacy_file_list_path.unlink(missing_ok=True)
    except Exception:
        pass
    merged_path = output_dir / f"{output_range_key}.mp3"
    expected_parts = max(1, end - start + 1)
    available_parts = len(audio_files)
    if available_parts < expected_parts:
        _remove_incomplete_merged_artifacts(output_dir, parts_dir, merged_path)
        LOGGER.info(
            "TTS merge gated | range=%s parts=%s/%s merged_output=%s",
            output_range_key,
            available_parts,
            expected_parts,
            merged_path,
        )
        return merged_path

    expected_merged_hash = _merged_cache_value(
        chapter_hashes,
        tempo=config.tts.tempo,
        bitrate=config.tts.bitrate,
    )
    cached_merged_hash = _read_merged_cached_hash(parts_dir, output_dir=output_dir)
    merged_exists = merged_path.exists() and merged_path.stat().st_size > 0
    should_merge = force or regenerated_any_audio or (not merged_exists)
    if (not should_merge) and cached_merged_hash is not None and cached_merged_hash != expected_merged_hash:
        should_merge = True
    if should_merge:
        LOGGER.info("TTS merge start | inputs=%s file_list=%s output=%s", len(audio_files), file_list_path, merged_path)
        run_ffmpeg(
            [
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(file_list_path),
                "-filter:a",
                f"atempo={config.tts.tempo}",
                "-acodec",
                "libmp3lame",
                "-b:a",
                config.tts.bitrate,
                str(merged_path),
            ]
        )
        _write_merged_cached_hash(parts_dir, expected_merged_hash)
        LOGGER.info("TTS merge done | output=%s size_bytes=%s", merged_path, merged_path.stat().st_size)
    else:
        if cached_merged_hash != expected_merged_hash:
            _write_merged_cached_hash(parts_dir, expected_merged_hash)
        LOGGER.info("TTS merge cached | output=%s size_bytes=%s", merged_path, merged_path.stat().st_size)
    menu_path = _generate_menu(config, audio_files, chapter_info, output_range_key)
    LOGGER.info("TTS menu generated | path=%s", menu_path)
    return merged_path
