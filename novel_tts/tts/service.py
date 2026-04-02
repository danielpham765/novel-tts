from __future__ import annotations

import hashlib
import re
import shutil
import time
from pathlib import Path

from novel_tts.common.ffmpeg import ffprobe_duration, run_ffmpeg
from novel_tts.common.logging import get_logger
from novel_tts.config.models import NovelConfig
from novel_tts.media_batch import media_range_key

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
    return media_range_key(start, end)


def _iter_translated_batch_paths(config: NovelConfig, start: int, end: int) -> list[tuple[int, int, Path]]:
    if not config.storage.translated_dir.exists():
        return []
    pattern = re.compile(r"^chuong_(\d+)-(\d+)\.txt$")
    matched: list[tuple[int, int, Path]] = []
    for path in sorted(config.storage.translated_dir.iterdir()):
        if not path.is_file():
            continue
        item = pattern.match(path.name)
        if item is None:
            continue
        batch_start = int(item.group(1))
        batch_end = int(item.group(2))
        if batch_start <= end and batch_end >= start:
            matched.append((batch_start, batch_end, path))
    matched.sort(key=lambda item: (item[0], item[1]))
    return matched


def _load_translated_text(config: NovelConfig, start: int, end: int) -> tuple[str, list[Path]]:
    matched = _iter_translated_batch_paths(config, start, end)
    if not matched:
        raise FileNotFoundError(
            f"No translated batch files overlap requested range {start}-{end} in {config.storage.translated_dir}"
        )
    paths = [path for _, _, path in matched]
    text = "\n\n".join(path.read_text(encoding="utf-8").strip() for path in paths if path.exists()).strip()
    if not text:
        raise ValueError(f"Translated batch files are empty for requested range {start}-{end}")
    return text, paths


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


def create_menu(config: NovelConfig, start: int, end: int, range_key: str | None = None) -> Path:
    """Create or refresh the chapter menu file.

    If the menu file already exists, its timestamps are reused and chapter
    headings are refreshed from the translated text (no audio files needed).
    If the menu file does not exist, audio parts under .parts/ are required
    to compute timestamps via ffprobe.
    """
    output_range_key = range_key or _range_key(start, end)
    menu_path = config.storage.subtitle_dir / f"{output_range_key}_menu.txt"

    text, source_paths = _load_translated_text(config, start, end)
    _, all_chapter_info = split_text_into_chunks(text)
    chapter_info = [c for c in all_chapter_info if start <= int(c["number"]) <= end]

    if menu_path.exists():
        existing_lines = menu_path.read_text(encoding="utf-8").splitlines()
        timestamps = [line.split(" ", 1)[0] for line in existing_lines if line.strip()]
        if len(timestamps) != len(chapter_info):
            LOGGER.warning(
                "Menu entry count mismatch, skipping | path=%s menu_entries=%s translated_chapters=%s range=%s-%s sources=%s",
                menu_path,
                len(timestamps),
                len(chapter_info),
                start,
                end,
                ",".join(path.name for path in source_paths),
            )
            return menu_path
        lines: list[str] = []
        for ts, chapter in zip(timestamps, chapter_info):
            label = f"Chương {chapter['number']}"
            if chapter.get("title"):
                label += f" - {chapter['title']}"
            lines.append(f"{ts} {label}")
        menu_path.write_text("\n".join(lines), encoding="utf-8")
        LOGGER.info("Menu refreshed | path=%s entries=%s", menu_path, len(lines))
        return menu_path

    output_dir = config.storage.audio_dir / output_range_key
    parts_dir = _chapter_parts_dir(output_dir)
    audio_files: list[Path] = []
    filtered_chapter_info: list[dict[str, object]] = []
    for info in chapter_info:
        f = _chapter_audio_path(parts_dir, int(info["number"]))
        if f.exists() and f.stat().st_size > 0:
            audio_files.append(f)
            filtered_chapter_info.append(info)

    if not audio_files:
        raise FileNotFoundError(f"No audio chapter files found for range {start}-{end} in {parts_dir}")

    menu_path = _generate_menu(config, audio_files, filtered_chapter_info, output_range_key)
    LOGGER.info("Menu created | path=%s entries=%s", menu_path, len(filtered_chapter_info))
    return menu_path


def regenerate_menu(config: NovelConfig, start: int, end: int, range_key: str | None = None) -> Path:
    output_range_key = range_key or _range_key(start, end)
    menu_path = config.storage.subtitle_dir / f"{output_range_key}_menu.txt"
    if not menu_path.exists():
        LOGGER.warning("TTS menu not found, skipping regeneration | path=%s", menu_path)
        return menu_path

    existing_lines = menu_path.read_text(encoding="utf-8").splitlines()
    timestamps = [line.split(" ", 1)[0] for line in existing_lines if line.strip()]

    text, _source_paths = _load_translated_text(config, start, end)
    _, chapter_info = split_text_into_chunks(text)
    chapter_info = [c for c in chapter_info if start <= int(c["number"]) <= end]

    if len(timestamps) != len(chapter_info):
        raise ValueError(
            f"Menu has {len(timestamps)} entries but translated text has {len(chapter_info)} chapters in range {start}-{end}"
        )

    lines: list[str] = []
    for ts, chapter in zip(timestamps, chapter_info):
        label = f"Chương {chapter['number']}"
        if chapter.get("title"):
            label += f" - {chapter['title']}"
        lines.append(f"{ts} {label}")

    menu_path.write_text("\n".join(lines), encoding="utf-8")
    LOGGER.info("TTS menu regenerated | path=%s entries=%s", menu_path, len(lines))
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


def _encode_chapter_aac(wav_path: Path, aac_path: Path, tempo: float, bitrate: str) -> None:
    run_ffmpeg(
        [
            "-y",
            "-i", str(wav_path),
            "-filter:a", f"atempo={tempo}",
            "-acodec", "aac",
            "-b:a", bitrate,
            "-f", "adts",
            str(aac_path),
        ]
    )


def _merge_audio(
    *,
    audio_files: list[Path],
    merged_path: Path,
    tempo: float,
    bitrate: str,
    workers: int,
    tmp_dir: Path,
) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if workers <= 1 or len(audio_files) <= 1:
        # Single-pass: concat all WAVs then apply atempo + encode in one ffmpeg call.
        file_list = tmp_dir.parent / "file-list.txt"
        run_ffmpeg(
            [
                "-y",
                "-f", "concat", "-safe", "0", "-i", str(file_list),
                "-filter:a", f"atempo={tempo}",
                "-acodec", "aac",
                "-b:a", bitrate,
                "-f", "adts",
                str(merged_path),
            ]
        )
        return

    # Parallel: encode each chapter WAV → AAC (ADTS) concurrently, then concat with copy.
    tmp_dir.mkdir(parents=True, exist_ok=True)
    aac_parts: list[Path] = [tmp_dir / f"{wav.stem}.aac" for wav in audio_files]
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_encode_chapter_aac, wav, aac, tempo, bitrate): i
                for i, (wav, aac) in enumerate(zip(audio_files, aac_parts))
            }
            for fut in as_completed(futures):
                fut.result()

        # ADTS is a raw byte-streamable format — frames are self-synchronizing,
        # so direct binary concatenation produces a valid stream without DTS issues.
        with merged_path.open("wb") as out:
            for aac in aac_parts:
                out.write(aac.read_bytes())
    finally:
        for p in aac_parts:
            p.unlink(missing_ok=True)
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


def run_tts(config: NovelConfig, start: int, end: int, range_key: str | None = None, force: bool = False) -> Path:
    text, source_paths = _load_translated_text(config, start, end)
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
        raise ValueError(f"No chapters found for requested range: {start}-{end}")
    
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
        ",".join(path.name for path in source_paths),
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
    merged_path = output_dir / f"{output_range_key}.aac"
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
        LOGGER.info("TTS merge start | inputs=%s output=%s", len(audio_files), merged_path)
        merge_workers = max(1, int(config.tts.merge_workers or 1))
        _merge_audio(
            audio_files=audio_files,
            merged_path=merged_path,
            tempo=config.tts.tempo,
            bitrate=config.tts.bitrate,
            workers=merge_workers,
            tmp_dir=parts_dir / ".merge_tmp",
        )
        _write_merged_cached_hash(parts_dir, expected_merged_hash)
        LOGGER.info("TTS merge done | output=%s size_bytes=%s", merged_path, merged_path.stat().st_size)
    else:
        if cached_merged_hash != expected_merged_hash:
            _write_merged_cached_hash(parts_dir, expected_merged_hash)
        LOGGER.info("TTS merge cached | output=%s size_bytes=%s", merged_path, merged_path.stat().st_size)
    return merged_path
