## Purpose

This document is a **token-efficient architecture guide for Cursor AIs** working on `novel-tts`.

It focuses on:

- **Where to start reading**
- **What invariants must never be broken**
- **Which files to touch for common changes**

If you need deeper background, prefer `docs/ARCHITECTURE.md` rather than re-discovering structure from scratch.

---

## Fast Mental Model

- **Shape**: one Python package `novel_tts` + one CLI entrypoint `novel-tts`.
- **Pipeline** (file-first, stage-separated):
  - **crawl** → writes `input/<novel>/origin/*.txt`
  - **translate** → writes `input/<novel>/.parts/...` and rebuilds `translated/*.txt`
  - **queue** → distributes chapter translation via Redis, but truth still lives on disk
  - **quota-supervisor** → global Redis-backed quota gate (RPM/TPM/RPD) used by queue translation
  - **tts** → reads `translated/*.txt`, writes `output/<novel>/audio/...`
  - **media** (`visual`, `video`) → reads audio + config, writes `output/<novel>/visual` and `video`
- **State**: filesystem is primary storage; Redis is for queue + quota bookkeeping only.

Think of the system as:

> **config assembly → crawl origin → translate (.parts + translated) → tts audio → media visual/video**

---

## Read This First (for AIs)

If you are trying to understand or modify behavior, **load these files in roughly this order**:

1. **High-level docs**
   - `README.md` – CLI overview and typical commands.
   - `docs/ARCHITECTURE.md` – full developer architecture.
2. **Agent-oriented summary**
   - `docs/agents/codex/AGENTS.md` – concise rules and module map (still valid for Cursor).
3. **Repo hygiene**
   - Avoid loading large/generated artifacts into context.
   - Do **not** read these folders (or files under them) unless the task explicitly requires it: `./input`, `./output`, `./image`, `./tmp`, `./.logs`, `./.secrets`, `./.venv`, `./tests`.
   - When you must inspect artifacts, prefer targeted, minimal reads (one file, small excerpts).
4. **Refactor/architecture rule**
   - When doing refactor/architecture/strategy changes, avoid keeping aliases/backward-compatible code paths by default.
   - Prefer a complete migration (update call sites/configs/docs) so the codebase has one consistent design.
5. **Runtime entrypoints**
   - `novel_tts/cli/main.py` – argument parsing, dispatch, logging decisions.
   - `novel_tts/config/loader.py` & `novel_tts/config/models.py` – how config is loaded and typed.
6. **Subsystem roots**
   - Crawl: `novel_tts/crawl/service.py`, `novel_tts/crawl/registry.py`, `novel_tts/crawl/resolvers/*.py`
   - Translate: `novel_tts/translate/novel.py`, `novel_tts/translate/providers.py`
   - Queue: `novel_tts/queue/translation_queue.py`
   - TTS: `novel_tts/tts/service.py`, `novel_tts/tts/providers.py`
   - Media: `novel_tts/media/service.py`
7. **Common utilities**
   - `novel_tts/common/logging.py`, `novel_tts/common/text.py`, `novel_tts/common/ffmpeg.py`, `novel_tts/common/subprocesses.py`

When answering questions or making edits, **anchor your reasoning** to these files instead of re-traversing the entire tree.

---

## Runtime Config & Storage

- **Loader**: `novel_tts.config.loader.load_novel_config()`
- **Sources**:
  - `configs/novels/<novel>.json`
  - `configs/sources/<source>.json`
  - `configs/app.yaml`
  - `configs/glossaries/<novel>.json` (or explicit glossary file)
  - selected environment variables
- **Dataclass root**: `NovelConfig` with key sections:
  - `storage`, `crawl`, `browser_debug`, `translation`, `captions`, `queue`, `tts`, `visual`, `video`

**Per-novel storage layout** (contract relied on across subsystems):

- Under `input/<novel>/`:
  - `origin/*.txt` – crawled source batches (truth for chapter discovery).
  - `translated/*.txt` – rebuilt translated batches (derived from `.parts`).
  - `captions/*.srt` – caption inputs/outputs.
  - `.parts/<origin_stem>/<chapter>.txt` – **canonical per-chapter translation truth**.
  - `.progress/*.json` – resumable state (crawl failures, translation chunk progress, etc.).
- Under `output/<novel>/`:
  - `audio/<range>/` – wav chunks + merged `chuong_<start>-<end>.mp3`.
  - `subtitle/` – timecoded menu text files.
  - `visual/` – overlay video & thumbnails.
  - `video/` – final muxed mp4.
- Logs:
  - `.logs/<novel>/<command>.log` – per-command logs, chosen by `novel_tts/cli/main.py`.

When modifying behavior, **preserve these directory and filename contracts** or audit all downstream call sites.

---

## Subsystem Overview (Cursor-Oriented)

### CLI Layer

- File: `novel_tts/cli/main.py`
- Role: thin dispatcher:
  - Parses commands: `crawl`, `translate`, `queue`, `ai-key`, `quota-supervisor`, `tts`, `visual`, `video`, `pipeline`.
  - Computes default log file (`_default_log_path`).
  - Loads `NovelConfig` and calls the right service function.
  - Keeps backward compatibility for old `crawl <novel>` syntax.
- **Guideline**: keep business logic in the service modules; keep CLI focused on I/O, parsing, and orchestration.

### Crawl

- Files: `novel_tts/crawl/service.py`, `strategies.py`, `challenge.py`, `base.py`, `registry.py`, `resolvers/*.py`
- Responsibilities:
  - Site-specific HTML parsing (resolvers).
  - Strategy chain for HTTP vs browser-bootstrapped vs Playwright fallback.
  - Crawl failure manifest + verification against saved files.
  - Writes `origin/*.txt` + `.progress/crawl_failures.json`.
- Extension:
  - Add/modify resolver → `resolvers/*.py` + registry + `configs/sources/*.json`.
  - Be careful with **batch filename semantics** and **failure manifest format**.

### Translation

- Files: `novel_tts/translate/novel.py`, `providers.py`, `glossary.py`, `captions.py`, `polish.py`, `repair.py`
- Responsibilities:
  - Split `origin/*.txt` into chapters (`translation.chapter_regex`).
  - Translate chapter-by-chapter, checkpointing at chunk level.
  - Heavy multi-pass cleanup for residual Han characters.
  - Glossary placeholder flow and optional auto-update.
  - Rebuild `translated/*.txt` from `.parts`.
  - Scan translated outputs and enqueue repair jobs back into Redis (`translate repair`).
- Provider abstraction:
  - `gemini_http`, `openai_chat` via `get_translation_provider`.
- **Key functions to know**:
  - `translate_novel`, `translate_chapter`, `rebuild_translated_file`, `translate_unit`.
  - `translate_unit` contains most of the **repair and cleanup pipeline** – high leverage but delicate.

### Queue

- File: `novel_tts/queue/translation_queue.py`
- Responsibilities:
  - Discover un-translated chapters by inspecting `.parts` vs `origin`.
  - Enqueue jobs in Redis (`pending/queued/inflight/retries/done`).
  - Spawn workers that run `python -m novel_tts translate chapter ...`.
  - Optionally enqueue a special captions job id: `captions` (runs `translate captions`).
  - Enable the central quota gate for translate subprocesses (`NOVEL_TTS_CENTRAL_QUOTA=1`, `GEMINI_REDIS_*`).
  - Launch/monitor processes for a novel (`queue launch`).
- Design:
  - Redis stores **job + quota bookkeeping only**; translation truth is still on disk.
  - Job id: `<origin_file_name>::<chapter_num>`.

### TTS

- Files: `novel_tts/tts/service.py`, `providers.py`
- Responsibilities:
  - Read a translated batch for a range `chuong_<start>-<end>.txt`.
  - Split into chapter chunks (regex on `Chương <n>`).
  - Call TTS provider (`gradio_vie_tts` currently) per chunk.
  - Merge wavs into one mp3 with `ffmpeg`, respecting tempo.
  - Generate chapter menu text file with timestamps.
- **Caching**:
  - If a chunk `.wav` already exists and is non-empty, it is reused (important for idempotent runs).

### Media

- File: `novel_tts/media/service.py`
- Responsibilities:
  - Render overlay text over background video using FFmpeg.
  - Loop visual track to match audio duration.
  - Mux audio + visual into final `video/<range>.mp4`.
- Source of truth for overlay content: `config.visual` and generated subtitles.

### Common Utilities

- Files: `novel_tts/common/logging.py`, `ffmpeg.py`, `text.py`, `subprocesses.py`
- Roles:
  - Configure logging and log file paths.
  - Wrap `ffmpeg` / `ffprobe`.
  - Parse text ranges (`parse_range`), normalize whitespace.
  - Convenience subprocess wrapper.

---

## Critical Invariants (Do Not Break)

When editing with Cursor, keep these **hard constraints** in mind:

1. **Storage contract**
   - `origin/*.txt` is the **source of truth** for chapter discovery.
   - `.parts/<batch>/<chapter>.txt` is the **canonical per-chapter translation state**.
   - `translated/*.txt` is **rebuilt** from `.parts` and is not the primary truth.
2. **Chapter heading format**
   - Translation and TTS assume headings of the form `Chương <n>` (optionally with a title).
   - `translation.chapter_regex` in config must stay consistent with how `origin` files are written.
3. **Queue semantics**
   - Redis key naming (`pending`, `queued`, `inflight`, `retries`, `done`) and job id format must remain compatible unless you migrate all usage.
   - Workers must continue to call the CLI translation entrypoint, not copy logic.
4. **Idempotency & resumability**
   - `.progress/*.json` files and `.parts` layout enable safe restarts.
   - Do not convert operations into all-or-nothing behaviors that delete partial results.
5. **External tool assumptions**
  - FFmpeg/FFprobe availability and CLI flags.
  - Gradio TTS server and model settings from `configs/providers/*.yaml`.
  - Redis presence for queue mode.
  - `quota-supervisor` should be running for queue translation (it grants central quota requests and publishes ETA hints used by `queue ps-all`).

If a change touches any of the above, you must **inspect and update all dependent subsystems**, not just local code.

---

## Common Change Workflows (for AIs)

### 1. Add or Adjust Crawl Behavior

- Likely files:
  - `novel_tts/crawl/resolvers/*.py`
  - `novel_tts/crawl/service.py`
  - `configs/sources/*.json`
- Be careful about:
  - `origin` filename convention and ranges.
  - `crawl_failures.json` schema.
  - Challenge / rate-limit detection thresholds.

### 2. Tune Translation Cleanup or Style

- Likely files:
  - `novel_tts/translate/novel.py`
  - `novel_tts/translate/polish.py`
  - `novel_tts/translate/glossary.py`
- Guard rails:
  - Preserve `translate_unit` resumability (progress files, `.parts` writing).
  - Keep glossary placeholder flow intact (placeholder → translate → restore).
  - Avoid introducing extra Han characters.

### 3. Change Queue Concurrency or Behavior

- File: `novel_tts/queue/translation_queue.py`
- Be careful about:
  - Redis key structure and prefixes.
  - Worker command lines (arguments must stay aligned with CLI parser).
  - Exit code interpretation for success/failure.

### 4. Adjust TTS or Media Output

- Files:
  - `novel_tts/tts/service.py`, `novel_tts/tts/providers.py`
  - `novel_tts/media/service.py`
- Check:
  - Expected presence of translated range file.
  - Menu file format (timestamps + labels).
  - FFmpeg filters and bitrate/tempo options.

---

## Cursor-Specific Guidance

- **Prefer small, local edits**:
  - When responding to user edits, modify **one subsystem at a time** and keep public contracts intact.
- **Always consult config & storage first**:
  - Before changing logic, inspect `NovelConfig` fields and the on-disk layout for the novel in question.
- **Use existing helpers**:
  - Reuse `parse_range`, logging helpers, FFmpeg wrappers, and provider factories instead of re-implementing.
- **Explain impact in terms of stages**:
  - When describing changes, map them to “crawl / translate / queue / tts / media” so humans can follow the pipeline.

This document is intentionally compact so it can be **loaded early in a Cursor session** and reused via caching to minimize tokens while still giving an accurate mental model of `novel-tts`.
