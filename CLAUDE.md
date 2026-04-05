# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Developer Setup

```bash
uv sync
uv run novel-tts --help
```

Optional stage requirements:
- Crawl (JS-heavy sites): `uv run playwright install chromium`
- Queue translation: Redis at `127.0.0.1:6379` (db `1`)
- TTS/media: `ffmpeg` + `ffprobe`
- TTS: reachable Gradio server configured in novel/app config

## Running Tests

```bash
uv run pytest tests/
uv run pytest tests/test_foo.py::test_bar   # single test
```

## Getting Coding Context

Use the compact task map before loading subsystem files:

```bash
uv run novel-tts-context --list
uv run novel-tts-context translate
uv run novel-tts-context queue
```

Source of truth: `docs/agents/context-map.yaml`

## Architecture

**One package** (`novel_tts`), **one CLI entrypoint** (`novel_tts.cli.main`). The system is intentionally file-first: stages communicate through files under `input/<novel_id>/` and `output/<novel_id>/`. Redis is only for queue/quota/telemetry bookkeeping—never for storing translated text.

### Pipeline stages (in order)

1. **Crawl** → `input/<novel>/origin/*.txt`
2. **Translate** → `input/<novel>/.parts/<batch>/<chapter>.txt` (canonical), rebuilt to `input/<novel>/translated/*.txt`
3. **Queue / quota** → Redis-backed job + quota bookkeeping only; translated text still lives on disk
4. **TTS** → `output/<novel>/audio/<range>/`
5. **Visual** → `output/<novel>/visual/`
6. **Video** (mux) → `output/<novel>/video/`
7. **Upload** → YouTube (real) or TikTok (dry-run)

Top-level CLI command families currently include:

- `crawl`, `translate`, `queue`, `background`, `tts`, `create-menu`, `visual`, `video`
- `upload`, `youtube`, `pipeline`, `glossary`, `quota-supervisor`, `ai-key`

### Config assembly

`novel_tts.config.loader.load_novel_config()` merges:
- `configs/novels/<novel_id>.yaml` (required root)
- `configs/sources/<source_id>.json`
- `configs/app.yaml` + optional `configs/app.local.yaml`
- `configs/glossaries/<novel_id>/glossary.json`
- `configs/polish_replacement/common.json` + `configs/polish_replacement/<novel_id>.json`
- selected environment variables

Result is a typed `NovelConfig` dataclass graph (`novel_tts/config/models.py`).

Important runtime sections:

- `storage`, `crawl`, `models`, `translation`, `captions`, `queue`
- `tts`, `media`, `upload`, `pipeline`, `proxy_gateway`

### Translation truth and `.parts` canonical state

Translation is chapter-granular even when crawl inputs are batch-granular.
- `.parts/<origin_stem>/<chapter>.txt` is the real completion state
- `translated/*.txt` is derived and rebuildable from `.parts` via `rebuild_translated_file()`
- Staleness is tracked per-chapter via `.parts/<origin_stem>/<chapter>.sha256`

### Queue mode (preferred for production)

All novels share **one queue, one supervisor, one set of workers**. Workers pick any job from the shared queue and load the appropriate `NovelConfig` per job (60s LRU cache).

**Job ID format:** `{novel_id}::{file}::{chapter}` (e.g. `tram-than::book1.txt::0042`)

**Redis key layout:**
- Global keys (shared): `{prefix}:pending`, `{prefix}:queued`, `{prefix}:inflight`, `{prefix}:force`, `{prefix}:stopping`
- Per-novel keys: `{prefix}:novel:{novel_id}:done`, `{prefix}:novel:{novel_id}:retries`, etc.
- Rate-limit/quota keys: indexed by worker key slot, not by novel

**CLI commands** (`queue supervisor/monitor/worker/launch/stop/reset-key`) no longer take a novel_id positional arg. Use `queue launch --novel <id>` to enqueue jobs for a novel on start.

**`queue ps-all` output:** Global stats header → per-novel done/retries lines → single unified worker table.

Workers spawn `python -m novel_tts translate chapter <novel_id> ...` subprocesses. Exit codes `75` (rate-limit) and `76` (quota gate) feed back into worker retry/hold logic. The global `quota-supervisor` process coordinates RPM/TPM/RPD across all workers via Redis Lua-based grant logic—run it whenever queue workers are active.

**Shared log dir:** `.logs/_shared/queue/` (supervisor, monitor, workers all log here)

### Heading format contract

- Crawl/origin headings: ASCII `Chuong <n> ...`
- Translated/TTS/media headings: Vietnamese `Chương <n> ...`

Changing heading formats cascades to: chapter splitting, translated rebuild, TTS chunk detection, subtitle menu generation, and media packaging.

## Key Files by Task

| Task | Read first |
|------|-----------|
| translate | `novel_tts/translate/novel.py`, `novel_tts/translate/providers.py` |
| queue | `novel_tts/queue/translation_queue.py`, `novel_tts/translate/novel.py` |
| tts | `novel_tts/tts/service.py`, `novel_tts/tts/providers.py` |
| media/upload | `novel_tts/media/service.py`, `novel_tts/upload/service.py` |
| glossary/pipeline | `novel_tts/translate/glossary.py`, `novel_tts/pipeline/watch.py` |
| config/CLI wiring | `novel_tts/config/loader.py`, `novel_tts/config/models.py`, `novel_tts/cli/main.py` |

Large files to load only when the bug clearly points there: `novel_tts/cli/main.py`, `novel_tts/translate/novel.py`, `novel_tts/queue/translation_queue.py`, `novel_tts/config/loader.py`.

## Do Not Read (Unless Task Explicitly Requires It)

- `input/`, `output/`, `image/`, `tmp/`, `.logs/`, `.secrets/`, `.venv/`, `tests/`

## Refactor/Architecture Change Rules

- Avoid keeping alias paths or backward-compatible behavior by default.
- Prefer a complete migration: update all call sites, configs, and docs to the new shape so the codebase has one consistent design.
- Only keep backward-compatibility when explicitly required for a real operational reason; if you must, make it time-boxed with a clear removal follow-up.

## Extension Points

- **New crawl source**: add resolver under `novel_tts/crawl/resolvers/`, register in `build_default_registry()`, add `configs/sources/<source>.json`
- **New translation provider**: implement provider class, register in `get_translation_provider()`
- **New TTS backend**: extend `novel_tts/tts/providers.py` and provider config YAMLs
- **New upload platform**: extend `novel_tts/upload/service.py` and upload config models

When extending, preserve file contracts first—most of the repo assumes the on-disk layout stays stable.

## Debug Approach

Inspect the nearest persisted artifact first:
- crawl: `origin/`, `.progress/crawl_failures.json`, `debug/img/`
- translate: `.parts/`, `.progress/*.json`, glossary file
- queue: Redis counts + logs + missing `.parts`
- TTS: translated range file + wav chunks under `audio/<range>/.parts/`
- media: background asset + visual MP4 + ffmpeg logs
