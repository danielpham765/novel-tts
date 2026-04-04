# novel-tts Architecture for Codex

## Purpose

`novel-tts` is a file-first CLI pipeline:

1. crawl source novel text
2. translate to Vietnamese
3. optionally queue translation via Redis
4. coordinate RPM/TPM/RPD via a global quota gate (`quota-supervisor`)
5. inspect per-key telemetry (Redis-backed)
6. synthesize audio
7. render visual
8. mux final video
9. upload video (YouTube; TikTok dry-run scaffold)

Primary package: `novel_tts`

Primary entrypoints:

- `novel_tts/__main__.py`
- `novel_tts/cli/main.py`

Console script:

- `novel-tts`
- `novel-tts-context` (compact task-specific coding context)

## Fastest Narrow-Context Path

Before loading subsystem code, start here:

```bash
uv run novel-tts-context --list
uv run novel-tts-context translate
uv run novel-tts-context queue
```

Source of truth for this compact map:

- `docs/agents/context-map.yaml`

Use it to decide which 2-4 files to read first for a task, and which large files to avoid unless the bug clearly points there.

## Core Rule

Most stages do not talk directly to each other.
They communicate through files under `input/<novel>` and `output/<novel>`.

If changing behavior, preserve artifact contracts.

## AI Agent Read Rules (Repo Hygiene)

Avoid loading large/generated artifacts into context.
Do **not** read these folders (or files under them) unless the task explicitly requires it:

- `./input`
- `./output`
- `./image`
- `./tmp`
- `./.logs`
- `./.secrets`
- `./.venv`
- `./tests`

If you must inspect artifacts, do it surgically (one file, small excerpts).

## AI Agent Change Rules (Refactor/Architecture)

When doing a **refactor**, **architecture change**, or **strategy change**:

- Do not default to aliases/backward-compatible code paths.
- Prefer a **clean cutover**: update all call sites, configs, and docs so there is only one “true” design in the code.
- Only keep backward-compatibility when explicitly required; make it time-boxed and plan removal to avoid long-term code rot.

## Runtime Config

Loaded by:

- `novel_tts.config.loader.load_novel_config`

Merged from:

- `configs/novels/<novel>.yaml`
- `configs/sources/<source>.json`
- `configs/app.yaml`
- glossary JSON
- selected env vars

Dataclass root:

- `NovelConfig`

Important sections:

- `storage`
- `crawl`
- `crawl.browser_debug`
- `models`
- `translation`
- `captions`
- `queue`
- `tts`
- `media`
- `media.visual`
- `media.video`
- `media.media_batch`
- `upload`
- `pipeline`
- `proxy_gateway`

## Storage Contract

Per novel under `input/<novel>/`:

- `origin/*.txt`: crawled source batches
- `translated/*.txt`: rebuilt translated batches
- `captions/*.srt`: caption in/out
- `.parts/<origin_stem>/<chapter>.txt`: per-chapter translation truth
- `.progress/*.json`: resumable state and crawl failures

Per novel under `output/<novel>/`:

- `audio/<range>/`
- `subtitle/`
- `visual/`
- `video/`

Logs:

- `.logs/<novel>/`

## Codex Workflows

Workflow docs live in `docs/agents/codex/workflows/`.

Current workflows:

- `update-project-docs.md`
- `create-image-prompts-from-opening-chapters.md`
- `optimize-background-video.md`

## Main Modules

### CLI

- `novel_tts/cli/main.py`

Dispatch only. Do not add business logic here unless argument parsing or orchestration requires it.

Current top-level command families include:

- `crawl`, `translate`, `queue`
- `background`
- `tts`, `create-menu`, `visual`, `video`
- `upload`, `youtube`
- `pipeline`, `quota-supervisor`, `ai-key`

### Crawl

- `novel_tts/crawl/service.py`
- `novel_tts/crawl/strategies.py`
- `novel_tts/crawl/challenge.py`
- `novel_tts/crawl/base.py`
- `novel_tts/crawl/registry.py`
- `novel_tts/crawl/resolvers/*.py`

Public entrypoints:

- `crawl_range`
- `verify_crawled_content`
- `repair_crawled_content`

Important facts:

- source-specific parsing lives in resolvers
- fetch strategy chain can use HTTP, browser-bootstrap HTTP, and Playwright browser fallback
- challenge detection is HTML/title token based
- failures persisted in `crawl_failures.json`
- verify scans saved origin files, not remote source

Resolver extension path:

1. implement resolver
2. register in registry
3. add source config

### Translate

- `novel_tts/translate/novel.py`
- `novel_tts/translate/providers.py`
- `novel_tts/translate/glossary.py`
- `novel_tts/translate/captions.py`
- `novel_tts/translate/polish.py`
- `novel_tts/translate/repair.py`

Public entrypoints:

- `translate_novel`
- `translate_captions`
- `polish_translations`
- `translate_chapter`
- `rebuild_translated_file`

Important facts:

- chapter splitting depends on `config.translation.chapter_regex`
- translation is chapter-granular even when crawl output is batch-granular
- `.parts` is the real completion state
- `translated/*.txt` is rebuilt from `.parts`
- translation does multiple cleanup/repair passes for residual Han
- glossary can auto-update from translated chapters
- `translate/repair.py` contains the scan logic used by `queue repair` to re-enqueue broken chapters

Provider support:

- `gemini_http`
- `openai_chat`

### Queue

- `novel_tts/queue/translation_queue.py`

Public entrypoints:

- `run_supervisor`
- `run_worker`
- `run_status_monitor`
- `launch_queue_stack`
- `requeue_untranslated_exhausted_jobs`

Important facts:

- queue job = one chapter in one source batch
- queue also supports a special captions job id: `captions` (runs `translate captions`)
- job id format: `<file_name>::<chapter_num>`
- workers spawn `python -m novel_tts translate chapter ...`
- Redis stores queue bookkeeping only
- disk files remain source of truth
- queue workers enable central quota for their translate subprocesses (`NOVEL_TTS_CENTRAL_QUOTA=1`, `GEMINI_REDIS_*`)
- queue add supports `--repair-report` in addition to direct chapter/range selection
- `queue repair` scans translated outputs and re-enqueues only suspicious chapters
- queue has a maintenance path to requeue exhausted-but-still-untranslated jobs

Operator UX:

- `uv run novel-tts queue ps-all` prints a pm2-like table grouped by novel.
- The column header `TARGET (N)` shows the number of **unique** chapter targets currently being processed (deduped across worker + translate-chapter subprocess rows).
- `uv run novel-tts queue reset-key <novel_id> --key kN [...]` clears per-key Redis cooldown/quota/throttle state when a key gets stuck.

Redis key suffixes:

- `pending`
- `queued`
- `inflight`
- `retries`
- `done`

### Central Quota (v2)

- `novel_tts/quota/client.py`
- `novel_tts/quota/supervisor.py`

What it is:

- a Redis-backed gate to coordinate RPM/TPM/RPD across worker processes (prevents per-process “sleep storms”)
- `quota-supervisor` is a **global** process that grants requests and publishes short-lived ETA hints used by `queue ps-all`

Operator command:

- `uv run novel-tts quota-supervisor` (run once globally; uses `configs/app.yaml` queue.redis.*)

### TTS / Menu / Media / Upload

- `novel_tts/tts/service.py`
- `novel_tts/media/service.py`
- `novel_tts/upload/service.py`

Public entrypoints worth knowing:

- `run_tts`
- `create_menu`
- `regenerate_menu`
- `generate_visual`
- `create_video`
- `run_upload`
- `run_uploads`

Important facts:

- `create-menu` can rebuild chapter menu text without re-running TTS
- `tts --re-generate-menu` preserves existing timestamps and refreshes labels from current translated text
- upload includes both publish commands and YouTube admin/quota utilities from the same service module

### AI Key Telemetry

- `novel_tts/ai_key/service.py`

Public entrypoint:

- `ai_key_ps`

Important facts:

- reads `.secrets/gemini-keys.txt` but never prints raw keys
- reads Redis cfg from `configs/app.yaml` (`queue.redis.*`)
- scans per-key/per-model 1-minute counters emitted by queue processes

## Critical Invariants

1. `origin/*.txt` must match `translation.chapter_regex`.
2. `.parts` is canonical per-chapter translation state.
3. `translated/*.txt` is derived and rebuildable.
4. TTS assumes translated headings start with `Chương`.
5. Media assumes exact range filenames exist.

If changing file naming or heading format, audit all downstream stages.

## Common Change Patterns

### Add/modify crawl behavior

Usually touch:

- resolver
- `crawl/service.py`
- maybe source config

Be careful with:

- batch filename semantics
- failure manifest semantics
- anti-bot fallback logic

### Add translation cleanup

Usually touch:

- `translate/novel.py`
- maybe `translate/polish.py`

Prefer:

- preserve resumability
- preserve placeholder restore flow
- avoid breaking `.parts` rebuild contract

### Change queue behavior

Usually touch:

- `queue/translation_queue.py`

Be careful with:

- Redis key compatibility
- retry semantics
- subprocess command shape

### Change TTS or media

Usually touch:

- `tts/service.py`
- `tts/providers.py`
- `media/service.py`

Be careful with:

- expected file locations
- ffmpeg assumptions
- menu generation format

## External Dependencies

Required/important:

- `requests`
- `beautifulsoup4`
- `playwright`
- `redis`
- `gradio_client`
- `openai`
- `ffmpeg`
- `ffprobe`

Operational dependencies:

- reachable Redis for queue mode
- Gemini/OpenAI API keys for translation providers
- reachable Gradio TTS server for current TTS path
- Chrome/remote debugging setup for some crawl modes

## Fast Mental Model

Think of the system as:

- config assembly
- crawl writes `origin`
- translate writes `.parts`, rebuilds `translated`
- TTS reads `translated`, writes `audio`
- media reads `audio` + background, writes `visual` and `video`
- queue is just a distributed wrapper around `translate chapter`

## Debug Order

When fixing bugs, inspect nearest persisted artifact first:

- crawl: `origin/`, `.progress/crawl_failures.json`, `debug/img/`
- translate: `.parts/`, `.progress/*.json`, glossary file
- queue: Redis counts + logs + missing `.parts`
- TTS: translated range file + wav chunks
- media: background asset + visual mp4 + ffmpeg

This repo is integration-heavy and lightly abstracted.
Prefer reading files and artifact paths before changing logic.
