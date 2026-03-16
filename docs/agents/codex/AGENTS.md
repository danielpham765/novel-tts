# novel-tts Architecture for Codex

## Purpose

`novel-tts` is a file-first CLI pipeline:

1. crawl source novel text
2. translate to Vietnamese
3. optionally queue translation via Redis
4. inspect per-key telemetry (Redis-backed)
5. synthesize audio
6. render visual
7. mux final video

Primary package: `novel_tts`

Primary entrypoints:

- `novel_tts/__main__.py`
- `novel_tts/cli/main.py`

Console script:

- `novel-tts`

## Core Rule

Most stages do not talk directly to each other.
They communicate through files under `input/<novel>` and `output/<novel>`.

If changing behavior, preserve artifact contracts.

## Runtime Config

Loaded by:

- `novel_tts.config.loader.load_novel_config`

Merged from:

- `configs/novels/<novel>.json`
- `configs/sources/<source>.json`
- `configs/app.yaml`
- glossary JSON
- selected env vars

Dataclass root:

- `NovelConfig`

Important sections:

- `storage`
- `crawl`
- `translation`
- `captions`
- `queue`
- `tts`
- `visual`
- `video`

## Storage Contract

Per novel under `input/<novel>/`:

- `origin/*.txt`: crawled source batches
- `translated/*.txt`: rebuilt translated batches
- `caption/*.srt`: caption in/out
- `.parts/<origin_stem>/<chapter>.txt`: per-chapter translation truth
- `.progress/*.json`: resumable state and crawl failures

Per novel under `output/<novel>/`:

- `audio/<range>/`
- `subtitle/`
- `visual/`
- `video/`

Logs:

- `.logs/<novel>/`

## Main Modules

### CLI

- `novel_tts/cli/main.py`

Dispatch only. Do not add business logic here unless argument parsing or orchestration requires it.

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

Important facts:

- queue job = one chapter in one source batch
- job id format: `<file_name>::<chapter_num>`
- workers spawn `python -m novel_tts translate chapter ...`
- Redis stores queue bookkeeping only
- disk files remain source of truth

Operator UX:

- `uv run novel-tts queue ps-all` prints a pm2-like table grouped by novel.
- The column header `TARGET (N)` shows the number of **unique** chapter targets currently being processed (deduped across worker + translate-chapter subprocess rows).
- `uv run novel-tts queue reset <novel_id> --key kN [...]` clears per-key Redis cooldown/quota/throttle state when a key gets stuck.

Redis key suffixes:

- `pending`
- `queued`
- `inflight`
- `retries`
- `done`

### AI Key Telemetry

- `novel_tts/ai_key/service.py`

Public entrypoint:

- `ai_key_ps`

Important facts:

- reads `.secrets/gemini-keys.txt` but never prints raw keys
- reads Redis cfg from `configs/app.yaml` (`queue.redis.*`)
- scans per-key/per-model 1-minute counters emitted by queue processes

### TTS

- `novel_tts/tts/service.py`
- `novel_tts/tts/providers.py`

Public entrypoint:

- `run_tts`

Important facts:

- expects translated batch file exactly matching requested range
- splits by `Chương <n>`
- writes wav chunks then merges mp3 via ffmpeg
- current provider: `gradio_vie_tts`

### Media

- `novel_tts/media/service.py`

Public entrypoints:

- `generate_visual`
- `create_video`

Important facts:

- visual rendering is FFmpeg drawtext over a background video
- final video loops visual to match audio duration

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
