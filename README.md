# novel-tts

File-first Python CLI pipeline for crawling serialized web novels, translating them to Vietnamese, generating TTS audio, and rendering publishable video assets.

- Architecture: `docs/ARCHITECTURE.md`
- Agent notes (internal): `docs/agents/codex/AGENTS.md`
- Compact coding context map: `docs/agents/context-map.yaml`

## Table of contents

- [What this repo does](#what-this-repo-does)
- [Quick start](#quick-start)
- [Requirements](#requirements)
- [Configuration](#configuration)
- [Storage contract and invariants](#storage-contract-and-invariants)
- [Recommended workflows](#recommended-workflows)
- [Developer Context Workflow](#developer-context-workflow)
- [Command reference](#command-reference)
- [Troubleshooting](#troubleshooting)

## What this repo does

At a high level, `novel-tts` supports this pipeline:

1. Crawl source chapters into `input/<novel_id>/origin/*.txt`
2. Translate chapters into canonical per-chapter outputs under `input/<novel_id>/.parts/`
3. Rebuild merged translated files under `input/<novel_id>/translated/*.txt`
4. Generate TTS audio under `output/<novel_id>/audio/`
5. Generate visual assets under `output/<novel_id>/visual/`
6. Mux final MP4s under `output/<novel_id>/video/`
7. Upload outputs to supported platforms

Important operational note:

- translation policy should normally be queue-first
- direct `translate novel` is mainly for debugging and small one-off runs

## Quick start

```bash
uv sync
uv run novel-tts --help
```

Tip:

- all commands support top-level `--log-file /path/to/file.log`
- narrow coding context with `uv run novel-tts-context <task>`

## Developer Context Workflow

Use this when coding with Claude Code, Codex, or another repo agent.

1. Start with the compact map instead of re-reading the whole repo:

```bash
uv run novel-tts-context --list
uv run novel-tts-context translate
uv run novel-tts-context queue
```

2. Read only the `Read first` files for the task you are changing.
3. Add `Read only if needed` files only after the bug points there.
4. Do not scan large/generated directories: `input/`, `output/`, `image/`, `tmp/`, `.logs/`, `.secrets/`, `.venv/`, `tests/`.

Why this exists:

- keeps stable repo context in one small deterministic artifact
- gives a single obvious narrow-scope path for common tasks
- reduces repeated loading of large files like `novel_tts/cli/main.py`, `novel_tts/translate/novel.py`, and `novel_tts/queue/translation_queue.py`

## Requirements

Base requirements:

- Python `3.10`
- `uv`

Optional / stage-specific requirements:

- crawl:
  - Playwright + Chromium
  - install with `uv run playwright install chromium`
- queue translation:
  - Redis
  - default config in `configs/app.yaml`: host `127.0.0.1`, port `6379`, db `1`, prefix `novel_tts`
- media rendering:
  - `ffmpeg`
  - `ffprobe`
- current TTS path:
  - a reachable Gradio TTS server configured in app/novel config

## Configuration

### Main config files

- novel configs: `configs/novels/*.json`
- source configs: `configs/sources/*.json`
- per-novel glossaries: `configs/glossaries/*.json`
- app defaults: `configs/app.yaml`
- polish replacements:
  - `configs/polish_replacement/common.json`
  - `configs/polish_replacement/<novel_id>.json`
- TTS provider catalogs:
  - `configs/providers/tts_servers.yaml`
  - `configs/providers/tts_models.yaml`

### Secrets and API keys

Gemini:

- queue workers read API keys from `.secrets/gemini-keys.txt`
- use one key per line
- direct translation can still use `GEMINI_API_KEY`
- if `GEMINI_API_KEY` is unset, direct Gemini translation falls back to the first non-empty line in `.secrets/gemini-keys.txt`

OpenAI:

- set `OPENAI_API_KEY`

YouTube:

- OAuth secrets and token live under `.secrets/youtube/`

## Storage contract and invariants

This repo is intentionally file-first: stages communicate through files under `input/<novel_id>/` and `output/<novel_id>/`.

### Translation truth lives on disk

- canonical per-chapter state:
  - `input/<novel_id>/.parts/<batch>/*.txt`
- derived merged outputs:
  - `input/<novel_id>/translated/*.txt`

`translated/*.txt` is rebuildable from `.parts` and should not be treated as the primary source of truth.

### Crawl outputs

- crawled source batches:
  - `input/<novel_id>/origin/*.txt`
- resumable state:
  - `input/<novel_id>/.progress/*`

### Media outputs

- audio:
  - `output/<novel_id>/audio/<range>/*`
- visual:
  - `output/<novel_id>/visual/*`
- final video:
  - `output/<novel_id>/video/*`

### Heading convention

Do not casually change heading formats:

- crawl/origin headings are typically ASCII:
  - `Chuong <n> ...`
- translated/TTS headings are typically Vietnamese:
  - `Chương <n> ...`

Changing headings affects:

- chapter splitting
- translated file rebuild
- TTS chapter detection
- subtitle/menu generation
- downstream media assets

## Recommended workflows

### Recommended queue-first workflow

Use this as the default production workflow.

```bash
# 1) Crawl
uv run novel-tts crawl run <novel_id> --range <start>-<end>

# 2) Verify crawled files (does not recrawl)
uv run novel-tts crawl verify <novel_id> --range <start>-<end>

# 3) Start the global quota supervisor
uv run novel-tts quota-supervisor
# or:
uv run novel-tts quota-supervisor -d

# 4) Launch queue stack for a novel
uv run novel-tts queue launch <novel_id>

# 5) Enqueue chapters for translation
uv run novel-tts queue add <novel_id> --range <start>-<end>
# or:
uv run novel-tts queue add <novel_id> --all

# 6) Monitor progress
uv run novel-tts queue monitor <novel_id>
uv run novel-tts queue ps <novel_id>

# 7) TTS + media
uv run novel-tts tts <novel_id> --range <start>-<end>
uv run novel-tts visual <novel_id> --range <start>-<end>
uv run novel-tts video <novel_id> --range <start>-<end>

# 8) Upload
uv run novel-tts upload <novel_id> --platform youtube --range <start>-<end>
```

Queue-first guidance:

- prefer queue translation for normal work
- avoid direct `translate novel` except for debugging or very small runs
- `quota-supervisor` should be running whenever queue workers need quota progress

### Optional end-to-end pipeline workflow

`pipeline run` is useful for orchestration, but note:

- its translation step launches the queue stack, enqueues the requested chapter range, and waits for queue completion before downstream media
- it does not run caption translation

Examples:

```bash
# End-to-end range
uv run novel-tts pipeline run <novel_id> --range <start>-<end>

# Use existing translated chapters only
uv run novel-tts pipeline run <novel_id> --range <start>-<end> --skip-translate

# Downstream media stage-by-stage across a large range
uv run novel-tts pipeline run <novel_id> --range 1-2000 --mode per-stage

# Process each translated batch end-to-end
uv run novel-tts pipeline run <novel_id> --range 1-2000 --mode per-video

# Watch one or more novels for new remote chapters, then crawl -> queue translate ->
# repair -> polish -> TTS. Visual/video/upload run automatically once a full batch is ready.
uv run novel-tts pipeline watch <novel_id>
uv run novel-tts pipeline watch <novel_a> <novel_b> --interval-seconds 600
uv run novel-tts pipeline watch --all
```

## Command reference

### Crawl

Writes crawled batches to `input/<novel_id>/origin/*.txt` and resumable state under `input/<novel_id>/.progress/`.

Backward compatibility:

- `novel-tts crawl <novel_id> ...` is treated as `novel-tts crawl run <novel_id> ...`

#### `crawl run`

```bash
# Crawl a chapter range
uv run novel-tts crawl run <novel_id> --range 1-10

# Same, but with explicit bounds
uv run novel-tts crawl run <novel_id> --from 1 --to 10

# Override directory URL if needed
uv run novel-tts crawl run <novel_id> --range 1-10 --dir-url 'https://...'
```

#### `crawl verify`

Sanity-checks already-crawled origin files. It does not recrawl.

`--file` is interpreted as a filename under `input/<novel_id>/origin/` and can be repeated.

```bash
uv run novel-tts crawl verify <novel_id> --range 1-10
uv run novel-tts crawl verify <novel_id> --file chuong_1-10.txt
```

#### `crawl repair`

Repairs already-crawled origin batches on disk. It can generate `input/<novel_id>/repair_config.yaml` from crawl research logs, then apply the repair plan back onto the saved `origin/*.txt` files.

```bash
# Generate/update repair_config.yaml from crawl research files
uv run novel-tts crawl repair <novel_id> --generate-repair-config

# Run repair using input/<novel_id>/repair_config.yaml
uv run novel-tts crawl repair <novel_id> --run --range 1-10

# Repair only selected origin files
uv run novel-tts crawl repair <novel_id> --run --file chuong_1-10.txt
```

### Translate

#### Direct translation (`translate novel`) is debug-oriented

Direct translation:

- reads `input/<novel_id>/origin/*.txt`
- writes canonical chapter outputs under `input/<novel_id>/.parts/`
- rebuilds `input/<novel_id>/translated/*.txt`

```bash
# Translate all discovered origin batches
uv run novel-tts translate novel <novel_id>

# Translate one origin batch file
uv run novel-tts translate novel <novel_id> --file chuong_1-10.txt

# Re-translate even if parts already exist
uv run novel-tts translate novel <novel_id> --file chuong_1-10.txt --force
```

#### `translate chapter`

Used by queue workers, and also useful for debugging:

```bash
uv run novel-tts translate chapter <novel_id> --file chuong_1-10.txt --chapter 7
```

#### `translate captions`

Translates captions when they exist under `input/<novel_id>/captions/`:

```bash
uv run novel-tts translate captions <novel_id>
```

#### `translate polish`

Runs a cleanup pass on translated outputs:

```bash
uv run novel-tts translate polish <novel_id> --range 101-500
uv run novel-tts translate polish <novel_id> --file chuong_1-10.txt
```

`translate polish` loads exact-match replacements from:

- `configs/polish_replacement/common.json`
- `configs/polish_replacement/<novel_id>.json`

Novel-specific entries override common keys.

### Queue

Queue translation produces the same on-disk artifacts as direct translation:

- `.parts`
- rebuilt `translated` batch files

`queue launch` reads `.secrets/gemini-keys.txt` and spawns:

- a supervisor
- a status monitor
- workers for configured models/keys

Queue workers use the centralized quota gate (central quota v2) to coordinate rate-limit and quota waits across processes.

#### Quota supervisor (global)

`quota-supervisor` is global, not per-novel.

Run it in a separate terminal whenever you use queue mode. Without it, jobs can remain in `waiting-quota` and appear stalled.

```bash
# Foreground
uv run novel-tts quota-supervisor

# Background daemon
uv run novel-tts quota-supervisor -d

# Stop / restart the background daemon
uv run novel-tts quota-supervisor --stop
uv run novel-tts quota-supervisor --restart
```

#### Queue process control

Process-control commands manage running queue processes.

```bash
# Launch queue stack
uv run novel-tts queue launch <novel_id>
uv run novel-tts queue launch <novel_id> --restart
uv run novel-tts queue launch <novel_id> --add-queue

# Monitor/status
uv run novel-tts queue monitor <novel_id>
uv run novel-tts queue ps <novel_id>
uv run novel-tts queue ps <novel_id> --all
uv run novel-tts queue ps-all
uv run novel-tts queue ps-all --all -f

# Stop queue processes for a novel
uv run novel-tts queue stop <novel_id>
uv run novel-tts queue stop <novel_id> --role supervisor,worker
uv run novel-tts queue stop <novel_id> --pid 1234
```

#### Queue scheduling commands

Scheduling commands enqueue work into Redis. They do not start workers.

If the queue stack is not running, nothing will change on disk.

#### `queue add`

Enqueue chapters for translation. Pass exactly one of `--range`, `--chapters`, `--repair-report`, or `--all`.

Use `--force` to re-translate.

```bash
uv run novel-tts queue add <novel_id> --range 2001-2500
uv run novel-tts queue add <novel_id> --range 2004-2016 --force
uv run novel-tts queue add <novel_id> --chapters 1205,1214
uv run novel-tts queue add <novel_id> --repair-report .logs/<novel_id>/crawl/addition-replacement_chapter_list.txt
uv run novel-tts queue add <novel_id> --all
```

#### `queue reset-key`

Reset per-key Redis state when a key gets stuck in cooldown/quota/throttle state.

```bash
uv run novel-tts queue reset-key <novel_id> --key k5
uv run novel-tts queue reset-key <novel_id> --key k5 --model gemini-3.1-flash-lite-preview
uv run novel-tts queue reset-key <novel_id> --key k5,k6 --model gemma-3-27b-it,gemma-3-12b-it
uv run novel-tts queue reset-key <novel_id> --all
```

#### `queue repair`

Scans a chapter range and enqueues only broken chapters back into the queue with force re-translate behavior.

Typical reasons:

- placeholder tokens like `ZXQ1156QXZ` / `QZX...QXZ`
- residual Han text
- missing or empty parts
- stale parts where origin is newer

If you do not see changes on disk after running this, ensure the queue stack is running and watch progress via `queue monitor`.

```bash
uv run novel-tts queue repair <novel_id> --range 1401-1410
uv run novel-tts queue repair <novel_id> --all
```

#### `queue requeue-untranslated-exhausted`

Re-enqueues jobs that exhausted retries but still do not have valid translated output on disk.

```bash
uv run novel-tts queue requeue-untranslated-exhausted <novel_id>
```

### AI key telemetry

Reads `.secrets/gemini-keys.txt` and inspects Redis metrics emitted by queue workers.

Raw keys are never printed.

```bash
uv run novel-tts ai-key ps
uv run novel-tts ai-key ps -f
uv run novel-tts ai-key ps --filter k1 --filter 1234
uv run novel-tts ai-key ps --filter-raw "$GEMINI_API_KEY"
```

### TTS

Reads `input/<novel_id>/translated/chuong_<start>-<end>.txt` and writes audio assets under `output/<novel_id>/audio/<range>/`.

Behavior notes:

- per-chapter WAVs are written under `output/<novel_id>/audio/<range>/.parts/`
- per-chapter text hashes are cached under `output/<novel_id>/audio/<range>/.parts/.cache/`
- merged MP3 cache metadata is stored at `output/<novel_id>/audio/<range>/.parts/.cache/merged.sha256`
- if all chapter WAVs are cache hits and `output/<novel_id>/audio/<range>/<range>.mp3` already exists, merge is skipped unless `--force` is used
- if translated text changes, the chapter will be re-synthesized even without `--force`

```bash
uv run novel-tts tts <novel_id> --range 1-10
uv run novel-tts tts <novel_id> --range 1-10 --force
uv run novel-tts tts <novel_id> --range 701-800 --tts-server-name onPremise --tts-model-name cpu
uv run novel-tts tts <novel_id> --range 701-800 --re-generate-menu
uv run novel-tts tts <novel_id> --re-generate-menu --all
```

### Create Menu

Builds or refreshes chapter menu files under `output/<novel_id>/subtitle/` from translated chapter headings without re-running TTS.

```bash
uv run novel-tts create-menu <novel_id> --range 1-10
```

### Visual

Generates the visual layer under `output/<novel_id>/visual/`.

Behavior notes:

- final visual outputs are cached via `output/<novel_id>/visual/.cache/<range>.sha256`
- rerender is skipped when the cached inputs still match the existing visual MP4 + thumbnail PNG
- use `--force` to rerender even when the cache matches

```bash
uv run novel-tts visual <novel_id> --range 1-10
uv run novel-tts visual <novel_id> --range 1-10 --force
uv run novel-tts visual <novel_id> --chapter 1
```

### Video

Writes final MP4s under `output/<novel_id>/video/`.

Behavior notes:

- final muxed videos are cached via `output/<novel_id>/video/.cache/<range>.sha256`
- remux is skipped when the cached visual/audio inputs still match the existing final MP4
- use `--force` to remux even when the cache matches

```bash
uv run novel-tts video <novel_id> --range 1-10
uv run novel-tts video <novel_id> --range 1-10 --force
```

### Upload

Uploads rendered videos by range.

Platforms:

- `youtube`: real upload via OAuth local token + YouTube Data API
- `tiktok`: dry-run payload/validation only

YouTube metadata convention:

- title:
  - `output/<novel_id>/title.txt`
- description:
  - `output/<novel_id>/description.txt`
  - plus `output/<novel_id>/subtitle/chuong_<start>-<end>_menu.txt`
- thumbnail:
  - `output/<novel_id>/visual/chuong_<start>-<end>.png`
- playlist:
  - `output/<novel_id>/playlist.txt`

Default audience/visibility:

- not made for kids
- public

```bash
uv run novel-tts upload <novel_id> --platform youtube --range 1-10
uv run novel-tts upload <novel_id> --platform youtube --range 1-10 --dry-run
uv run novel-tts upload <novel_id> --platform tiktok --range 1-10
uv run novel-tts upload <novel_id> --platform youtube --remove-duplicated
```

YouTube upload pacing can be tuned with:

- `upload.youtube.upload_batch_size`
- `upload.youtube.upload_batch_sleep_seconds`

This helps smooth bursts even though Google mainly documents daily quota units rather than short-window limits.

#### YouTube OAuth setup

Required files:

- one `client_secrets.json` per account
- one `token.json` per account

Example templates:

- `.secrets/youtube/client_secrets.example.json`
- `.secrets/youtube/token.example.json`

How to get `client_secrets.json`:

1. Open [Google Cloud Console](https://console.cloud.google.com/).
2. Create or select a project.
3. Enable `YouTube Data API v3`.
4. Configure the OAuth consent screen.
5. Create OAuth client credentials as a Desktop app.
6. Download the JSON and save it as `.secrets/youtube/client_secrets.json`.

How to get `token.json`:

1. Ensure `upload.youtube.credentials_path` contains the OAuth client JSON path for each account.
2. Ensure `upload.youtube.token_path` contains the matching token JSON path list in the same order.
3. Run a first upload or dry-run command:
   - `uv run novel-tts upload <novel_id> --platform youtube --range 1-10 --dry-run`
4. Complete the browser login/consent flow once.
5. The CLI will create the matching `token.json` for the account being used.

Multi-account note:

- upload video now auto-selects across all configured YouTube project slots based on remaining daily quota
- the uploader estimates quota for duplicate-check + upload + thumbnail + playlist insert before choosing a slot
- `upload.youtube.project` is still used by some admin/read-only YouTube commands, but upload selection itself is now quota-driven
- `upload.youtube.credentials_path` and `upload.youtube.token_path` are parallel arrays.
- Entry `0` in `credentials_path` is paired with entry `0` in `token_path`, and so on.
- When a YouTube request fails with `quotaExceeded`, upload rotates to the next configured account automatically.
- before upload, the CLI estimates quota for duplicate-check + upload + thumbnail + playlist insert and prefers a configured project slot with enough remaining quota
- if a saved quota session is missing or expired for a slot, upload automatically re-captures that slot's quota session via the attached debug browser before checking usage
- each slot's last known YouTube quota snapshot is also stored in Redis with `last_sync_time`
- if live quota sync fails, upload temporarily falls back to Redis + estimated spend since the last sync
- if the cached snapshot crosses YouTube's daily quota reset boundary while live sync is unavailable, the Redis snapshot resets itself to the new day before estimating further spend

### YouTube admin commands

Inspect accessible YouTube playlists and videos using configured OAuth credentials.

```bash
# Playlists
uv run novel-tts youtube playlist
uv run novel-tts youtube playlist --title-only
uv run novel-tts youtube playlist --id PLxxxxxxxx
uv run novel-tts youtube playlist --id 'https://www.youtube.com/playlist?list=PLxxxxxxxx'

# Playlist update
uv run novel-tts youtube playlist update --id PLxxxxxxxx --title 'New title'
uv run novel-tts youtube playlist update --id PLxxxxxxxx --description 'New description'
uv run novel-tts youtube playlist update --id PLxxxxxxxx --privacy-status private
uv run novel-tts youtube playlist update --id PLxxxxxxxx

# Videos
uv run novel-tts youtube video
uv run novel-tts youtube video --title-only
uv run novel-tts youtube video --id xxxxxxxx

# Video update
uv run novel-tts youtube video update --id xxxxxxxx --title 'New title'
uv run novel-tts youtube video update --id xxxxxxxx --description 'New description'
uv run novel-tts youtube video update --id xxxxxxxx --privacy_status private
uv run novel-tts youtube video update --id xxxxxxxx --made_for_kids true
uv run novel-tts youtube video update --id xxxxxxxx --playlist_position 7
uv run novel-tts youtube video update --id xxxxxxxx

# Quota usage
# 1) Capture browser-derived session secrets
uv run novel-tts youtube quota capture --session-slot 1
uv run novel-tts youtube quota capture --session-slot 2
uv run novel-tts youtube quota capture --session-slot 3
uv run novel-tts youtube quota capture --session-slot 1 --remote-debugging-url http://127.0.0.1:9222
uv run novel-tts youtube quota capture --project-id your-gcp-project-id --session-slot 1

# 2) Call HTTP directly using the saved session secret file
uv run novel-tts youtube quota --session-slot 1
uv run novel-tts youtube quota --session-slot 2
uv run novel-tts youtube quota --session-slot 3
uv run novel-tts youtube quota --session-file .secrets/youtube/quota_session-1.json
uv run novel-tts youtube quota --raw

# 3) Inspect shared Redis quota snapshots
uv run novel-tts youtube quota redis --session-slot 1
uv run novel-tts youtube quota redis --session-slot 2
uv run novel-tts youtube all --redis

# Rewrite playlist links in uploaded video descriptions
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-index
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-index --range 1-150

# Reorder videos inside the configured playlist by episode number in title
uv run novel-tts upload <novel_id> --platform youtube --update-playlist-position

# Remove duplicated videos in the configured playlist, keeping the latest uploaded copy per title
uv run novel-tts upload <novel_id> --platform youtube --remove-duplicated
```

`youtube quota` notes:

- `youtube quota capture` attaches to an existing Chrome/Chromium debug browser via CDP
- it captures the Cloud Console quota request secrets and stores them at `.secrets/youtube/quota_session-<slot>.json`
- `youtube quota` then calls HTTP directly using the saved secret file
- `youtube quota redis --session-slot N` reads the shared Redis snapshot for one slot
- `youtube all --redis` reads Redis snapshots for all configured slots
- it prints normalized summary fields including `current_usage`, `effective_limit`, and `remaining`
- if the saved session expires, rerun `youtube quota capture`
- if the attached browser is not logged into Google Cloud Console, sign in there first and retry
- YouTube daily quota reset is treated as `midnight Pacific Time (PT)`, matching the YouTube docs

How to get the required secrets safely:

1. Start Chrome or Chromium with remote debugging enabled and log into Google Cloud Console in that browser profile.
2. Run `uv run novel-tts youtube quota capture --session-slot <1|2|3>`.
3. If needed, override the inferred project id with `--project-id <your-project-id>`.
4. The command saves the required request headers/body into `.secrets/youtube/quota_session-<slot>.json`.
5. Future quota reads use `uv run novel-tts youtube quota --session-slot <1|2|3>` without needing browser interaction until the session expires.

### Pipeline

Runs multiple stages in order for a given range, with optional `--skip-*` flags for iteration.

By default, pipeline runs upload at the end using `upload.default_platform` (default `youtube`).

Use:

- `--skip-upload` to disable upload
- `--upload-platform` to override platform
- `--from-stage` / `--to-stage` to run only a contiguous slice of stages
- `--force` to force supported stages to rerun existing work

```bash
uv run novel-tts pipeline run <novel_id> --range 1-10
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-translate
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-upload
uv run novel-tts pipeline run <novel_id> --range 1-10 --upload-platform tiktok
uv run novel-tts pipeline run <novel_id> --range 1-10 --skip-crawl --skip-translate --skip-tts
uv run novel-tts pipeline run <novel_id> --range 1-10 --from-stage tts
uv run novel-tts pipeline run <novel_id> --range 1-10 --from-stage translate --to-stage video
uv run novel-tts pipeline run <novel_id> --range 1-10 --force
```

`pipeline watch` is continuous orchestration for ongoing serialized novels:

- checks the remote source for newly published chapters
- crawls only chapters newer than the current highest local crawled chapter
- launches queue-first translation, waits for completion, then runs repair and polish
- reruns TTS on the affected batch range (for example `1251-1260` when chapter `1253` arrives)
- only runs `visual`, `video`, and `upload` once the audio batch has all chapter parts for that range

Notes:

- for safety, a novel with no local crawled chapters is skipped unless you pass `--bootstrap-from`
- upload completion is remembered in `input/<novel_id>/.progress/watch_pipeline_state.json`
- default watch settings come from `configs/app.yaml > pipeline.watch` and can still be overridden by CLI flags
- `--all` uses `configs/app.yaml > pipeline.watch.novels` when that list is non-empty; otherwise it falls back to all files under `configs/novels/*.json`
- `--from-stage` / `--to-stage` let you run only a contiguous slice of watch stages

```bash
uv run novel-tts pipeline watch <novel_id>
uv run novel-tts pipeline watch <novel_id> --once
uv run novel-tts pipeline watch <novel_id> --interval-seconds 900
uv run novel-tts pipeline watch --all
uv run novel-tts pipeline watch <novel_id> --bootstrap-from 1201
uv run novel-tts pipeline watch <novel_id> --skip-upload
uv run novel-tts pipeline watch <novel_id> --skip-crawl --skip-translate --skip-repair --skip-polish
uv run novel-tts pipeline watch --all --to-stage polish
uv run novel-tts pipeline watch <novel_id> --from-stage tts --to-stage upload
```

## Troubleshooting

### Crawl issues

- if a source blocks plain HTTP, install Playwright runtime:
  - `uv run playwright install chromium`

### Queue issues

- if `waiting-quota` never progresses, ensure the global quota supervisor is running:
  - `uv run novel-tts quota-supervisor`
  - or `uv run novel-tts quota-supervisor -d`
- if a specific key gets stuck in cooldown/quota state:
  - `uv run novel-tts queue reset-key <novel_id> --key kN [--model ...]`
- if translated outputs look poisoned:
  - `uv run novel-tts queue repair <novel_id> --range <start>-<end>`

### Built-in help

When in doubt, rely on CLI help:

```bash
uv run novel-tts --help
uv run novel-tts queue --help
uv run novel-tts translate --help
```
