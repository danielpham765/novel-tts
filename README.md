# novel-tts

Generic Python CLI for novel crawling, translation, caption translation, TTS, visual generation, and video rendering.

## Quick start with uv

```bash
uv sync
uv run novel-tts --help
```

## Config

- Novel configs live in `configs/novels/*.json`.
- Source configs live in `configs/sources/*.json`.
- Per-novel glossaries live in `configs/glossaries/*.json`.

Gemini API keys for queue workers live in `.secrets/gemini-keys.txt`, one key per line.
For one-off translation runs, `GEMINI_API_KEY` from env still works.

## Commands

Tip: all commands support `--log-file /path/to/file.log` at the top level.

Translation policy: use the Redis-backed queue for translation for all novels. Avoid running direct `translate novel`
except for debugging/small one-off experiments.

### Crawl (fetch raw source text)

Writes crawled batches to `input/<novel>/origin/*.txt` and resumable state under `input/<novel>/.progress/`.
Backward compatible: `novel-tts crawl <novel_id> ...` is treated as `novel-tts crawl run <novel_id> ...`.

```bash
# Crawl a chapter range
uv run novel-tts crawl run vo-cuc-thien-ton --range 1-10

# Same, but with explicit bounds
uv run novel-tts crawl run vo-cuc-thien-ton --from 1 --to 10

# Optional: override directory URL (useful when source changes)
uv run novel-tts crawl run vo-cuc-thien-ton --range 1-10 --dir-url 'https://...'
```

### Crawl Verify (sanity-check saved origin files)

Verifies already-crawled files (does not recrawl). Useful before translation/TTS.
`--file` can be specified multiple times and is interpreted as a filename under `input/<novel>/origin/`.

```bash
# Verify an entire range
uv run novel-tts crawl verify vo-cuc-thien-ton --range 1-10

# Verify a specific origin batch file (filename under input/<novel>/origin/)
uv run novel-tts crawl verify vo-cuc-thien-ton --file chuong_1-10.txt
```

### Translate Novel (direct, file-first)

Translates `input/<novel>/origin/*.txt` chapter-by-chapter, writing canonical per-chapter outputs under
`input/<novel>/.parts/` and rebuilding `input/<novel>/translated/*.txt`.
`--file` can be specified multiple times and is interpreted as a filename under `input/<novel>/origin/`.

```bash
# Debug-only (normal translation should be done via `novel-tts queue ...`)
#
# Translate all discovered origin batches
uv run novel-tts translate novel vo-cuc-thien-ton

# Translate only one origin batch file (filename under input/<novel>/origin/)
uv run novel-tts translate novel vo-cuc-thien-ton --file chuong_1-10.txt

# Re-translate even if parts already exist (use with care)
uv run novel-tts translate novel vo-cuc-thien-ton --file chuong_1-10.txt --force
```

### Translate Chapter (debug / used by queue workers)

Translates a single chapter number from a specific origin batch file, then rebuilds the translated batch file.

```bash
uv run novel-tts translate chapter vo-cuc-thien-ton --file chuong_1-10.txt --chapter 7
```

### Translate Captions (SRT)

Translates captions under `input/<novel>/caption/` (when they exist).

```bash
uv run novel-tts translate captions vo-cuc-thien-ton
```

### Translate Polish (cleanup pass)

Applies cleanup/polish steps to translated outputs. Can target a file or a chapter range.
`--file` can be specified multiple times and is interpreted as a filename under `input/<novel>/origin/`.

```bash
# Polish a chapter range (e.g. fix headings, spacing, residual Han, etc.)
uv run novel-tts translate polish vo-cuc-thien-ton --range 101-500

# Polish a single origin batch file (filename under input/<novel>/origin/)
uv run novel-tts translate polish vo-cuc-thien-ton --file chuong_1-10.txt
```

### Translate Repair (scan + requeue broken chapters)

Scans a chapter range and enqueues only the broken chapters back into the queue (force re-translate),
then queue workers will rebuild the translated batch file when all chapter parts for that file exist.

This is designed for cases where outputs contain placeholder tokens like `ZXQ1156QXZ`/`QZX...QXZ`, residual Han,
missing/empty parts, or stale parts (origin is newer than the part).

Important: `translate repair` only *enqueues* jobs into Redis. It does not translate anything by itself.
You need a running queue stack (`queue launch`, or manual `queue supervisor` + `queue worker`) for jobs to be processed.

```bash
uv run novel-tts translate repair vo-cuc-thien-ton --range 1401-1410

# If nothing changes on disk after repair, start/monitor the queue:
uv run novel-tts queue launch vo-cuc-thien-ton
uv run novel-tts queue monitor vo-cuc-thien-ton
```

### Queue (distributed translation via Redis)

Used when you want supervisor/worker processes and rate limiting. Requires Redis (defaults in `configs/app.yaml`):
`127.0.0.1:6379`, db `1`, prefix `novel_tts`.

Queue translation writes the same on-disk artifacts as direct translation: chapter parts under `input/<novel>/.parts/`
and rebuilt batch files under `input/<novel>/translated/`.

`queue launch` reads `.secrets/gemini-keys.txt` and spawns a supervisor + workers for the configured models.

```bash
# One-shot: launch the whole queue stack for a novel
uv run novel-tts queue launch vo-cuc-thien-ton

# Restart the stack (stop existing processes for the novel first)
uv run novel-tts queue launch vo-cuc-thien-ton --restart

# Manual: run supervisor and a worker (useful for debugging)
uv run novel-tts queue supervisor vo-cuc-thien-ton
uv run novel-tts queue worker vo-cuc-thien-ton --key-index 1 --model gemma-3-27b-it

# Monitor/status
uv run novel-tts queue ps vo-cuc-thien-ton
uv run novel-tts queue ps vo-cuc-thien-ton --all           # include verbose subprocess roles
uv run novel-tts queue ps-all
uv run novel-tts queue ps-all --all -f                     # watch mode (refresh every 1s; Ctrl+P pause/resume)
uv run novel-tts queue monitor vo-cuc-thien-ton            # periodic status output

# Reset per-key Redis state (cooldown/quota/throttle) when a key gets stuck
uv run novel-tts queue reset vo-cuc-thien-ton --key k5
uv run novel-tts queue reset vo-cuc-thien-ton --key k5 --model gemini-3.1-flash-lite-preview
uv run novel-tts queue reset vo-cuc-thien-ton --key k5,k6 --model gemma-3-27b-it,gemma-3-12b-it

# Enqueue a specific chapter range for translation (by chapter number, can be inside a batch file)
uv run novel-tts queue add vo-cuc-thien-ton --range 2001-2500
uv run novel-tts queue add vo-cuc-thien-ton --range 2004-2016 --force

# Stop queue processes for a novel (keeps Redis state)
uv run novel-tts queue stop vo-cuc-thien-ton
uv run novel-tts queue stop vo-cuc-thien-ton --role supervisor,worker
uv run novel-tts queue stop vo-cuc-thien-ton --pid 1234
```

### AI Key (Gemini key telemetry)

Reads `.secrets/gemini-keys.txt` and inspects Redis metrics emitted by queue workers (rate limit / quota / request counts).
Raw keys are never printed.

```bash
# Snapshot
uv run novel-tts ai-key ps

# Watch mode (refresh every 1s; Ctrl+P pause/resume)
uv run novel-tts ai-key ps -f

# Filter by key index (kN or N) or by last4 of the raw key
uv run novel-tts ai-key ps --filter k1 --filter 1234

# Filter by raw key (exact match). Repeatable or comma-separated.
uv run novel-tts ai-key ps --filter-raw "$GEMINI_API_KEY"
```

### TTS (audio synthesis)

Reads `input/<novel>/translated/chuong_<start>-<end>.txt` and writes audio assets under `output/<novel>/audio/<range>/`.

```bash
uv run novel-tts tts vo-cuc-thien-ton --range 1-10
uv run novel-tts tts vo-cuc-thien-ton --range 1-10 --force   # re-synthesize even if cached
```

### Visual (overlay render)

Generates the visual layer under `output/<novel>/visual/` (typically requires audio for the same range).

```bash
uv run novel-tts visual vo-cuc-thien-ton --range 1-10
```

### Video (mux visual + audio)

Writes final MP4s under `output/<novel>/video/`.

```bash
uv run novel-tts video vo-cuc-thien-ton --range 1-10
```

### Pipeline (end-to-end orchestration)

Runs multiple stages in order for a given range, with optional `--skip-*` flags for iteration.
Note: the pipeline's translation step uses direct `translate novel`. With a queue-only translation policy, run pipeline
with `--skip-translate`, then translate via `novel-tts queue ...`, and resume the remaining stages.

```bash
uv run novel-tts pipeline run vo-cuc-thien-ton --range 1-10
uv run novel-tts pipeline run vo-cuc-thien-ton --range 1-10 --skip-crawl --skip-captions
uv run novel-tts pipeline run vo-cuc-thien-ton --range 1-10 --skip-translate
```

## Notes

- Python target: `3.10`
- Install browser runtime for crawl mode when needed: `uv run playwright install chromium`
- `queue launch` reads `.secrets/gemini-keys.txt` and spawns `1 supervisor + 1 status monitor`, then the supervisor spawns workers based on `configs/app.yaml` (`queue.enabled_models` + `queue.model_configs.<model>.worker_count`).
- Queue pick burst guard: `queue.min_pick_interval_seconds` (default `0.5`) serializes job picks per API key (`--key-index`); set `0` to disable.
