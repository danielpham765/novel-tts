# Refactor: Shared Queue Translation Architecture

## Context

Hiện tại mỗi novel chạy riêng 1 bộ supervisor + monitor + workers, với Redis keys namespace riêng (`{prefix}:{novel_id}:{suffix}`). Điều này gây trùng lặp processes, khó quản lý khi chạy nhiều novel đồng thời, và `ps-all` hiện nhiều tables khó đọc.

**Mục tiêu**: Tất cả novels dùng chung 1 queue, 1 supervisor, 1 bộ workers. Workers pick job từ queue chung, tự load config của novel tương ứng để translate.

---

## 1. Job ID Format Change

**Hiện tại**: `{file_name}::{chapter:04d}` (ví dụ: `chapters_0001-0100.txt::0042`)

**Mới**: `{novel_id}::{file_name}::{chapter:04d}` (ví dụ: `vo-cuc-thien-ton::chapters_0001-0100.txt::0042`)

Special jobs:
- Captions: `{novel_id}::captions`
- Repair glossary: `{novel_id}::repair-glossary::{chunk:04d}`

**Functions cần sửa** (trong `translation_queue.py`):
- `_job_id()` → thêm param `novel_id`
- `_parse_job_id()` → return `(novel_id, file_name, chapter_num)`
- Thêm `_extract_novel_id(job_id) -> str`
- `_is_captions_job()`, `_is_repair_glossary_job()`, `_repair_glossary_job_id()`, `_parse_repair_glossary_chunk_index()` → handle novel_id prefix

---

## 2. Redis Key Restructuring

### Global (shared) keys — drive worker pool:
```
{prefix}:pending              # LIST — shared pending queue
{prefix}:pending_priority     # LIST — priority requeue
{prefix}:pending_delayed      # ZSET — delayed jobs (all novels)
{prefix}:queued               # SET  — dedup guard (all job IDs)
{prefix}:inflight             # HASH — job_id -> JSON {novel_id, worker, model, ...}
{prefix}:force                # HASH — force-translate markers
{prefix}:stopping             # global stop signal
{prefix}:startup_ramp_applied:{model}  # per-model
```

### Per-novel tracking keys:
```
{prefix}:novel:{novel_id}:done          # HASH — completed jobs
{prefix}:novel:{novel_id}:retries       # HASH — retry counts
{prefix}:novel:{novel_id}:model_done    # HASH — per-model successes
{prefix}:novel:{novel_id}:model_failed  # HASH — per-model failures
{prefix}:novel:{novel_id}:pending_count # STRING (INT) — atomic counter
```

### Per-key keys (drop novel_id, keys are shared):
```
{prefix}:key:{key_token}:{model}:rate_limit_cooldown
{prefix}:key:{key_token}:{model}:out_of_quota_cooldown
{prefix}:key:{key_token}:last_pick_ms
```

### IP ban keys (global, not per-novel):
```
{prefix}:ip_ban_429:{model}
{prefix}:ip_ban_state:{model}
{prefix}:ip_ban_probe_lock:{model}
{prefix}:ip_recover_state:{model}
{prefix}:ip_recover_slot:{model}:{slot}
```

### Key helpers thay đổi:
- `_key(config, suffix)` → split thành `_global_key(prefix, suffix)` và `_novel_key(prefix, novel_id, suffix)`
- `_worker_key_prefix()` → drop `novel_id` khỏi key prefix, dùng `build_global_key_prefix(prefix, raw_key)`
- `_client(config)` → `_client_from_redis(redis_cfg: RedisConfig)`

### Per-novel pending count:
- `add_jobs_to_queue()` khi push job → `INCR {prefix}:novel:{novel_id}:pending_count`
- `_throttled_pick_job_id()` khi pop job → extract novel_id → `DECR {prefix}:novel:{novel_id}:pending_count`
- Lua script `_PICK_THROTTLE_POP_LUA` cần extend: sau LPOP, parse novel_id từ job_id, DECR counter tương ứng
- Counter này phục vụ `ps-all` hiện pending per-novel mà không cần scan list

---

## 3. Config Changes

### 3a. Tách queue config ra khỏi NovelConfig

`QueueConfig` giữ nguyên trong `NovelConfig` nhưng chỉ để load Redis connection info và per-novel translation settings (repair_model, glossary_model, chunk_max_len qua model_configs).

Thêm function mới trong `novel_tts/config/loader.py`:

```python
def load_queue_config() -> QueueConfig:
    """Load shared queue config from configs/app.yaml (queue section + models section)."""
```

Supervisor, monitor, workers sẽ gọi `load_queue_config()` thay vì `load_novel_config()`.

### 3b. Worker cần load NovelConfig per job

```python
_novel_config_cache: dict[str, tuple[float, NovelConfig]] = {}
_CACHE_TTL = 60.0

def _cached_novel_config(novel_id: str) -> NovelConfig:
    now = time.time()
    entry = _novel_config_cache.get(novel_id)
    if entry and (now - entry[0]) < _CACHE_TTL:
        return entry[1]
    config = load_novel_config(novel_id)
    _novel_config_cache[novel_id] = (now, config)
    return config
```

---

## 4. Supervisor/Worker Architecture Changes

### 4a. `run_supervisor(queue_config: QueueConfig)` (line 3529)
- Không nhận `NovelConfig` nữa, nhận `QueueConfig`
- `_ensure_worker_processes(queue_config)` spawn workers không gắn novel_id
- `_drain_delayed_jobs()` dùng global delayed ZSET
- `_requeue_stale_inflight()` dùng global inflight hash
- `_count_alive_worker_processes()` pattern `"queue worker"` (không có novel_id)
- `_is_stopping()` check global stopping key

### 4b. `run_worker(queue_config: QueueConfig, key_index, model)` (line 2693)
- Pick job từ shared pending lists
- Extract `novel_id = _extract_novel_id(job_id)`
- Load `novel_config = _cached_novel_config(novel_id)` 
- Translate dùng `novel_config` (paths, glossary, translation settings)
- Mark done/retries dùng per-novel Redis keys
- Rate limit / IP ban dùng global keys

### 4c. `run_status_monitor(queue_config: QueueConfig)` (line 3563)
- Iterate all known novel_ids (scan `{prefix}:novel:*:done` hoặc đọc từ `configs/novels/`)
- Write per-novel status snapshots

### 4d. `launch_queue_stack(queue_config: QueueConfig)` (line 3798)
- Spawn 1 supervisor + 1 monitor (global)
- Không nhận novel_id

### 4e. Worker command & log paths
- Command: `novel-tts queue worker -k {key_index} -m {model}` (bỏ novel_id)
- Log: `.logs/queue/workers/k{N}-{model}-w{M}.log` (shared, không per-novel)
- Supervisor log: `.logs/queue/supervisor.log`
- Monitor log: `.logs/queue/monitor.log`
- Per-novel status vẫn ở: `.logs/{novel_id}/queue/status.state.json`

### 4f. Process matching patterns
- `_matching_worker_pids()`: pattern `"queue worker --key-index {N} --model {M}"` (bỏ novel_id)
- `_count_alive_worker_processes()`: pattern `"queue worker"` (bỏ novel_id)
- `_force_stop_queue_processes()`: patterns không có novel_id

---

## 5. CLI Command Changes

| Hiện tại | Mới | Ghi chú |
|----------|-----|---------|
| `queue supervisor {novel_id}` | `queue supervisor` | Global |
| `queue monitor {novel_id}` | `queue monitor` | Global |
| `queue worker {novel_id} -k N -m M` | `queue worker -k N -m M` | Global |
| `queue launch {novel_id} [--restart]` | `queue launch [--restart]` | Global |
| `queue stop {novel_id} [--force]` | `queue stop [--force]` | Global stop |
| `queue ps {novel_id}` | `queue ps {novel_id}` | **Giữ** — per-novel counts + filter workers đang xử lý novel đó |
| `queue ps-all` | `queue ps-all` | **Output mới** — consolidated view |
| `queue add {novel_id} ...` | `queue add {novel_id} ...` | **Giữ** novel_id |
| `queue remove {novel_id} ...` | `queue remove {novel_id} ...` | **Giữ** novel_id |
| `queue repair {novel_id} ...` | `queue repair {novel_id} ...` | **Giữ** novel_id |
| `queue reset-key {novel_id} ...` | `queue reset-key ...` | Bỏ novel_id (keys are global) |
| `queue requeue-untranslated-exhausted {novel_id}` | `queue requeue-untranslated-exhausted {novel_id}` | **Giữ** novel_id |
| _(mới)_ | `queue drain {novel_id}` | Remove pending jobs của 1 novel khỏi shared queue |

### Thêm command `queue drain {novel_id}`
- Scan shared pending list, remove entries có novel_id prefix match
- Hữu ích khi muốn dừng translate 1 novel mà không stop toàn bộ workers

---

## 6. `ps-all` Output Mới

```
Novels:
  vo-cuc-thien-ton: pending=0 queued=0 inflight=0 retries=0 exhausted=0 done=4536
  tu-tieu-than-loi: pending=120 queued=120 inflight=3 retries=2 exhausted=0 done=1200

PID   | ROLE       | KEY | MODEL              | STATE       | COUNTDOWN | TARGET
12345 | supervisor | -   | -                  | running     |           |
12346 | monitor    | -   | -                  | running     |           |
12347 | worker     | k1  | gemma-3-27b-it     | busy        |           | tu-tieu-than-loi::ch_0001.txt::0042
12348 | worker     | k2  | gemma-3-27b-it     | idle        |           |
12349 | worker     | k3  | gemma-3-27b-it     | waiting-429 | 12s       |
```

- **Heading**: Mỗi novel 1 dòng với counts, dùng `{prefix}:novel:{novel_id}:pending_count`, per-novel done/retries hashes, và filter inflight hash by novel_id
- **Table**: 1 table duy nhất cho tất cả workers, TARGET column hiện `{novel_id}::...` khi worker đang busy

### `queue ps {novel_id}`:
- Chỉ hiện counts của novel đó
- Table chỉ gồm workers đang inflight cho novel đó + idle workers

---

## 7. Split Monolith (`translation_queue.py` → modules)

File hiện tại 4676 dòng. Tách thành:

```
novel_tts/queue/
  __init__.py              # Public API exports (giữ nguyên interface)
  keys.py                  # _global_key(), _novel_key(), _client_from_redis()
  job_id.py                # _job_id(), _parse_job_id(), _extract_novel_id(), special job helpers
  enqueue.py               # add_jobs_to_queue(), add_chapters_to_queue(), add_all_jobs_to_queue(),
                           #   remove_jobs_from_queue(), _enqueue_needed_jobs()
  worker.py                # run_worker(), job execution loop, subprocess spawning
  supervisor.py            # run_supervisor(), _ensure_worker_processes(), launch_queue_stack(),
                           #   stop_queue_processes(), _reap_unwanted_worker_processes()
  monitor.py               # run_status_monitor(), _write_status_line(), _status_paths()
  ps.py                    # list_queue_processes(), list_all_queue_processes(), _render_queue_table(),
                           #   _collect_queue_rows_from_ps(), _classify_queue_rows()
  rate_limit.py            # IP ban, cooldown, recovery, startup ramp logic
  completion.py            # wait_for_range_completion(), _queue_counts_from_redis()
```

---

## 8. Các functions chính cần sửa

### `translation_queue.py` (trước khi split):
| Function | Line | Thay đổi |
|----------|------|----------|
| `_key()` | 1226 | → `_global_key()` + `_novel_key()` |
| `_worker_key_prefix()` | 85 | Drop novel_id |
| `_worker_key_prefix_for_index()` | 93 | Drop novel_id |
| `_pending_priority_key()` | 1230 | Global key |
| `_pending_delayed_key()` | 1234 | Global key |
| `_pending_total_len()` | 1238 | Global keys |
| `_stopping_key()` | ~1247 | Global key |
| `_is_stopping()` | ~1251 | `QueueConfig` thay `NovelConfig` |
| `_throttled_pick_job_id()` | 1317 | Global pending, extend Lua script |
| `_requeue_job_priority()` | ~1402 | Global queued/pending_priority |
| `_delay_job()` | ~1415 | Global delayed/queued |
| `_drain_delayed_jobs()` | ~1436 | Global keys |
| `_job_id()` | ~1606 | Thêm `novel_id` param |
| `_parse_job_id()` | ~1631 | Return `(novel_id, file, chapter)` |
| `_retry_count()` | ~1692 | Per-novel retries hash |
| `add_jobs_to_queue()` | 1734 | Push to global queue, INCR per-novel counter |
| `add_chapters_to_queue()` | 1856 | Push to global queue, INCR per-novel counter |
| `add_all_jobs_to_queue()` | 1928 | Push to global queue, INCR per-novel counter |
| `run_worker()` | 2693 | `QueueConfig`, load NovelConfig per job |
| `run_supervisor()` | 3529 | `QueueConfig`, global scope |
| `run_status_monitor()` | 3563 | `QueueConfig`, iterate all novels |
| `_worker_command()` | 3608 | Bỏ novel_id trong command |
| `_worker_log_path()` | 3599 | Shared log path |
| `_matching_worker_pids()` | 3627 | Pattern không có novel_id |
| `_count_alive_worker_processes()` | 3653 | Pattern không có novel_id |
| `_ensure_worker_processes()` | 3752 | `QueueConfig` |
| `launch_queue_stack()` | 3798 | `QueueConfig`, global |
| `stop_queue_processes()` | 4616 | Global hoặc per-novel drain |
| `_force_stop_queue_processes()` | 4573 | Global patterns |
| `_queue_counts_from_redis()` | 4284 | Global pending/inflight + per-novel done/retries |
| `list_all_queue_processes()` | 4486 | Consolidated view |
| `list_queue_processes()` | 4444 | Per-novel filtered view |
| `wait_for_range_completion()` | ~1794 | Filter inflight by novel_id |

### `novel_tts/config/loader.py`:
- Thêm `load_queue_config() -> QueueConfig` (load từ app.yaml)

### `novel_tts/config/models.py`:
- Không cần thêm dataclass mới — `QueueConfig` đã đủ

### `novel_tts/key_identity.py`:
- Thêm `build_global_key_prefix(prefix, raw_key)` → `{prefix}:{key_token}`

### `novel_tts/cli/main.py`:
- Sửa CLI parser: bỏ `novel_id` khỏi supervisor/monitor/worker/launch/stop/reset-key
- Thêm `queue drain {novel_id}` subcommand
- Handler dùng `load_queue_config()` cho global commands

### `novel_tts/pipeline/watch.py` (line 361):
- `launch_queue_stack()` không nhận NovelConfig nữa → gọi `launch_queue_stack(load_queue_config())`
- `add_jobs_to_queue()` vẫn nhận NovelConfig (để biết source files)

---

## 9. Edge Cases & Risks

### Starvation
Một novel có nhiều pending jobs có thể chiếm hết workers. Chấp nhận FIFO cho v1 — user kiểm soát thứ tự enqueue. Nếu cần fair-share sau này, extend Lua pop script round-robin giữa novels.

### Config loading cost
`load_novel_config()` parse YAML mỗi lần → cache với TTL 60s trong worker process.

### Atomic pending counter
INCR/DECR phải atomic với push/pop. Lua script `_PICK_THROTTLE_POP_LUA` extend: sau LPOP success, parse novel_id prefix từ job_id, `DECR {prefix}:novel:{novel_id}:pending_count`.

### Migration (đổi key format giữa chừng)
Yêu cầu drain toàn bộ queue trước khi deploy code mới. Không giữ backward-compat cho old keys.

### `wait_for_range_completion` correctness
Inflight giờ là global hash. Cần filter entries by novel_id — scan inflight hash, kiểm tra job_id prefix. Inflight thường nhỏ (< 100 items) nên scan OK.

### Per-novel `queue stop` (giờ là `queue drain`)
Scan shared pending list O(n) để remove jobs by novel_id. Với pending list lớn (> 10K), có thể chậm. Giải pháp: dùng Lua script scan + LREM.

---

## 10. Implementation Phases

### Phase 1: Tách module (pure refactor, no behavior change)
- Split `translation_queue.py` thành modules theo Section 7
- Giữ nguyên `__init__.py` exports
- Run tests sau mỗi file extraction

### Phase 2: Job ID format + key helpers
- Implement `_extract_novel_id()`, update `_job_id()` nhận novel_id
- Implement `_global_key()`, `_novel_key()`, `_client_from_redis()`
- Implement `build_global_key_prefix()` trong `key_identity.py`
- Implement `load_queue_config()` trong `config/loader.py`
- Chưa thay đổi behavior — new helpers song song với old ones

### Phase 3: Migrate core queue logic
- Switch pending/queued/inflight/force/stopping sang global keys
- Switch done/retries/model_done/model_failed sang per-novel keys
- Switch rate limit / IP ban keys drop novel_id
- Implement per-novel pending counter + Lua script extension
- Update `run_worker()`, `run_supervisor()`, `run_status_monitor()`

### Phase 4: CLI + ps-all changes
- Update CLI parser: bỏ novel_id khỏi global commands
- Implement `queue drain {novel_id}`
- Implement consolidated `ps-all` output
- Update `queue ps {novel_id}` filtered view

### Phase 5: Pipeline integration + cleanup
- Update `pipeline/watch.py`
- Remove old `_key(config, suffix)` helper
- Remove backward-compat code
- Update CLAUDE.md architecture docs

---

## 11. Verification

1. **Unit tests**: Run `uv run pytest tests/` sau mỗi phase
2. **Manual test**: 
   - `uv run novel-tts queue launch` (không novel_id)
   - `uv run novel-tts queue add {novel_a} --range 1-10`
   - `uv run novel-tts queue add {novel_b} --range 1-10`
   - `uv run novel-tts queue ps-all` → verify consolidated output
   - `uv run novel-tts queue ps {novel_a}` → verify per-novel view
   - Verify workers translate jobs từ cả 2 novels
   - `uv run novel-tts queue drain {novel_a}` → verify chỉ remove novel_a jobs
   - `uv run novel-tts queue stop` → verify global stop

---

## Critical Files

- `novel_tts/queue/translation_queue.py` — main implementation (4676 lines)
- `novel_tts/queue/__init__.py` — public exports
- `novel_tts/config/models.py` — QueueConfig, RedisConfig, QueueModelConfig
- `novel_tts/config/loader.py` — load_novel_config(), cần thêm load_queue_config()
- `novel_tts/cli/main.py` — CLI parser + handlers (lines 376-1606)
- `novel_tts/key_identity.py` — build_key_prefix()
- `novel_tts/pipeline/watch.py` — pipeline integration (line 361)
- `configs/app.yaml` — shared queue + models config
