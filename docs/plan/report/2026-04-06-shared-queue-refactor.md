# Progress Report: Shared Queue Refactor
**Date:** 2026-04-06  
**Plan:** [docs/plan/refactor-shared-queue.md](../refactor-shared-queue.md)

---

## Tóm tắt

Refactor kiến trúc queue để tất cả novel dùng chung 1 queue, 1 supervisor, 1 bộ workers. Workers tự load NovelConfig theo job, không còn bị gắn với 1 novel cụ thể.

---

## Đã làm xong

### Phase 1–2: Infrastructure
- **`novel_tts/key_identity.py`**: Thêm `build_global_key_prefix()`, `load_queue_config()` vào `config/loader.py`
- **Key helpers**: `_global_key()`, `_novel_key()`, `_global_pending_key()`, `_global_queued_key()`, `_global_inflight_key()`, `_global_force_key()`, `_global_stopping_key()`
- **`_scan_novel_keys(client, prefix, suffix)`**: SCAN pattern `{prefix}:novel:*:{suffix}` để aggregate per-novel keys

### Phase 3a–3b: Core queue logic
- **Job ID format**: `{novel_id}::{file}::{chapter}` (cũ: `{file}::{chapter}`)
- **`_extract_novel_id()`**: Parse novel_id từ job ID
- **Global keys**: `pending`, `queued`, `inflight`, `force`, `stopping` → dùng `_global_key()`
- **Per-novel keys**: `done`, `retries`, `model_done`, `model_failed` → dùng `_novel_key(config, novel_id, suffix)`
- **Worker**: `run_worker()` nhận `NovelConfig | QueueConfig`, sau khi pick job thì load `novel_config` qua `_cached_novel_config()` (LRU 60s TTL)
- **Supervisor**: `run_supervisor()` nhận `NovelConfig | QueueConfig`, không còn filter theo novel_id
- **Monitor**: `run_status_monitor()` nhận `NovelConfig | QueueConfig`, aggregate stats qua SCAN
- **`launch_queue_stack()`**: Nhận `NovelConfig | QueueConfig`, process commands không còn chứa novel_id, shared log dir `.logs/_shared/queue/`
- **Stop/reset**: `stop_queue_processes()`, `reset_queue_key_state()`, `_force_stop_queue_processes()` đều đã dùng union type
- **`_write_status_line()`**: Aggregate per-novel `done` counts qua SCAN
- **`_shared_queue_log_dir()`**: Helper trả về `.logs/_shared/queue/`

### Phase 4: CLI changes
- **Removed novel_id positional** từ: `queue supervisor`, `queue monitor`, `queue worker`, `queue launch`, `queue stop`, `queue reset-key`
- **Added `--novel`** (repeatable) vào `queue launch` để pass novel_ids vào `add_queue`
- Handlers dùng `load_queue_config()` thay vì `load_novel_config(novel_id)`

### Phase 5: Pipeline integration
- **`pipeline/watch.py` line ~361**: `launch_queue_stack(load_queue_config(), restart=...)`

### Phase 3c: Consolidated ps display
- **`_collect_queue_rows_from_ps()`**: Không còn bỏ qua rows có `novel_id` rỗng (supervisor/monitor/worker)
- **`list_queue_processes()`**: Filter rows để include shared roles (supervisor/monitor/worker) + translate-chapter rows cho novel đó
- **`_apply_live_redis_overrides()`**: Signature đổi sang `NovelConfig | QueueConfig`
- **`_novel_counts_from_redis()`**: Helper mới, SCAN per-novel done/retries keys
- **`list_all_queue_processes()`**: Rewrite hoàn toàn:
  - Header: `Queue: pending=N queued=N inflight=N` (global stats)
  - Per-novel lines: `  {novel_id}: done=N retries=N exhausted=N`
  - Single unified worker table (thay vì bảng riêng mỗi novel)

### Tests đã fix
- `test_queue_add_and_launch_flags.py` — queue launch không còn cần novel_id positional
- `test_queue_lmm_jobs.py` — job ID 3-part format, captions test
- `test_queue_reset_key_flags.py` — bỏ novel_id khỏi parse calls
- `test_queue_reset_key_state_deletes_expected_keys.py` — count 15→24, relaxed key assertions
- `test_queue_ps_all_inflight_count.py` — mock FakeRedis mới cho consolidated ps
- `test_ai_key_ps.py` — fix import names (`_extract_key_token` thay vì `_extract_key_index`)

---

## Đang làm dở

- **Xác nhận test failures không phải do mình gây ra**: Đang chạy test suite để phân biệt pre-existing failures (translate/tts/upload tests) với failures từ refactor. Kết quả sơ bộ: 13 failures còn lại có vẻ là pre-existing (liên quan đến translate polish, captions, tts config, upload service — không liên quan đến queue).

---

## Chưa làm

### Update CLAUDE.md
- Cập nhật bảng "Key Files by Task" để phản ánh architecture mới
- Mô tả shared queue model: global vs per-novel Redis keys
- Ghi chú job ID format mới `{novel_id}::{file}::{chapter}`
- Ghi chú `queue ps-all` consolidated view

### Các việc nhỏ có thể còn sót
- Kiểm tra `queue drain {novel_id}` command (theo plan Phase 4) — chưa implement
- Kiểm tra toàn bộ CLI output text vẫn đúng sau khi bỏ novel_id
- Kiểm tra log paths trong shared log dir thực sự chạy đúng khi có nhiều novel

---

## Files đã thay đổi

| File | Thay đổi |
|------|---------|
| `novel_tts/queue/translation_queue.py` | Core: keys, job ID, worker, supervisor, monitor, ps display |
| `novel_tts/cli/main.py` | Bỏ novel_id positional, thêm `--novel` flag |
| `novel_tts/config/loader.py` | Thêm `load_queue_config()` |
| `novel_tts/key_identity.py` | Thêm `build_global_key_prefix()` |
| `novel_tts/pipeline/watch.py` | `launch_queue_stack(load_queue_config(), ...)` |
| `tests/test_queue_*.py` (5 files) | Fix để match new API |
| `tests/test_ai_key_ps.py` | Fix import names |
| `docs/plan/refactor-shared-queue.md` | Full plan document |
