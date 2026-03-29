#!/bin/zsh

set -euo pipefail

ROOT_DIR="/Users/danielpham/sync-workspace/05_Stories/novel-tts"
LIST_FILE="$ROOT_DIR/input/thai-hu-chi-ton/garbage_fixed.txt"
LOG_DIR="$ROOT_DIR/.logs/thai-hu-chi-ton"
LOG_FILE="$LOG_DIR/garbage_fixed_under_751.log"
export UV_CACHE_DIR="/tmp/uv-cache"

mkdir -p "$LOG_DIR"
mkdir -p "$UV_CACHE_DIR"

{
  echo "== $(date '+%Y-%m-%d %H:%M:%S') start run_garbage_fixed_under_751 =="

  while IFS= read -r batch; do
    [[ -z "$batch" ]] && continue
    [[ "$batch" != chuong_* ]] && continue

    range="${batch#chuong_}"
    start="${range%%-*}"
    end="${range##*-}"

    if [[ "$start" -ge 751 ]]; then
      continue
    fi

    if [[ -f "$LOG_FILE" ]] && rg -Fq "finished $batch" "$LOG_FILE"; then
      echo "== $(date '+%Y-%m-%d %H:%M:%S') skip $batch (already finished) =="
      continue
    fi

    echo
    echo "== $(date '+%Y-%m-%d %H:%M:%S') running $batch =="
    uv run novel-tts pipeline run thai-hu-chi-ton --from-stage tts --to-stage video --force --range "$start-$end" < /dev/null
    echo "== $(date '+%Y-%m-%d %H:%M:%S') finished $batch =="
  done < "$LIST_FILE"

  echo
  echo "== $(date '+%Y-%m-%d %H:%M:%S') completed run_garbage_fixed_under_751 =="
} | tee -a "$LOG_FILE"
