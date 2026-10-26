#!/usr/bin/env bash
# Full v2 run: logs + boletins for every seção into data/v2, then per-seção reconciliation.
# Runs SHARDS fetcher processes in parallel, each with CONCURRENCY requests in flight
# (default 3 x 16 = 48 in total). Safe to re-run: done seções are skipped, errors retried.
# Per-process logs: data/v2/logs/fetch-*.log (readable) and events-*.jsonl (one line per seção).
set -uo pipefail
cd "$(dirname "$0")"
export HDV_RUN="${HDV_RUN:-v2}" TQDM_MININTERVAL=60
SHARDS="${SHARDS:-3}"
CONCURRENCY="${CONCURRENCY:-16}"
LOGS="../data/$HDV_RUN/logs"
mkdir -p "$LOGS"

run_shards() {  # $1 = concurrency per shard, $2 = label
  local pids=()
  for ((k = 0; k < SHARDS; k++)); do
    uv run python fetch_parse.py --shard "$k/$SHARDS" --concurrency "$1" \
      > "$LOGS/stdout-$2-s$k.txt" 2>&1 &
    pids+=($!)
  done
  local failed=0
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  echo "$(date '+%F %T') $2 pass finished (failed=$failed)"
}

echo "$(date '+%F %T') start: $SHARDS shards x $CONCURRENCY concurrency"
run_shards "$CONCURRENCY" main
# second, gentler pass picks up seções that failed with network errors
run_shards 4 retry
uv run python reconcile.py | tee "../data/$HDV_RUN/reconciliation.txt"
echo "$(date '+%F %T') done"
