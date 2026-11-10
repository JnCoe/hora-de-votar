#!/usr/bin/env bash
# The whole pipeline: list seções, fetch every seção's log and boletim into data/$HDV_RUN
# (default data/v2), reconcile each seção with its BU, then build docs/data/*.json.
#
# Runs SHARDS fetcher processes in parallel, each with CONCURRENCY requests in flight
# (default 3 x 16 = 48 in total). Safe to re-run: done seções are skipped, errors retried.
# Per-process logs: data/$HDV_RUN/logs/fetch-*.log (readable) and events-*.jsonl (one line per seção).
set -uo pipefail
cd "$(dirname "$0")"
export HDV_RUN="${HDV_RUN:-v2}" TQDM_MININTERVAL=60
SHARDS="${SHARDS:-3}"
CONCURRENCY="${CONCURRENCY:-16}"
RUN="../data/$HDV_RUN"
LOGS="$RUN/logs"
mkdir -p "$LOGS"

# polling places come from a TSE open-data file that has to be downloaded by hand (see README)
LOCAIS_ZIP=../data/official/detalhe_votacao_secao_2026.zip
if [ ! -f "$LOCAIS_ZIP" ] && [ ! -f ../data/official/locais.parquet ]; then
  echo "missing $LOCAIS_ZIP (see README)" >&2
  exit 1
fi

[ -f ../data/secoes.parquet ] || uv run python discover.py || exit 1
[ -f ../data/official/mun_comparecimento.parquet ] || uv run python official.py || exit 1

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

echo "$(date '+%F %T') start: $SHARDS shards x $CONCURRENCY concurrency -> $RUN"
run_shards "$CONCURRENCY" main
# second, gentler pass picks up seções that failed with network errors
run_shards 4 retry

# reconcile.py exits with an error while any seção is unexplained; the page is only rebuilt after it passes
uv run python reconcile.py | tee "$RUN/reconciliation.txt" || exit 1
uv run python aggregate.py || exit 1
echo "$(date '+%F %T') done"
