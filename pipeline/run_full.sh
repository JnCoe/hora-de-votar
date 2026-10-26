#!/usr/bin/env bash
# Full pipeline: fetch every seção's log, validate, build docs/data/*.json.
# Safe to re-run: fetch_parse skips seções already done and retries errors.
set -euo pipefail
cd "$(dirname "$0")"

CONCURRENCY="${CONCURRENCY:-48}"

[ -f ../data/secoes.parquet ] || uv run python discover.py
[ -f ../data/official/mun_comparecimento.parquet ] || uv run python official.py

uv run python fetch_parse.py --concurrency "$CONCURRENCY"
# second pass picks up seções that failed with network errors
uv run python fetch_parse.py --concurrency 16

uv run python validate.py | tee ../data/validation_full.txt
uv run python aggregate.py
