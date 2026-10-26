# hora-de-votar

Timeline of every vote cast in the 2026 Brazilian presidential runoff (2º turno, 25/10/2026), rebuilt from the logs of every urna published by the TSE.

- `docs/`: the static page (served as-is, e.g. by GitHub Pages) and the small JSON files it reads from `docs/data/`.
- `pipeline/`: the code that downloads the urna logs and boletins, extracts vote timestamps, checks them against official turnout and writes `docs/data/*.json`.

## Reproducing the data

The raw and intermediate data (~2 GB) lives in `data/`, which is not committed. To rebuild it you need [uv](https://docs.astral.sh/uv/) and enough disk space.

```bash
uv sync
cd pipeline

# 1. list every seção and fetch official turnout per município
uv run python discover.py      # -> data/secoes.parquet
uv run python official.py      # -> data/official/mun_comparecimento.parquet

# 2. download the TSE open-data file with the polling place of each seção
#    ("Detalhe da apuração por seção eleitoral", 2026) into data/official/:
#    data/official/detalhe_votacao_secao_2026.zip

# 3. fetch every seção's log and boletim, then reconcile them
./run_v2.sh                    # -> data/v2/, docs/data/reconciliation.json

# 4. build the files the page uses
HDV_RUN=v2 uv run python aggregate.py   # -> docs/data/timeline.json, stats.json
```

`run_v2.sh` is resumable: re-running it skips seções already done and retries the ones that failed. Its speed is set with `SHARDS` (parallel processes, default 3) and `CONCURRENCY` (requests in flight per process, default 16). Progress is logged to `data/v2/logs/`.

`reconcile.py` compares, for every seção, the votes found in the log with the Presidente comparecimento in its boletim de urna. A difference is accepted only through a rule checked on that seção's own files, or through a verified note in `pipeline/reconciliation_notes.json`. Anything else is reported as pending, and the script exits with an error.

To try the pipeline on a sample first, see `sample.py` and the `--sample` option of `fetch_parse.py`.

`run_full.sh` is the older single-run version of steps 1–4 (writing to `data/` instead of `data/v2/`).

## Viewing the page

```bash
python -m http.server -d docs
```

Then open http://localhost:8000.
