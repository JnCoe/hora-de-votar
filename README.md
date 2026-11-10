# hora-de-votar

Timeline of every vote cast in the 2026 Brazilian presidential runoff (2º turno, 25/10/2026), rebuilt from the logs of every urna published by the TSE.

- `docs/`: the static page and the small JSON files it reads from `docs/data/`.
- `pipeline/`: the code that downloads every urna's log and boletim de urna (BU), extracts the vote timestamps, checks every seção against its own BU and writes `docs/data/*.json`.

## Results

- All 499,248 seções with their own urna were checked against their own BU. The BUs add up to the official comparecimento (128,236,074) in every município.
- 128,234,739 votes have a timestamp and are on the timeline.
- 1,335 votes have no timestamp in any published log: 831 from paper-ballot seções abroad, 454 from 2 urnas whose log the TSE did not publish, and 50 lost to urna failures, each verified individually in `pipeline/reconciliation_notes.json`.

## Reproducing the data

The raw and intermediate data lives in `data/` and is not committed: about 2 GB for a full run. You need [uv](https://docs.astral.sh/uv/).

1. Install the dependencies:

   ```bash
   uv sync
   ```

2. Download the TSE open-data file with the polling place of each seção into `data/official/`:

   ```bash
   mkdir -p data/official
   curl -o data/official/detalhe_votacao_secao_2026.zip \
     https://cdn.tse.jus.br/estatistica/sead/odsele/detalhe_votacao_secao/detalhe_votacao_secao_2026.zip
   ```

   It is only used for the name and address of each polling place. The TSE has updated this file since it was downloaded for the published data (26/10/2026), so a few hundred polling-place names differ. No number changes.

3. Run the pipeline:

   ```bash
   pipeline/run.sh
   ```

`run.sh` runs these steps (each script can also be run on its own from `pipeline/`):

| Script | What it does | Output |
|---|---|---|
| `discover.py` | Lists every seção of the 2º turno | `data/secoes.parquet` |
| `official.py` | Fetches the official comparecimento per município | `data/official/mun_comparecimento.parquet` |
| `fetch_parse.py` | Downloads each seção's log and BU and extracts the vote times | `data/v2/` |
| `reconcile.py` | Checks every seção's log votes against its BU, and fails if any difference is unexplained | `data/v2/reconciliation.parquet`, `docs/data/reconciliation.json` |
| `aggregate.py` | Converts the times to Brasília time and builds the page data | `docs/data/timeline.json`, `docs/data/stats.json` |

The fetch is resumable: re-running `run.sh` skips seções already done and retries the ones that failed. It runs `SHARDS` processes in parallel (default 3), each with `CONCURRENCY` requests in flight (default 16). The full fetch took about 7 hours. Progress is logged to `data/v2/logs/`.

To try the pipeline on part of the data first, write to a separate run directory with `HDV_RUN`:

```bash
cd pipeline
uv run python sample.py                     # -> data/sample.parquet
HDV_RUN=test uv run python fetch_parse.py --sample ../data/sample.parquet
HDV_RUN=test uv run python reconcile.py --only-fetched
```

## How each seção is reconciled

A vote is counted from each `O voto do eleitor foi computado` line logged on election day. `reconcile.py` then compares, for every seção, those votes with the Presidente comparecimento in its BU. A difference is accepted only through a rule checked on that seção's own files:

- paper ballots, seções not installed, logs not published;
- or a log that stops after `Voto confirmado para [Presidente]` with no `computado` line. That vote is counted only when the BU confirms it.

Anything else needs an individually verified note in `pipeline/reconciliation_notes.json` whose numbers match the data exactly. If any seção is left unexplained, the script exits with an error.

## Viewing the page

```bash
python -m http.server -d docs
```

Then open http://localhost:8000. When served locally the page reads `docs/data/`; when published, it reads the same files from this repository on GitHub.
