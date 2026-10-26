"""List every seção of the 2º turno from the TSE arquivo-urna config files.

Output: data/secoes.parquet with one row per seção. Aggregated seções appear
both in their principal's "nsa" list and as their own row with "principal"
set; they have no urna (and no log) of their own.
"""

import json

import httpx
import polars as pl

from config import HEADERS, PLEITO, SECOES, UFS, URNA_BASE


def fetch_uf(client: httpx.Client, uf: str) -> list[dict]:
    url = f"{URNA_BASE}/config/{uf}/{uf}-p00{PLEITO}-cs.json"
    r = client.get(url)
    r.raise_for_status()
    rows = []
    for abr in json.loads(r.content)["abr"]:
        for mu in abr["mu"]:
            for zo in mu["zon"]:
                for se in zo["sec"]:
                    rows.append({
                        "uf": uf,
                        "mun": mu["cd"],
                        "mun_nome": mu["nm"],
                        "zona": zo["cd"],
                        "secao": se["ns"],
                        "agregadas": se.get("nsa", []),
                        # aggregated seção: votes in its principal seção's urna
                        "principal": se.get("nsp"),
                    })
    return rows


def main() -> None:
    rows = []
    with httpx.Client(headers=HEADERS, timeout=60, http2=True) as client:
        for uf in UFS:
            uf_rows = fetch_uf(client, uf)
            print(f"{uf}: {len(uf_rows):>6} seções")
            rows += uf_rows
    df = (pl.DataFrame(rows, schema_overrides={"agregadas": pl.List(pl.String),
                                             "principal": pl.String})
          .with_row_index("secao_id")
          .with_columns(pl.col("secao_id").cast(pl.UInt32)))
    SECOES.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(SECOES)
    print(f"total: {df.height} seções, {df['principal'].is_not_null().sum()} agregadas -> {SECOES}")


if __name__ == "__main__":
    main()
