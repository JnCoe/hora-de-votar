"""Fetch official 2º turno turnout (comparecimento) per município for Presidente.

Output: data/official/mun_comparecimento.parquet (uf, mun, aptos, comparecimento)
"""

import asyncio
import json

import httpx
import polars as pl

from config import BASE, CICLO, ELEICAO, HEADERS, OFFICIAL_DIR, SECOES


def url(uf: str, mun: str) -> str:
    return f"{BASE}/{CICLO}/{ELEICAO}/dados/{uf}/{uf}{mun}-c0001-e00{ELEICAO}-u.json"


async def fetch(client, sem, uf, mun) -> dict:
    async with sem:
        for attempt in range(5):
            try:
                r = await client.get(url(uf, mun))
                if r.status_code == 200:
                    e = json.loads(r.content)["e"]
                    return {"uf": uf, "mun": mun, "aptos": int(e["te"]),
                            "comparecimento": int(e["c"])}
                if r.status_code == 404:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(2 ** attempt)
    return {"uf": uf, "mun": mun, "aptos": None, "comparecimento": None}


async def run(muns: list[tuple[str, str]]) -> list[dict]:
    sem = asyncio.Semaphore(32)
    async with httpx.AsyncClient(headers=HEADERS, timeout=60, http2=True) as client:
        return await asyncio.gather(*(fetch(client, sem, uf, mun) for uf, mun in muns))


def main() -> None:
    muns = pl.read_parquet(SECOES).select("uf", "mun").unique().sort("uf", "mun").rows()
    rows = asyncio.run(run(muns))
    df = pl.DataFrame(rows, schema={"uf": pl.String, "mun": pl.String,
                                    "aptos": pl.Int64, "comparecimento": pl.Int64})
    OFFICIAL_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OFFICIAL_DIR / "mun_comparecimento.parquet")
    print(df.group_by("uf").agg(pl.len(), pl.col("comparecimento").null_count().alias("missing"),
                                pl.col("comparecimento").sum()).sort("uf"))
    print("Brasil + exterior comparecimento:", df["comparecimento"].sum())


if __name__ == "__main__":
    main()
