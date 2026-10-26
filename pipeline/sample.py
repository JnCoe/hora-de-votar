"""Pick the pilot sample: whole UFs (AC, RR, DF) plus whole random municípios
elsewhere, so every sampled município can be checked against official totals."""

import polars as pl

from config import DATA, SECOES

FULL_UFS = ["ac", "rr", "df"]
MUNS_PER_UF = 2
MAX_SECOES_PER_MUN = 600
ZZ_CITIES = 10
SEED = 2026


def main() -> None:
    secoes = pl.read_parquet(SECOES)
    muns = secoes.group_by("uf", "mun").agg(pl.len().alias("n"))
    picked = (muns.filter(~pl.col("uf").is_in(FULL_UFS + ["zz"]),
                          pl.col("n") <= MAX_SECOES_PER_MUN)
              .sample(fraction=1.0, shuffle=True, seed=SEED)
              .group_by("uf", maintain_order=True).head(MUNS_PER_UF))
    zz = muns.filter(pl.col("uf") == "zz").sample(ZZ_CITIES, seed=SEED)
    sample = pl.concat([
        secoes.filter(pl.col("uf").is_in(FULL_UFS)),
        secoes.join(pl.concat([picked, zz]).select("uf", "mun"), on=["uf", "mun"]),
    ])
    sample.write_parquet(DATA / "sample.parquet")
    print(sample.group_by("uf").agg(pl.len(), pl.col("mun_nome").unique().head(3)).sort("uf"))
    print(f"{sample.height} seções in pilot sample")


if __name__ == "__main__":
    main()
