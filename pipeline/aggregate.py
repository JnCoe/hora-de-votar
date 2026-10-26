"""Turn per-vote timestamps into the small JSON files the page uses.

Times are converted from each urna's local clock to Brasília time (BRT):
  - Brazil: the UTC offset of each município is inferred from when its urnas
    opened (voting opens at 08:00 BRT everywhere), falling back to the UF default.
  - Abroad: the IANA timezone of the city (tz_exterior.py); polls there close
    at 17:00 local, so "after closing" uses local time.

Outputs docs/data/timeline.json and docs/data/stats.json.
"""

import argparse
import io
import json
import zipfile

import polars as pl

from config import (BRT_OFFSET, DOCS_DATA, OFFICIAL_DIR, SECOES, STATUS_DIR,
                    UF_UTC_OFFSET, VOTES_DIR, FALLBACK_DIR, load_status)
from tz_exterior import utc_offset_seconds

H = 3600
OPEN_BRT = 8 * H
CLOSE = 17 * H           # official closing time (BRT in Brazil, local abroad)
SLIDER_START = 15 * H    # everything before is summed into one base value


def load_locais() -> pl.DataFrame:
    """Seção -> local de votação, from the 1º turno per-seção file (same layout)."""
    cache = OFFICIAL_DIR / "locais.parquet"
    if cache.exists():
        return pl.read_parquet(cache)
    frames = []
    with zipfile.ZipFile(OFFICIAL_DIR / "detalhe_votacao_secao_2026.zip") as z:
        for name in z.namelist():
            if not name.endswith(".csv") or name.endswith("_BR.csv"):
                continue
            df = pl.read_csv(io.BytesIO(z.read(name)), separator=";", encoding="latin1",
                             infer_schema=False,
                             columns=["SG_UF", "CD_MUNICIPIO", "NR_ZONA", "NR_SECAO",
                                      "NR_LOCAL_VOTACAO", "NM_LOCAL_VOTACAO",
                                      "DS_LOCAL_VOTACAO_ENDERECO"])
            frames.append(df)
    locais = (pl.concat(frames).select(
        uf=pl.col("SG_UF").str.to_lowercase(),
        mun=pl.col("CD_MUNICIPIO").str.zfill(5),
        zona=pl.col("NR_ZONA").str.zfill(4),
        secao=pl.col("NR_SECAO").str.zfill(4),
        local_id=pl.col("NR_LOCAL_VOTACAO"),
        local=pl.col("NM_LOCAL_VOTACAO"),
        endereco=pl.col("DS_LOCAL_VOTACAO_ENDERECO"),
    ).unique(["uf", "mun", "zona", "secao"]))
    locais.write_parquet(cache)
    return locais


def secao_offsets(secoes: pl.DataFrame, status: pl.DataFrame) -> pl.DataFrame:
    """Seconds to add to each seção's local clock to get BRT, plus close time in BRT."""
    brazil = (secoes.filter(pl.col("uf") != "zz")
              .join(status.select("secao_id", "ready_first"), on="secao_id", how="left"))
    per_mun = (brazil.group_by("uf", "mun")
               .agg(pl.col("ready_first").median().alias("ready_median"))
               .with_columns(
                   inferred=((OPEN_BRT - pl.col("ready_median")) / H).round().cast(pl.Int32),
                   default=(pl.col("uf").replace_strict(UF_UTC_OFFSET, return_dtype=pl.Int32)
                            - BRT_OFFSET).mul(-1)))
    # hours the local clock is *behind* BRT: AC = 2, AM = 1, Noronha = -1
    per_mun = per_mun.with_columns(
        behind=pl.when(pl.col("inferred").is_between(-1, 2))
        .then(pl.col("inferred")).otherwise(pl.col("default")))
    br = brazil.join(per_mun.select("uf", "mun", "behind", "default"), on=["uf", "mun"]).select(
        "secao_id", shift=pl.col("behind") * H, close_brt=pl.lit(CLOSE),
        tz_differs=pl.col("behind") != pl.col("default"))

    zz = secoes.filter(pl.col("uf") == "zz")
    city_shift = {c: BRT_OFFSET * H - utc_offset_seconds(c) for c in zz["mun_nome"].unique()}
    zz = zz.select("secao_id",
                   shift=pl.col("mun_nome").replace_strict(city_shift, return_dtype=pl.Int32))
    zz = zz.with_columns(close_brt=CLOSE + pl.col("shift"), tz_differs=pl.lit(False))
    return pl.concat([br.with_columns(pl.col("shift").cast(pl.Int32),
                                      pl.col("close_brt").cast(pl.Int32)),
                      zz.with_columns(pl.col("close_brt").cast(pl.Int32))])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DOCS_DATA))
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()

    secoes = pl.read_parquet(SECOES).drop("agregadas")
    status = load_status()
    ok = status.filter(pl.col("status") == "ok")
    secoes = secoes.join(ok.select("secao_id"), on="secao_id", how="semi")
    offs = secao_offsets(secoes, ok)
    meta = secoes.join(offs, on="secao_id")

    # computed votes plus the ones inferred by the fallback rule (kept apart on disk)
    sources = [pl.scan_parquet(VOTES_DIR / "*.parquet")]
    if list(FALLBACK_DIR.glob("*.parquet")):
        sources.append(pl.scan_parquet(FALLBACK_DIR / "*.parquet").select("secao_id", "t_local"))
    votes = (pl.concat(sources)
             .join(meta.lazy().select("secao_id", "uf", "shift", "close_brt"), on="secao_id")
             .with_columns(t=pl.col("t_local") + pl.col("shift"))
             .with_columns(late=pl.col("t") >= pl.col("close_brt"),
                           minute=pl.col("t") // 60))

    # --- timeline ---------------------------------------------------------------
    per_min = (votes.group_by("uf", "minute").agg(pl.len().alias("n"))
               .collect(engine="streaming"))
    start_min = SLIDER_START // 60
    end_min = int(per_min["minute"].max())
    minutes = list(range(start_min, end_min + 1))

    def series(df: pl.DataFrame) -> tuple[int, list[int]]:
        base = int(df.filter(pl.col("minute") < start_min)["n"].sum())
        m = dict(df.filter(pl.col("minute") >= start_min).group_by("minute")
                 .agg(pl.col("n").sum()).iter_rows())
        return base, [int(m.get(x, 0)) for x in minutes]

    nat_base, nat = series(per_min)
    by_uf = {}
    for uf in sorted(per_min["uf"].unique()):
        b, s = series(per_min.filter(pl.col("uf") == uf))
        by_uf[uf] = {"base": b, "per_minute": s}
    day = (per_min.group_by("minute").agg(pl.col("n").sum()).sort("minute"))
    timeline = {
        "start_minute": start_min, "end_minute": end_min,
        "base": nat_base, "per_minute": nat, "by_uf": by_uf,
        "day": {"first_minute": int(day["minute"].min()),
                "per_minute": dict(zip(map(str, day["minute"]), map(int, day["n"])))},
    }

    # --- stats ------------------------------------------------------------------
    per_sec = (votes.group_by("secao_id")
               .agg(pl.len().alias("votes"), pl.col("late").sum().alias("late"),
                    pl.col("t").max().alias("last_t"))
               .collect(engine="streaming")
               .join(meta, on="secao_id")
               .with_columns(last_after_close=pl.col("last_t") - pl.col("close_brt")))
    locais = load_locais()
    per_sec = per_sec.join(locais, on=["uf", "mun", "zona", "secao"], how="left")

    official = pl.read_parquet(OFFICIAL_DIR / "mun_comparecimento.parquet")
    fetched_muns = per_sec.select("uf", "mun").unique()
    total = int(per_sec["votes"].sum())
    late = int(per_sec["late"].sum())

    def top(df: pl.DataFrame, keys: list[str]) -> list[dict]:
        return (df.group_by(keys)
                .agg(pl.col("votes").sum(), pl.col("late").sum(),
                     (pl.col("late") > 0).sum().alias("secoes_late"),
                     pl.len().alias("secoes"), pl.col("last_t").max())
                .with_columns(pct_late=pl.col("late") / pl.col("votes"))
                .sort("late", descending=True).head(args.top).to_dicts())

    uf_tab = (per_sec.group_by("uf")
              .agg(pl.col("votes").sum(), pl.col("late").sum(),
                   (pl.col("late") > 0).sum().alias("secoes_late"), pl.len().alias("secoes"),
                   pl.col("last_t").max())
              .with_columns(pct_late=pl.col("late") / pl.col("votes"))
              .sort("late", descending=True))
    latest = (per_sec.filter(pl.col("uf") != "zz").sort("last_t", descending=True)
              .head(args.top).select("uf", "mun_nome", "zona", "secao", "local", "votes",
                                     "late", "last_t").to_dicts())
    close_hist = (per_sec.filter(pl.col("uf") != "zz")
                  .with_columns(b=(pl.col("last_after_close").clip(lower_bound=-H) // 900))
                  .group_by("b").agg(pl.len()).sort("b"))

    stats = {
        "total_votes": total,
        "official_comparecimento": int(official.join(fetched_muns, on=["uf", "mun"],
                                                     how="semi")["comparecimento"].sum()),
        "official_comparecimento_all": int(official["comparecimento"].sum()),
        "secoes_ok": ok.height,
        "secoes_total": pl.read_parquet(SECOES).height,
        "secoes_not_ok": status.filter(pl.col("status") != "ok")["status"].value_counts()
        .to_dicts(),
        "late_votes": late, "late_pct": late / total if total else 0,
        "late_votes_brazil": int(per_sec.filter(pl.col("uf") != "zz")["late"].sum()),
        "secoes_with_late": int((per_sec["late"] > 0).sum()),
        "by_uf": uf_tab.to_dicts(),
        "top_municipios": top(per_sec, ["uf", "mun", "mun_nome"]),
        "top_locais": top(per_sec.filter(pl.col("local").is_not_null()),
                          ["uf", "mun_nome", "zona", "local_id", "local", "endereco"]),
        "latest_secoes": latest,
        "close_histogram_15min": [{"bucket_min": int(b) * 15, "secoes": int(n)}
                                  for b, n in close_hist.iter_rows()],
        "tz_inferred_differs": meta.filter(pl.col("tz_differs"))
        .group_by("uf", "mun_nome").len().to_dicts(),
    }

    from pathlib import Path
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "timeline.json").write_text(json.dumps(timeline, separators=(",", ":")))
    (out / "stats.json").write_text(json.dumps(stats, ensure_ascii=False, separators=(",", ":")))
    print(f"votes {total:,} | late {late:,} ({stats['late_pct']:.2%}) | "
          f"last minute {end_min // 60:02d}:{end_min % 60:02d} BRT -> {out}")


if __name__ == "__main__":
    main()
