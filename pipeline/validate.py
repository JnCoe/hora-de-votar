"""Compare votes counted from logs with official comparecimento per município.

Only municípios whose seções have all been fetched are compared.
"""

import polars as pl

from config import DATA, OFFICIAL_DIR, SECOES, load_status


def hhmm(col: str) -> pl.Expr:
    s = pl.col(col).cast(pl.Int64)
    return pl.format("{}:{}", (s // 3600).cast(pl.String).str.zfill(2),
                     (s % 3600 // 60).cast(pl.String).str.zfill(2))


def main() -> None:
    # aggregated seções have no urna of their own; their voters are in the principal's log
    secoes = (pl.read_parquet(SECOES).filter(pl.col("principal").is_null())
              .drop("agregadas", "principal"))
    status = load_status()
    st = secoes.join(status, on="secao_id")
    print("status counts:\n", st["status"].value_counts().sort("count", descending=True))

    official = pl.read_parquet(OFFICIAL_DIR / "mun_comparecimento.parquet")
    # computed votes + votes inferred by the fallback rule (stored apart in votes_fallback/)
    status = status.with_columns(n_fallback=pl.col("n_fallback").fill_null(0)
                                 if "n_fallback" in status.columns else pl.lit(0))
    per_mun = (secoes.join(status.select("secao_id", "status", "n_votes", "n_fallback"),
                           on="secao_id", how="left")
               .group_by("uf", "mun", "mun_nome")
               .agg(pl.len().alias("secoes"),
                    pl.col("status").is_null().sum().alias("not_fetched"),
                    (pl.col("status") != "ok").sum().alias("not_ok"),
                    pl.col("n_fallback").sum().alias("fallback_votes"),
                    (pl.col("n_votes") + pl.col("n_fallback")).sum().alias("log_votes"))
               .filter(pl.col("not_fetched") == 0)
               .join(official, on=["uf", "mun"])
               .with_columns((pl.col("log_votes") - pl.col("comparecimento")).alias("diff"),
                             (pl.col("log_votes") / pl.col("comparecimento") - 1).alias("rel")))
    print(f"\n{per_mun.height} municípios fully fetched; "
          f"{per_mun['fallback_votes'].sum()} votes come from the fallback rule")
    tot_log, tot_off = per_mun["log_votes"].sum(), per_mun["comparecimento"].sum()
    print(f"log votes {tot_log:,} vs official {tot_off:,}: diff {tot_log - tot_off:+,} "
          f"({tot_log / tot_off - 1:+.4%})")
    print(f"exact match: {(per_mun['diff'] == 0).mean():.1%} of municípios; "
          f"|rel| < 0.1%: {(per_mun['rel'].abs() < 0.001).mean():.1%}")
    print("\nper UF:")
    print(per_mun.group_by("uf").agg(pl.len().alias("muns"), pl.col("log_votes").sum(),
                                     pl.col("comparecimento").sum(), pl.col("diff").sum(),
                                     (pl.col("diff") == 0).mean().round(3).alias("exact"))
          .sort("uf"))
    mismatches = per_mun.filter(pl.col("diff") != 0)
    mismatches.write_parquet(DATA / "mismatches.parquet")
    print(f"\n{mismatches.height} mismatched municípios -> {DATA / 'mismatches.parquet'}")
    print("\nlargest mismatches:")
    print(per_mun.filter(pl.col("diff") != 0).sort(pl.col("diff").abs(), descending=True)
          .select("uf", "mun_nome", "secoes", "not_ok", "fallback_votes", "log_votes",
                  "comparecimento", "diff")
          .head(15))

    print("\nurna opening time (local clock) by UF, median of first 'Urna pronta':")
    print(st.filter(pl.col("status") == "ok").group_by("uf")
          .agg(pl.col("ready_first").median().alias("ready"),
               pl.col("close_last").median().alias("close"),
               pl.col("last_vote").max().alias("last_vote"))
          .with_columns(hhmm("ready"), hhmm("close"), hhmm("last_vote")).sort("uf"))
    print("\nanomalies: dupes", st["n_dupes"].sum(), "| votes before ready", st["n_pre_ready"].sum(),
          "| seções with >1 urna", (st["n_urnas"] > 1).sum(),
          "| >1 hash", (st["n_hashes"] > 1).sum(), "| >1 zip member", (st["n_members"] > 1).sum())


if __name__ == "__main__":
    main()
