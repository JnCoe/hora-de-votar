"""Reconcile every seção: votes found in its logs vs the Presidente comparecimento in its BU.

A difference is accepted only through a rule checked on that seção's own files,
or through an individually verified note in pipeline/reconciliation_notes.json
whose numbers match the data exactly. Anything else is listed as "pendente" and
the script exits with an error.

Also checks, per município, that the BUs add up to the official comparecimento,
and that no vote (urna, time, line hash) is counted in more than one seção.

    HDV_RUN=v2 uv run python reconcile.py
"""

import json
import sys

import polars as pl

from config import DOCS_DATA, NOTES, OFFICIAL_DIR, RUN_DIR, SECOES, VOTES_DIR, load_status

# category -> (description for the page, are the votes on the timeline?)
CATEGORIES = {
    "match": ("Votos do log = comparecimento do BU", True),
    "fallback_confirmado": ("Log termina após 'Voto confirmado' sem 'computado'; o BU confirma o voto", True),
    "fallback_rejeitado": ("Log termina após 'Voto confirmado' sem 'computado'; o BU não conta o voto", True),
    "sem_arquivo": ("Seção sem nenhum arquivo publicado e sem comparecimento", True),
    "cedula": ("Votação totalmente manual (cédulas): não existe horário dos votos", False),
    "log_nao_publicado": ("Urna eletrônica, mas o log não foi publicado pelo TSE", False),
    "nota": ("Diferença verificada individualmente (ver nota)", None),
    "pendente": ("Diferença ainda não explicada", None),
}


def load_notes() -> dict:
    if not NOTES.exists():
        return {}
    return {(n["uf"], n["mun"], n["zona"], n["secao"]): n for n in json.loads(NOTES.read_text())}


def classify(r: dict, notes: dict) -> dict:
    bu, computed, cands = r["bu_comparecimento"], r["n_votes"] or 0, r["n_fallback"] or 0
    out = {"fallback_accepted": 0, "votes_without_time": 0, "note": None}
    if r["status"] == "no_files" and bu is None:
        return {**out, "category": "sem_arquivo"}
    if r["status"] == "no_log" and bu is not None:
        cat = "cedula" if r["bu_tp"] == "busa" else "log_nao_publicado"
        return {**out, "category": cat, "votes_without_time": bu}
    if r["status"] == "ok" and bu is not None:
        gap = bu - computed
        if gap == 0:
            return {**out, "category": "fallback_rejeitado" if cands else "match"}
        if cands and gap == cands:
            return {**out, "category": "fallback_confirmado", "fallback_accepted": cands}
    note = notes.get((r["uf"], r["mun"], r["zona"], r["secao"]))
    if note and note["bu_comparecimento"] == bu and note["log_computed"] == computed \
            and note["fallback_candidates"] == cands:
        return {**out, "category": "nota", "fallback_accepted": note["fallback_accepted"],
                "votes_without_time": note["votes_without_time"], "note": note["summary_pt"]}
    return {**out, "category": "pendente"}


def main() -> None:
    only_fetched = "--only-fetched" in sys.argv  # for test runs on a subset
    secoes = pl.read_parquet(SECOES).filter(pl.col("principal").is_null()).drop("agregadas", "principal")
    status = load_status()
    if only_fetched:
        fetched_muns = secoes.join(status.select("secao_id"), on="secao_id").select("uf", "mun").unique()
        secoes = secoes.join(fetched_muns, on=["uf", "mun"])
    cols = ["secao_id", "status", "bu_tp", "bu_comparecimento", "n_votes", "n_fallback"]
    df = secoes.join(status.select(cols), on="secao_id", how="left")
    notes = load_notes()
    cls = pl.DataFrame([classify(r, notes) for r in df.iter_rows(named=True)],
                       schema={"fallback_accepted": pl.Int32, "votes_without_time": pl.Int32,
                               "note": pl.String, "category": pl.String})
    df = pl.concat([df, cls], how="horizontal").with_columns(
        timeline_votes=pl.col("n_votes").fill_null(0) + pl.col("fallback_accepted"),
        bu=pl.col("bu_comparecimento").fill_null(0))
    df.write_parquet(RUN_DIR / "reconciliation.parquet")

    ok = True
    print("seções by category:")
    print(df.group_by("category").agg(pl.len().alias("secoes"), pl.col("bu").sum().alias("bu_votes"),
                                      pl.col("timeline_votes").sum(), pl.col("votes_without_time").sum())
          .sort("secoes", descending=True))

    unfetched = df.filter(pl.col("status").is_null() | pl.col("status").str.starts_with("error"))
    if unfetched.height:
        ok = False
        print(f"\n!! {unfetched.height} seções not fetched (or with errors)")

    pend = df.filter(pl.col("category") == "pendente")
    if pend.height:
        ok = False
        print(f"\n!! {pend.height} seções pendentes:")
        print(pend.select("uf", "mun_nome", "mun", "zona", "secao", "status", "bu_tp", "bu_comparecimento",
                          "n_votes", "n_fallback"))

    # every vote must be explained: timeline votes + votes without time = BU, per seção
    bad = df.filter(pl.col("category") != "pendente", pl.col("status").is_not_null(),
                    pl.col("timeline_votes") + pl.col("votes_without_time") != pl.col("bu"),
                    pl.col("category") != "fallback_rejeitado")
    rejected = df.filter(pl.col("category") == "fallback_rejeitado")
    if bad.height:
        ok = False
        print(f"\n!! {bad.height} seções whose accounting does not add up:")
        print(bad.select("uf", "mun_nome", "zona", "secao", "category", "bu", "timeline_votes",
                         "votes_without_time"))

    official = pl.read_parquet(OFFICIAL_DIR / "mun_comparecimento.parquet")
    if only_fetched:
        official = official.join(df.select("uf", "mun").unique(), on=["uf", "mun"])
    per_mun = (df.group_by("uf", "mun", "mun_nome").agg(pl.col("bu").sum())
               .join(official, on=["uf", "mun"]).filter(pl.col("bu") != pl.col("comparecimento")))
    if per_mun.height:
        ok = False
        print(f"\n!! {per_mun.height} municípios where the BUs do not add up to the official total:")
        print(per_mun)

    dup = (pl.scan_parquet(VOTES_DIR / "*.parquet")
           .group_by("urna", "t_local", "hash").agg(pl.col("secao_id").n_unique().alias("n"))
           .filter(pl.col("n") > 1).collect(engine="streaming"))
    if dup.height:
        ok = False
        print(f"\n!! {dup.height} votes appear in more than one seção")

    tot = df.select(pl.col("bu").sum(), pl.col("timeline_votes").sum(), pl.col("votes_without_time").sum())
    print(f"\nBU total {tot['bu'][0]:,} | on the timeline {tot['timeline_votes'][0]:,} | "
          f"without time {tot['votes_without_time'][0]:,} | official {official['comparecimento'].sum():,}")

    exceptions = (df.filter(~pl.col("category").is_in(["match", "sem_arquivo"]))
                  .select("uf", "mun", "mun_nome", "zona", "secao", "category", "bu", "n_votes",
                          "n_fallback", "fallback_accepted", "votes_without_time", "note")
                  .sort("uf", "mun_nome", "zona", "secao"))
    summary = (df.group_by("category").agg(pl.len().alias("secoes"), pl.col("bu").sum().alias("votos_bu"),
                                           pl.col("timeline_votes").sum().alias("votos_na_linha_do_tempo"),
                                           pl.col("votes_without_time").sum().alias("votos_sem_horario"))
               .with_columns(descricao=pl.col("category").replace_strict(
                   {k: v[0] for k, v in CATEGORIES.items()}, return_dtype=pl.String)))
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    (DOCS_DATA / "reconciliation.json").write_text(json.dumps(
        {"complete": ok, "official_total": int(official["comparecimento"].sum()),
         "bu_total": int(tot["bu"][0]), "timeline_total": int(tot["timeline_votes"][0]),
         "without_time_total": int(tot["votes_without_time"][0]),
         "fallback_rejected": rejected.select("uf", "mun_nome", "zona", "secao").to_dicts(),
         "summary": summary.to_dicts(), "exceptions": exceptions.to_dicts()},
        ensure_ascii=False, separators=(",", ":")))
    print("\nRECONCILED: every seção explained" if ok else "\nNOT RECONCILED (see !! above)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
