"""Shared constants for the pipeline."""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DOCS_DATA = ROOT / "docs" / "data"

# 2026 general election, 2º turno (25/10/2026)
CICLO = "ele2026"
PLEITO = "3221"          # arquivo-urna pleito code for the 2º turno
ELEICAO = "6258"         # Eleição Ordinária Federal 2026 2º turno (Presidente)
ELECTION_DATE = "25/10/2026"

BASE = "https://resultados.tse.jus.br/oficial"
URNA_BASE = f"{BASE}/{CICLO}/arquivo-urna/{PLEITO}"

UFS = [
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go", "ma", "mg", "ms", "mt",
    "pa", "pb", "pe", "pi", "pr", "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to",
    "zz",
]

# Default UTC offset (hours) per UF. Urnas log their *local* clock time; the
# aggregation step refines this per município from observed opening times.
UF_UTC_OFFSET = {uf: -3 for uf in UFS}
UF_UTC_OFFSET.update({"ac": -5, "am": -4, "rr": -4, "ro": -4, "mt": -4, "ms": -4})
BRT_OFFSET = -3

HEADERS = {"User-Agent": "hora-de-votar/0.1 (pesquisa; dados abertos TSE)"}

SECOES = DATA / "secoes.parquet"
OFFICIAL_DIR = DATA / "official"

# Per-run outputs. HDV_RUN=v2 writes to data/v2/..., leaving earlier runs untouched.
RUN_DIR = DATA / os.environ["HDV_RUN"] if os.environ.get("HDV_RUN") else DATA
VOTES_DIR = RUN_DIR / "votes"
# votes inferred by logparse's fallback rule, kept apart from the computed votes
FALLBACK_DIR = RUN_DIR / "votes_fallback"
STATUS_DIR = RUN_DIR / "status"
BU_DIR = RUN_DIR / "bu"  # raw bu.dat / busa.dat bytes per seção
NOTES = ROOT / "pipeline" / "reconciliation_notes.json"


def aux_url(uf: str, mun: str, zona: str, secao: str) -> str:
    return (f"{URNA_BASE}/dados/{uf}/{mun}/{zona}/{secao}/"
            f"p00{PLEITO}-{uf}-m{mun}-z{zona}-s{secao}-aux.json")


def file_url(uf: str, mun: str, zona: str, secao: str, hash_: str, name: str) -> str:
    return f"{URNA_BASE}/dados/{uf}/{mun}/{zona}/{secao}/{hash_}/{name}"


def load_status():
    """All status chunks, oldest first; columns added later are null in older chunks."""
    import polars as pl
    files = sorted(STATUS_DIR.glob("part-*.parquet"))
    return (pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
            .unique("secao_id", keep="last", maintain_order=True))
