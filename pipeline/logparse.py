"""Parse an urna log package (.jez = zip with logd.dat) into vote timestamps.

Times are kept as seconds since *local* midnight of election day, exactly as
the urna clock recorded them; timezone conversion happens in aggregate.py.
"""

import io
import re
import zipfile

from config import ELECTION_DATE

_D = re.escape(ELECTION_DATE.encode())
_PREFIX = rb"^" + _D + rb" (\d\d):(\d\d):(\d\d)\t\w+\t(\d+)\tVOTA\t"
RE_VOTE = re.compile(_PREFIX + rb"O voto do eleitor foi computado\t(\w+)", re.M)
RE_READY = re.compile(_PREFIX + rb"Urna pronta para receber votos", re.M)
RE_CLOSE = re.compile(_PREFIX + rb"Operador selecionou: Encerrar vota", re.M)
# Events for the fallback rule: a Presidente vote confirmed but never "computado"
# before the next voter, an urna restart (logd starting) or the end of the log.
RE_EVENT = re.compile(rb"^" + _D + rb" (\d\d):(\d\d):(\d\d)\t\w+\t(\d+)\t(?:VOTA\t(?P<vota>"
                      rb"Eleitor foi habilitado|Voto confirmado para \[Presidente\]|"
                      rb"O voto do eleitor foi computado)|LOGD\t(?P<boot>In\xedcio das opera\xe7\xf5es do logd))"
                      rb"\t(?P<hash>\w+)", re.M)
FALLBACK_RULE = "confirmado_sem_computado"
# Each urna declares its seção at boot (app GAP); contingency urnas declare seção 0000.
_GAP = rb"^" + _D + rb" [\d:]{8}\t\w+\t\d+\tGAP\t"
_EOL = rb"\t\w+\r?\n"  # line hash, end of line
RE_DECL = re.compile(_GAP + rb"Munic\xedpio: (\d+)" + _EOL + _GAP + rb"Zona Eleitoral: (\d+)" + _EOL
                     + _GAP + rb"Se\xe7\xe3o Eleitoral: (\d+)", re.M)


def _secs(m: re.Match) -> int:
    return int(m[1]) * 3600 + int(m[2]) * 60 + int(m[3])


def declared_secoes(text: bytes) -> set[tuple[int, int, int]]:
    """(município, zona, seção) the urna was loaded for on election day, contingency excluded."""
    return {(int(m[1]), int(m[2]), int(m[3])) for m in RE_DECL.finditer(text) if int(m[3]) != 0}


def confirmed_not_computed(text: bytes) -> list[tuple[int, int, str, bytes]]:
    """Presidente votes confirmed but never computed in this log (e.g. urna failed mid-vote).

    Returns (seconds, urna id, line hash, line) for each such confirmation.
    """
    out, pending = [], None
    for m in RE_EVENT.finditer(text):
        if m["vota"] == b"Voto confirmado para [Presidente]":
            pending = m
        elif m["vota"] == b"O voto do eleitor foi computado":
            pending = None
        elif pending is not None:  # next voter enabled, or the urna restarted
            out.append(pending)
            pending = None
    if pending is not None:  # the log ended mid-vote
        out.append(pending)
    return [(_secs(m), int(m[4]), m["hash"].decode(), m[0]) for m in out]


def parse_log_bytes(texts: list[bytes], secao_key: tuple[int, int, int] | None = None) -> dict:
    """Extract election-day votes from one or more logd.dat contents.

    A package can carry logs of other seções' urnas (e.g. from a contingency urna
    that recovered several seções); with secao_key, votes in logs that declare a
    different seção are skipped and counted as foreign.

    Votes inferred by the fallback rule are returned separately in "fallback",
    never mixed with the computed votes.
    """
    seen = set()
    votes = []  # (seconds, urna id, line hash)
    fallback = []
    seen_fb = set()
    urnas = set()
    ready, close = [], []
    dupes = 0
    foreign_logs = foreign_votes = 0
    logs = []  # per-log summary, kept as evidence for reconciliation
    for text in texts:
        decl = declared_secoes(text)
        found = RE_VOTE.findall(text)
        info = {"urnas": sorted({int(u) for *_, u, _ in found}),
                "declared": sorted(f"{m}/{z}/{s}" for m, z, s in decl),
                "computed": len(found), "used": True}
        logs.append(info)
        if secao_key and decl and secao_key not in decl:
            info["used"] = False
            foreign_logs += 1
            foreign_votes += len(found)
            continue
        for m in RE_VOTE.finditer(text):
            key = (m[0], m[5])
            if key in seen:
                dupes += 1
                continue
            seen.add(key)
            votes.append((_secs(m), int(m[4]), m[5].decode()))
            urnas.add(m[4])
        for t, urna, hash_, line in confirmed_not_computed(text):
            if line not in seen_fb:
                seen_fb.add(line)
                fallback.append((t, urna, hash_))
        ready += [_secs(m) for m in RE_READY.finditer(text)]
        close += [_secs(m) for m in RE_CLOSE.finditer(text)]
    first_ready = min(ready) if ready else None
    n_pre = 0
    if first_ready is not None:
        kept = [v for v in votes if v[0] >= first_ready]
        n_pre = len(votes) - len(kept)
        votes = kept
        fallback = [f for f in fallback if f[0] >= first_ready]
    votes.sort()
    fallback.sort()
    return {
        "votes": votes,
        "fallback": fallback,
        "logs": logs,
        "n_votes": len(votes),
        "n_fallback": len(fallback),
        "n_dupes": dupes,
        "n_pre_ready": n_pre,
        "n_foreign_logs": foreign_logs,
        "n_foreign_votes": foreign_votes,
        "n_urnas": len(urnas),
        "ready_first": first_ready,
        "close_last": max(close) if close else None,
        "first_vote": votes[0][0] if votes else None,
        "last_vote": votes[-1][0] if votes else None,
    }


def unpack(blobs: list[bytes]) -> tuple[list[bytes], int]:
    """Return the .dat members of each log package (and nested packages)."""
    texts, n_members = [], 0
    stack = list(blobs)
    while stack:
        blob = stack.pop()
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            for name in z.namelist():
                data = z.read(name)
                n_members += 1
                if data[:2] == b"PK":
                    stack.append(data)
                elif name.endswith(".dat"):
                    texts.append(data)
    return texts, n_members


def parse_packages(blobs: list[bytes], secao_key: tuple[int, int, int] | None = None) -> dict:
    texts, n_members = unpack(blobs)
    out = parse_log_bytes(texts, secao_key)
    out["n_members"] = n_members
    return out
