"""Download every seção's urna log and boletim, parse them in memory and store vote times.

Resumable: seções already present in <run>/status/*.parquet are skipped
(errors are retried on the next run). Raw logs never touch the disk; the
(small) boletins are kept in <run>/bu/ so every seção can be reconciled later.

    HDV_RUN=v2 uv run python fetch_parse.py --sample ../data/sample.parquet
    HDV_RUN=v2 uv run python fetch_parse.py            # everything
    HDV_RUN=v2 uv run python fetch_parse.py --shard 0/3  # one of 3 parallel processes

Every run writes to <run>/logs/: a readable log (fetch-*.log) and one JSON line
per seção with every request made for it (events-*.jsonl).
"""

import argparse
import asyncio
import collections
import json
import logging
import time
import traceback
from concurrent.futures import ProcessPoolExecutor

import httpx
import polars as pl
from tqdm import tqdm

from bu import presidente
from config import (BU_DIR, DATA, FALLBACK_DIR, HEADERS, RUN_DIR, SECOES, STATUS_DIR, VOTES_DIR,
                    aux_url, file_url, load_status)
from logparse import FALLBACK_RULE, parse_packages

log = logging.getLogger("fetch")
THROTTLE_CODES = {429, 503}
STATS = collections.Counter()  # run-wide counters reported in the log


class Events:
    """One JSON line per seção: what was requested, what came back, how long it took."""

    def __init__(self, path):
        self.f = open(path, "a", encoding="utf-8", buffering=1)  # line-buffered: survives a kill

    def write(self, rec: dict) -> None:
        self.f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def close(self) -> None:
        self.f.close()


def setup_logging(tag: str) -> Events:
    logs = RUN_DIR / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    handler = logging.FileHandler(logs / f"fetch-{stamp}-{tag}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return Events(logs / f"events-{stamp}-{tag}.jsonl")

STATUS_SCHEMA = {
    "secao_id": pl.UInt32, "status": pl.String, "aux_st": pl.String, "hash_st": pl.String,
    "tipos": pl.String, "n_hashes": pl.UInt8, "n_logfiles": pl.UInt8, "n_members": pl.UInt8,
    "n_votes": pl.UInt32, "n_dupes": pl.UInt32, "n_pre_ready": pl.UInt32,
    "n_foreign_logs": pl.UInt8, "n_foreign_votes": pl.UInt32, "n_fallback": pl.UInt16,
    "n_urnas": pl.UInt8, "ready_first": pl.Int32, "close_last": pl.Int32,
    "first_vote": pl.Int32, "last_vote": pl.Int32, "bytes": pl.UInt32,
    "bu_tp": pl.String, "bu_aptos": pl.Int32, "bu_comparecimento": pl.Int32,
    "logs": pl.String,  # JSON: per-log urnas, declared seção, computed votes, used
}
VOTE_SCHEMA = {"secao_id": pl.UInt32, "t_local": pl.Int32, "urna": pl.UInt32, "hash": pl.UInt64}
FALLBACK_SCHEMA = {**VOTE_SCHEMA, "rule": pl.String}
BU_SCHEMA = {"secao_id": pl.UInt32, "tp": pl.String, "data": pl.Binary}


class Writer:
    """Buffers results and flushes them as numbered parquet chunks."""

    def __init__(self, flush_every: int, tag: str):
        self.flush_every = flush_every
        self.tag = tag  # keeps chunk names unique across parallel shards
        for d in (VOTES_DIR, FALLBACK_DIR, STATUS_DIR, BU_DIR):
            d.mkdir(parents=True, exist_ok=True)
        self.part = len(list(STATUS_DIR.glob(f"part-*-{tag}-*.parquet")))
        self.t0 = time.time()
        self.done = 0
        self._reset()

    def _reset(self) -> None:
        self.status: list[dict] = []
        self.votes: list[tuple] = []
        self.fallback: list[tuple] = []
        self.bus: list[tuple] = []

    def add(self, secao_id: int, status: dict, votes, fallback, bu) -> None:
        self.status.append({"secao_id": secao_id, **status})
        self.votes += [(secao_id, t, urna, int(h, 16)) for t, urna, h in votes]
        self.fallback += [(secao_id, t, urna, int(h, 16), FALLBACK_RULE) for t, urna, h in fallback]
        if bu:
            self.bus.append((secao_id, *bu))
        if len(self.status) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.status:
            return
        name = f"part-{time.strftime('%Y%m%d%H%M%S')}-{self.tag}-{self.part:05d}.parquet"
        # data first: a status row must never exist without its votes
        for rows, schema, d in ((self.votes, VOTE_SCHEMA, VOTES_DIR),
                                (self.fallback, FALLBACK_SCHEMA, FALLBACK_DIR),
                                (self.bus, BU_SCHEMA, BU_DIR)):
            pl.DataFrame(rows, schema=schema, orient="row").write_parquet(d / name, compression="zstd")
        (pl.DataFrame(self.status, schema=STATUS_SCHEMA, strict=False)
         .write_parquet(STATUS_DIR / name, compression="zstd"))
        self.done += len(self.status)
        rate = self.done / max(time.time() - self.t0, 1e-9)
        log.info("saved %s: %d seções (%d this run, %.1f seções/s) | %s", name, len(self.status),
                 self.done, rate, " ".join(f"{k}={v}" for k, v in sorted(STATS.items())))
        self.part += 1
        self._reset()


async def get(client: httpx.AsyncClient, url: str, trace: list, retries: int = 5) -> httpx.Response:
    """GET with retries; every attempt is appended to trace (the seção's event record)."""
    for attempt in range(1, retries + 1):
        t = time.perf_counter()
        try:
            r = await client.get(url)
        except httpx.HTTPError as e:
            trace.append({"url": url, "attempt": attempt, "error": f"{type(e).__name__}: {e}",
                          "ms": round((time.perf_counter() - t) * 1000)})
            STATS["http_exceptions"] += 1
            log.warning("attempt %d/%d %s: %s: %s", attempt, retries, url, type(e).__name__, e)
            if attempt == retries:
                raise
            await asyncio.sleep(2 ** attempt)
            continue
        trace.append({"url": url, "attempt": attempt, "code": r.status_code, "bytes": len(r.content),
                      "ms": round((time.perf_counter() - t) * 1000)})
        STATS["requests"] += 1
        if r.status_code == 404 or r.status_code < 400:
            return r
        throttled = r.status_code in THROTTLE_CODES
        STATS["throttled" if throttled else "http_errors"] += 1
        log.warning("attempt %d/%d %s: HTTP %d%s", attempt, retries, url, r.status_code,
                    " (throttled, backing off)" if throttled else "")
        # back off much longer when the server says we're going too fast
        await asyncio.sleep((15 if throttled else 1) * 2 ** attempt)
    r.raise_for_status()
    return r


async def process(row: dict, client, sem, pool, writer: Writer, events: Events, bar) -> None:
    uf, mun, zona, secao = row["uf"], row["mun"], row["zona"], row["secao"]
    status = {"status": "ok"}
    votes, fallback, bu = [], [], None
    trace: list = []
    t0 = time.perf_counter()
    error = None
    try:
        async with sem:
            r = await get(client, aux_url(uf, mun, zona, secao), trace)
            if r.status_code == 404:
                status["status"] = "no_aux"
            else:
                aux = json.loads(r.content)
                hashes = aux.get("hashes", [])
                status.update(aux_st=aux.get("st"), n_hashes=len(hashes),
                              hash_st=",".join(h.get("st", "") for h in hashes))
                # prefer the hashes that were actually totalized
                used = [h for h in hashes if h.get("st") == "Totalizado"] or hashes[-1:]
                arqs = [(h["hash"], a) for h in used for a in h.get("arq", [])]
                status["tipos"] = ",".join(sorted({a["tp"] for _, a in arqs}))
                bu_arq = next(((h, a) for h, a in arqs if a.get("tp") in ("bu", "busa")), None)
                if bu_arq:
                    br = await get(client, file_url(uf, mun, zona, secao, bu_arq[0], bu_arq[1]["nm"]),
                                   trace)
                    if br.status_code == 200:
                        bu = (bu_arq[1]["tp"], br.content)
                        result = presidente(br.content)
                        status["bu_tp"] = bu[0]
                        if result:
                            status["bu_aptos"], status["bu_comparecimento"] = result
                    else:
                        status["bu_tp"] = f"{bu_arq[1]['tp']}:http{br.status_code}"
                urls = [file_url(uf, mun, zona, secao, h, a["nm"]) for h, a in arqs
                        if a.get("tp") == "log"]
                status["n_logfiles"] = len(urls)
                blobs = []
                for u in urls:
                    lr = await get(client, u, trace)
                    if lr.status_code == 200:
                        blobs.append(lr.content)
                status["bytes"] = sum(len(b) for b in blobs)
                if not arqs:
                    status["status"] = "no_files"
                elif not blobs:
                    status["status"] = "no_log"
        if status["status"] == "ok":
            parsed = await asyncio.get_running_loop().run_in_executor(
                pool, parse_packages, blobs, (int(mun), int(zona), int(secao)))
            votes = parsed.pop("votes")
            fallback = parsed.pop("fallback")
            status["logs"] = json.dumps(parsed.pop("logs"))
            status.update(parsed)
    except Exception as e:  # recorded as error, retried next run
        status = {"status": f"error:{type(e).__name__}"}
        votes, fallback, bu = [], [], None
        error = traceback.format_exc()
        log.error("seção %s %s/%s/%s (id %d) failed:\n%s", uf, mun, zona, secao, row["secao_id"], error)
    STATS[status["status"]] += 1
    events.write({"secao_id": row["secao_id"], "uf": uf, "mun": mun, "zona": zona, "secao": secao,
                  "status": status["status"], "tipos": status.get("tipos"), "bu_tp": status.get("bu_tp"),
                  "bu_comparecimento": status.get("bu_comparecimento"), "n_votes": status.get("n_votes"),
                  "n_fallback": status.get("n_fallback"),
                  "ms": round((time.perf_counter() - t0) * 1000), "requests": trace, "error": error,
                  "at": time.strftime("%Y-%m-%dT%H:%M:%S")})
    writer.add(row["secao_id"], status, votes, fallback, bu)
    bar.update(1)


async def run(todo: list[dict], concurrency: int, workers: int, flush_every: int, tag: str,
              events: Events) -> None:
    writer = Writer(flush_every, tag)
    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    with ProcessPoolExecutor(workers) as pool:
        async with httpx.AsyncClient(headers=HEADERS, timeout=60, http2=True,
                                     limits=limits) as client:
            bar = tqdm(total=len(todo), unit="seção", smoothing=0.05)
            queue: asyncio.Queue = asyncio.Queue()
            for row in todo:
                queue.put_nowait(row)

            async def worker():
                while not queue.empty():
                    await process(queue.get_nowait(), client, sem, pool, writer, events, bar)

            await asyncio.gather(*(worker() for _ in range(concurrency * 2)))
            bar.close()
    writer.flush()


def done_ids() -> set[int]:
    files = list(STATUS_DIR.glob("part-*.parquet"))
    if not files:
        return set()
    df = pl.concat([pl.read_parquet(f, columns=["secao_id", "status"]) for f in files])
    return set(df.filter(~pl.col("status").str.starts_with("error"))["secao_id"].to_list())


def purge(ids: set[int], files: list) -> None:
    """Drop the rows of these seções from earlier chunks (after a redo)."""
    for name in files:
        for d in (STATUS_DIR, VOTES_DIR, FALLBACK_DIR, BU_DIR):
            path = d / name
            if not path.exists():  # chunks written before that dataset existed
                continue
            df = pl.read_parquet(path)
            kept = df.filter(~pl.col("secao_id").is_in(list(ids)))
            if kept.height != df.height:
                kept.write_parquet(path, compression="zstd")


def redo_mismatched_ids(secoes: pl.DataFrame) -> set[int]:
    """Every seção with a log in municípios whose total differs from the official one."""
    mism = pl.read_parquet(DATA / "mismatches.parquet").select("uf", "mun")
    status = load_status().select("secao_id", "status")
    return set(secoes.join(mism, on=["uf", "mun"]).join(status, on="secao_id")
               .filter(pl.col("status") == "ok")["secao_id"].to_list())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", help="parquet with a secao_id column to restrict to")
    ap.add_argument("--uf", nargs="*", help="restrict to these UFs")
    ap.add_argument("--mun", nargs="*", help="restrict to these município codes")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--flush-every", type=int, default=5000)
    ap.add_argument("--redo-mismatched", action="store_true",
                    help="re-parse every seção in municípios listed in data/mismatches.parquet")
    ap.add_argument("--shard", default="0/1",
                    help="K/N: process only seções with secao_id %% N == K (run N of these in parallel)")
    args = ap.parse_args()
    k, n = map(int, args.shard.split("/"))
    tag = f"s{k}of{n}"
    events = setup_logging(tag)
    log.info("start: run dir %s, shard %s, concurrency %d, workers %d, args %s",
             RUN_DIR, args.shard, args.concurrency, args.workers, vars(args))

    secoes = (pl.read_parquet(SECOES).filter(pl.col("principal").is_null())
              .drop("agregadas", "principal")
              .filter(pl.col("secao_id") % n == k))
    if args.sample:
        ids = pl.read_parquet(args.sample, columns=["secao_id"])
        secoes = secoes.join(ids, on="secao_id", how="semi")
    if args.uf:
        secoes = secoes.filter(pl.col("uf").is_in(args.uf))
    if args.mun:
        secoes = secoes.filter(pl.col("mun").is_in(args.mun))
    if args.redo_mismatched:
        ids = redo_mismatched_ids(secoes)
        before = [p.name for p in STATUS_DIR.glob("part-*.parquet")]
        todo = secoes.filter(pl.col("secao_id").is_in(list(ids)))
        log.info("re-parsing %d seções in mismatched municípios", todo.height)
        asyncio.run(run(todo.to_dicts(), args.concurrency, args.workers, args.flush_every, tag, events))
        purge(ids, before)
        log.info("redo finished | %s", dict(STATS))
        return
    done = done_ids()
    todo = secoes.filter(~pl.col("secao_id").is_in(list(done)))
    if args.limit:
        todo = todo.head(args.limit)
    msg = (f"{STATUS_DIR.parent}: {secoes.height} seções selected, {len(done)} already done, "
           f"{todo.height} to fetch")
    print(msg)
    log.info(msg)
    t0 = time.time()
    try:
        asyncio.run(run(todo.to_dicts(), args.concurrency, args.workers, args.flush_every, tag, events))
    except BaseException:
        log.critical("run aborted:\n%s", traceback.format_exc())
        raise
    finally:
        events.close()
    log.info("finished in %.0fs | %s", time.time() - t0, " ".join(f"{k}={v}" for k, v in sorted(STATS.items())))
    print(f"finished in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
