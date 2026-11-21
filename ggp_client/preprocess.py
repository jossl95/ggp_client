"""Save downloaded datafiles as parquet under data_dir/download/processed, with columns that say where each row came from.

One parquet per data file, at the same path as in download/raw, plus a .meta.json with its labels (dta and sav):
download/raw/ggs-ii/w1/at/ggs2_w1_at_v1.1/x/GGSII_Wave1_AT_V_1_1.dta ->
    download/processed/ggs-ii/w1/at/ggs2_w1_at_v1.1/x/GGSII_Wave1_AT_V_1_1.parquet and GGSII_Wave1_AT_V_1_1.meta.json
A file that comes in several types (dta, sav and csv of one dataset) is read once, in the order of read.FORMATS.
FFS is read from its converted_data.dta/.sav, which the download puts in download/processed/ffs/<cc>/ already;
a country without one is read from the raw .sps/.dat.
Added columns: ggp_project, ggp_round, ggp_series, ggp_wave, ggp_country, ggp_version, ggp_file.
An existing parquet (and meta.json) is kept unless force.
"""
import asyncio
import json
import logging
import os
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import polars as pl

from . import ffs_conversion, read

log = logging.getLogger("ggp_client")


def datafiles(folder, types=read.FORMATS):
    """One file per dataset file in folder: the preferred type of each name.

    FFS: the converted files (labelled) when there are any, else the raw ones; never both, as they hold the same data.
    """
    files = [p for p in sorted(Path(folder).rglob("*"))
             if p.suffix.lower() in types and p.is_file() and ".part" not in p.name]
    converted = [p for p in files if p.name.lower().startswith(ffs_conversion.CONVERTED)]
    best = {}
    for p in converted or files:
        key = p.with_suffix("").as_posix().lower()
        if key not in best or read.FORMATS.index(p.suffix.lower()) < read.FORMATS.index(best[key].suffix.lower()):
            best[key] = p
    return sorted(best.values())


def sources(r, root, out):
    """[(data file, its path under root or out)] of row r; none if it is not downloaded.

    FFS: its converted dta/sav in out (download/processed) when there are any, else the raw files under root.
    """
    root, out, lp = Path(root), Path(out), r["local_path"]
    if not lp or not (root / lp).exists():
        return []
    conv = [p for p in datafiles(out / lp, read.LABELLED) if p.name.lower().startswith(ffs_conversion.CONVERTED)]
    return [(p, p.relative_to(out if conv else root)) for p in conv or datafiles(root / lp)]


def jobs(rows, root, out, force=False):
    """(source, path, target, row) for every data file of rows whose parquet is missing (or all, if force).

    Returns (jobs, skipped).
    """
    todo, skipped = [], 0
    for r in rows:
        for src, rel in sources(r, root, out):
            target = Path(out) / rel.with_suffix(".parquet")
            if not force and target.exists() and (src.suffix.lower() not in read.LABELLED or _meta(target).exists()):
                skipped += 1
            else:
                todo.append((src, rel, target, r))
    return todo, skipped


def plan(src, r, rel):
    """The LazyFrame of one file with the provenance columns; ggp_file is rel."""
    tags = dict(project=r["project"], round=r["round"], series=r["series"], wave=r["wave"] or None,
                country=r["country"] or None, version=r["version"], file=Path(rel).as_posix())
    return read.scan(src).with_columns(pl.lit(v, pl.Int64 if k == "round" else pl.String).alias(f"ggp_{k}")
                                       for k, v in tags.items())


def _meta(target):
    return target.with_suffix(".meta.json")


def save_meta(src, target, rel):
    """Write target's .meta.json with the labels of src (dta, sav); nothing for other types."""
    m = read.meta(src)
    if m is not None:
        part = target.with_suffix(".meta.part.json")
        part.write_text(json.dumps(dict(file=Path(rel).as_posix(), **m), ensure_ascii=False, indent=1),
                        encoding="utf-8")
        os.replace(part, _meta(target))


def _part(target):
    target.parent.mkdir(parents=True, exist_ok=True)
    return target.with_name(target.stem + ".part.parquet")


def write(src, rel, target, r):
    """One file: parquet and meta.json, written whole or not at all. The async version runs this in a process."""
    part = _part(target)
    try:
        plan(src, r, rel).sink_parquet(part)
        save_meta(src, target, rel)
        os.replace(part, target)
    except BaseException:
        part.unlink(missing_ok=True)
        target.with_suffix(".meta.part.json").unlink(missing_ok=True)
        raise


def preprocess(rows, root, out, force=False):
    """Write the parquet files one after the other. Returns (done, skipped, [(file, error)])."""
    todo, skipped = jobs(rows, root, out, force)
    failed = []
    for src, rel, target, r in todo:
        try:
            write(src, rel, target, r)
            log.info("preprocessed %s", target.relative_to(out))
        except Exception as e:  # one bad file must not stop the rest
            failed.append((str(rel), str(e) or type(e).__name__))
            log.error("preprocess failed %s: %s", *failed[-1])
    return len(todo) - len(failed), skipped, failed


async def preprocess_async(rows, root, out, force=False, concurrency=4):
    """preprocess(), `concurrency` files at a time, each in its own process (read, then polars sinks it lazily)."""
    todo, skipped = jobs(rows, root, out, force)
    loop, failed = asyncio.get_running_loop(), []

    async def one(src, rel, target, r):
        try:
            await loop.run_in_executor(ex, write, src, rel, target, dict(r))  # a sqlite3.Row cannot go to a process
            log.info("preprocessed %s", target.relative_to(out))
        except Exception as e:  # the worker's log lines would not reach this log: log here
            msg = ("a worker process died: out of memory (lower concurrency), or a script without "
                   "`if __name__ == \"__main__\":`") if isinstance(e, BrokenProcessPool) else str(e) or type(e).__name__
            failed.append((str(rel), msg))
            log.error("preprocess failed %s: %s", *failed[-1])

    with read.pool(concurrency) as ex:  # also bounds memory: dta and sav are read whole
        await asyncio.gather(*(one(*j) for j in todo))
    return len(todo) - len(failed), skipped, failed
