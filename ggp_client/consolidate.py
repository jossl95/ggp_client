"""Stack the preprocessed parquet files per round and wave into one file: data_dir/consolidated/<series>_<wave>.parquet.

    consolidated/ggs-ii_w1.parquet   every GGS-II wave 1 country
    consolidated/ggs-i_w2.parquet
    consolidated/ffs.parquet         FFS has no waves

Columns are matched by name; a column a country lacks is null there, and a column with different types in
different countries gets the type that holds both (a number and a text column become text). The ggp_country
and ggp_file columns say where each row came from. Everything is lazy: polars streams the files into the result.
An existing result is kept unless force.
"""
import asyncio
import logging
import os
from collections import defaultdict
from pathlib import Path

import polars as pl

from . import preprocess

log = logging.getLogger("ggp_client")


def groups(rows, root, pre):
    """{name: [parquet files]} for the preprocessed files of rows, one name per series and wave."""
    root, pre = Path(root), Path(pre)
    out = defaultdict(list)
    for r in rows:
        if r["local_path"] and (root / r["local_path"]).exists():
            name = f"{r['series']}_{r['wave']}" if r["wave"] else r["series"]
            for src in preprocess.datafiles(root / r["local_path"]):
                f = pre / src.relative_to(root).with_suffix(".parquet")
                if f.exists():
                    out[name.lower()].append(f)
    return dict(out)


def plan(files):
    return pl.concat([pl.scan_parquet(f, glob=False) for f in files], how="diagonal_relaxed")


def _todo(rows, root, pre, out, force):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    todo, skipped = {}, 0
    for name, files in groups(rows, root, pre).items():
        if (out / f"{name}.parquet").exists() and not force:
            skipped += 1
        else:
            todo[name] = files
    return todo, skipped


def _done(out, name, files):
    os.replace(out / f"{name}.part.parquet", out / f"{name}.parquet")
    log.info("consolidated %d files into %s.parquet", len(files), name)


def consolidate(rows, root, pre, out, force=False):
    """One group after the other. Returns (done, skipped, [(name, error)])."""
    todo, skipped = _todo(rows, root, pre, out, force)
    failed = []
    for name, files in todo.items():
        try:
            plan(files).sink_parquet(Path(out) / f"{name}.part.parquet")
            _done(Path(out), name, files)
        except Exception as e:  # say, two columns whose types cannot be combined
            (Path(out) / f"{name}.part.parquet").unlink(missing_ok=True)
            failed.append((name, str(e) or type(e).__name__))
            log.error("consolidate failed %s: %s", *failed[-1])
    return len(todo) - len(failed), skipped, failed


async def consolidate_async(rows, root, pre, out, force=False):
    """All groups at once, as one polars run (polars spreads the work over its threads)."""
    todo, skipped = _todo(rows, root, pre, out, force)
    out = Path(out)
    names = list(todo)
    try:
        await pl.collect_all_async([plan(todo[n]).sink_parquet(out / f"{n}.part.parquet", lazy=True) for n in names])
    except Exception:  # find which group failed: run them one by one
        for n in names:
            (out / f"{n}.part.parquet").unlink(missing_ok=True)
        log.warning("consolidate: running the groups one by one to find the one that fails")
        return await asyncio.to_thread(consolidate, rows, root, pre, out, force)
    for n in names:
        _done(out, n, todo[n])
    return len(names), skipped, []
