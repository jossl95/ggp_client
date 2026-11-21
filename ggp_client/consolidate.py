"""Stack the preprocessed parquet files per round and wave into one file: data_dir/consolidated/<series>_<wave>.parquet.

    consolidated/ggs-ii_w1.parquet   every GGS-II wave 1 country
    consolidated/ggs-i_w2.parquet
    consolidated/ffs.parquet         FFS has no waves

Columns are matched by name; a column a country lacks is null there, and a column with different types in
different countries gets the type that holds both (a number and a text column become text). The ggp_country
and ggp_file columns say where each row came from. Everything is lazy: polars streams the files into the result.

Country-specific variables are left out: GGS names them after the GGP country number, <name>_<country><nn>
(aut01a_2101 and covid_2201a are Austria's 21 and Estonia's 22; dem07_1401 is Germany's 14). The number comes
from COUNTRY_NUMBERS, or else from the file's own `country` variable. The per-country files in processed/download keep them.
An existing result is kept unless force.
"""
import asyncio
import logging
import os
import re
from collections import defaultdict
from pathlib import Path

import polars as pl

from . import preprocess

log = logging.getLogger("ggp_client")

# GGP country numbers, as in the `country` variable of the GGS-II files (Kazakhstan's file has none: from its names)
COUNTRY_NUMBERS = dict(AT=21, CZ=28, DE=14, DK=37, EE=22, FI=39, FR=15, GB=43, HK=38, HR=31, IS=44, KZ=34, MD=35,
                       NL=18, NO=20, PL=26, SE=29, TW=41, UY=40)


def groups(rows, root, pre):
    """{name: [(parquet file, country)]} for the preprocessed files of rows, one name per series and wave."""
    out = defaultdict(list)
    for r in rows:
        for _, rel in preprocess.sources(r, root, pre):
            f = Path(pre) / rel.with_suffix(".parquet")
            name = f"{r['series']}_{r['wave']}" if r["wave"] else r["series"]
            if f.exists():
                out[name.lower()].append((f, r["country"]))
    return dict(out)


def country_columns(lf, country):
    """The names in lf that end in this country's number: aut01a_2101 for Austria (21)."""
    names = lf.collect_schema().names()
    number = COUNTRY_NUMBERS.get((country or "").upper())
    own = next((c for c in names if c.lower() == "country"), None)
    if number is None and own:
        v = lf.select(pl.col(own).drop_nulls().first()).collect().item()
        number = int(v) if isinstance(v, (int, float)) and 10 <= v < 100 else None
    return [c for c in names if number and re.search(rf"_{number}\d\d[a-z]*$", c, re.I)]


def plan(files):
    """The stacked LazyFrame of [(parquet file, country)], without country-specific variables."""
    frames = []
    for f, country in files:
        lf = pl.scan_parquet(f, glob=False)
        if drop := country_columns(lf, country):
            log.info("consolidate: leaving out %d %s-specific variables of %s", len(drop), country, f.name)
        frames.append(lf.drop(drop))
    return pl.concat(frames, how="diagonal_relaxed")


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
