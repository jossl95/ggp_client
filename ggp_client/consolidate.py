"""Stack the preprocessed parquet files per round and wave into one file: data_dir/download/consolidated/<series>_<wave>.parquet.

    download/consolidated/ggs-ii_w1.parquet   every GGS-II wave 1 country
    download/consolidated/ggs-i_w2.parquet
    download/consolidated/ffs.parquet         FFS has no waves

Columns are matched by name; a column a country lacks is null there, and a column with different types in
different countries gets the type that holds both (a number and a text column become text). The ggp_country
and ggp_file columns say where each row came from. Everything is lazy: polars streams the files into the result.

Country-specific variables are left out: GGS names them after the GGP country number, <name>_<country><nn>
(aut01a_2101 and covid_2201a are Austria's 21 and Estonia's 22; dem07_1401 is Germany's 14). The number comes
from COUNTRY_NUMBERS, or else from the file's own `country` variable. The per-country files in download/processed keep them.
Each result gets <name>.meta.json: the variable and value labels of its input files, harmonized (see
merged_meta). to_stata() writes <name>.dta from the two, with the labels applied.
An existing result is kept while it is up to date: it holds exactly the files of the rows given and is newer
than all of them. Otherwise (a country added or left out, a file preprocessed again) it is made again.
"""
import asyncio
import json
import logging
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl
import pyreadstat

from . import processing, read

log = logging.getLogger("ggp_client")

# GGP country numbers, as in the `country` variable of the GGS-II files (Kazakhstan's file has none: from its names)
COUNTRY_NUMBERS = dict(AT=21, CZ=28, DE=14, DK=37, EE=22, FI=39, FR=15, GB=43, HK=38, HR=31, IS=44, KZ=34, MD=35,
                       NL=18, NO=20, PL=26, SE=29, TW=41, UY=40)


def groups(rows, root, pre):
    """{name: [(parquet file, country, source path)]} for the preprocessed files of rows, per series and wave."""
    out = defaultdict(list)
    for r in rows:
        for _, rel in processing.sources(r, root, pre):
            f = Path(pre) / rel.with_suffix(".parquet")
            name = f"{r['series']}_{r['wave']}" if r["wave"] else r["series"]
            if f.exists():
                out[name.lower()].append((f, r["country"], rel.as_posix()))
    return dict(out)


def country_columns(lf, country):
    """The names in lf that end in this country's number: aut01a_2101 for Austria (21)."""
    names = lf.collect_schema().names()
    number = COUNTRY_NUMBERS.get((country or "").upper())
    own = next((c for c in names if c.lower() == "country"), None)
    if number is None and own:
        vals = lf.select(pl.col(own).drop_nulls().unique()).collect().to_series().to_list()
        v = vals[0] if len(vals) == 1 else None  # several countries in one file: no number of its own
        number = int(v) if isinstance(v, (int, float)) and 10 <= v < 100 else None
    return [c for c in names if number and re.search(rf"_{number}\d\d[a-z]*$", c, re.I)]


def plan(files):
    """The stacked LazyFrame of [(parquet file, country, source path)], without country-specific variables."""
    frames = []
    for f, country, _ in files:
        lf = pl.scan_parquet(f, glob=False)
        if drop := country_columns(lf, country):
            log.info("consolidate: leaving out %d %s-specific variables of %s", len(drop), country, f.name)
        frames.append(lf.drop(drop))
    return pl.concat(frames, how="diagonal_relaxed")


def current(target, files):
    """True if target was made from exactly these files (its ggp_file column) and is newer than all of them."""
    try:
        if target.stat().st_mtime < max(f.stat().st_mtime for f, *_ in files) or not _meta(target).exists():
            return False
        have = pl.scan_parquet(target, glob=False).select(pl.col("ggp_file").unique()).collect().to_series()
        return set(have) == {src for *_, src in files}
    except (OSError, pl.exceptions.PolarsError):  # missing or unreadable: make it again
        return False


def _todo(rows, root, pre, out, force):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    todo, skipped = {}, 0
    for name, files in groups(rows, root, pre).items():
        if not force and current(out / f"{name}.parquet", files):
            skipped += 1
        else:
            todo[name] = files
    return todo, skipped


def _meta(parquet):
    return parquet.with_suffix(".meta.json")


def _norm(label):
    return " ".join(label.split()).casefold()  # "Czech  Republic" and "czech republic" are one label


def _pick(spellings):
    """The label most files use (by its normalized form), in its most common spelling; ties go to the first file."""
    counts = Counter(_norm(s) for s in spellings)
    top = counts.most_common(1)[0][0]
    return Counter(s for s in spellings if _norm(s) == top).most_common(1)[0][0], counts


def merged_meta(files, columns):
    """The harmonized labels of a consolidated file, from its input files' meta.json, for the columns it kept.

    Variable label: the one most files use. Value labels, per variable: a code gets the label more than half of the
    files that label it use; with no such majority, all its labels joined by " / " (most used first). A label left
    on several codes gets its code added, "elementary school [2102]", so every code and every label is unique.
    Codes are never changed. original_labels keeps, for every code labelled differently, each wording and its files.
    """
    names, labels = defaultdict(list), defaultdict(lambda: defaultdict(list))  # labels[var][code] = [(label, file)]
    for f, *_ in files:
        if not _meta(f).exists():  # csv sources have none
            continue
        m = json.loads(_meta(f).read_text(encoding="utf-8"))
        for c, label in m["column_labels"].items():
            if label and c in columns:
                names[c].append(label)
        for c, d in m["value_labels"].items():
            if c in columns:
                for k, v in d.items():
                    if v:
                        labels[c][k].append((v, f.name))
    out = dict(column_labels={c: _pick(v)[0] for c, v in names.items()}, value_labels={}, original_labels={})
    for c, codes in labels.items():
        d = {}
        for k, found in codes.items():
            label, counts = _pick([v for v, _ in found])
            if len(counts) > 1:
                if counts[_norm(label)] * 2 <= len(found):  # no majority: say what each country means
                    label = " / ".join(_pick([v for v, _ in found if _norm(v) == n])[0] for n, _ in counts.most_common())
                orig = defaultdict(list)
                for v, f in found:
                    orig[v].append(f)
                out["original_labels"].setdefault(c, {})[k] = dict(orig)
            d[k] = label
        shared = Counter(_norm(v) for v in d.values())
        out["value_labels"][c] = {k: f"{v} [{k}]" if shared[_norm(v)] > 1 else v for k, v in d.items()}
    return out


def _done(out, name, files):
    target = out / f"{name}.parquet"
    columns = set(pl.read_parquet_schema(out / f"{name}.part.parquet"))
    _meta(target).write_text(json.dumps(merged_meta(files, columns), ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(out / f"{name}.part.parquet", target)  # last: a parquet without its meta.json is never current
    log.info("consolidated %d files into %s.parquet", len(files), name)


STATA_TEXT = 2045  # the longest text pyreadstat writes to Stata (no strL)
STATA_LONG = (-2_147_483_647, 2_147_483_620)  # the values a Stata value label can have


def _code(k):
    """A value-label code from meta.json ("1", "1.0") as a Stata integer, or None if it cannot be one."""
    try:
        f = float(k)
    except ValueError:
        return None
    return int(f) if f.is_integer() and STATA_LONG[0] <= f <= STATA_LONG[1] else None


def to_stata(parquet, force=False):
    """Write <name>.dta next to a consolidated <name>.parquet, with the labels of its meta.json applied.

    Stata labels whole numbers only, so value labels go on integer columns and on decimal columns that hold
    only whole numbers. Text longer than 2045 characters is cut. Kept while newer than the parquet, unless
    force. Returns the columns whose text was cut ([] if none), or None when the .dta was kept.
    """
    parquet = Path(parquet)
    target = parquet.with_suffix(".dta")
    if not force and target.exists() and target.stat().st_mtime >= parquet.stat().st_mtime:
        return None
    m = json.loads(_meta(parquet).read_text(encoding="utf-8"))
    df = read.read(parquet)
    cut = [c for c, t in df.schema.items() if t == pl.String and (df[c].str.len_chars().max() or 0) > STATA_TEXT]
    df = df.with_columns(pl.col(pl.Null).cast(pl.Int8), pl.col(pl.String).str.slice(0, STATA_TEXT))  # empty: numeric
    whole = {c for c, t in df.schema.items() if t.is_integer()}
    if floats := [c for c, t in df.schema.items() if t.is_float()]:
        flags = df.select(((pl.col(c) % 1 == 0) | pl.col(c).is_null()).all() for c in floats).row(0)
        whole |= {c for c, ok in zip(floats, flags) if ok}
    if whole:  # whole numbers as 4-byte Stata longs, not 8-byte doubles: half the file (Stata's compress does more)
        lo, hi = df.select(pl.col(sorted(whole)).min()).row(0), df.select(pl.col(sorted(whole)).max()).row(0)
        df = df.with_columns(pl.col(c).cast(pl.Int32) for c, a, b in zip(sorted(whole), lo, hi)
                             if a is None or (STATA_LONG[0] <= a and b <= STATA_LONG[1]))
    labels = {c: {code: v for k, v in d.items() if (code := _code(k)) is not None}
              for c, d in m["value_labels"].items() if c in whole}
    part = target.with_name(target.stem + ".part.dta")  # pyreadstat wants the real extension
    try:
        pyreadstat.write_dta(df, part, column_labels=[(m["column_labels"].get(c) or "")[:80] or None for c in df.columns],
                             variable_value_labels={c: d for c, d in labels.items() if d})
        os.replace(part, target)
    finally:
        part.unlink(missing_ok=True)
    return cut


async def to_stata_async(parquets, force=False, concurrency=2):
    """to_stata() for several files, `concurrency` at a time in processes (pyreadstat holds the GIL).

    Each file is held whole in memory while it is written: keep concurrency low for big files.
    Returns (done, skipped, [(file, error)]).
    """
    loop, failed, done = asyncio.get_running_loop(), [], 0

    async def one(p):
        nonlocal done
        try:
            cut = await loop.run_in_executor(ex, to_stata, p, force)
            if cut is not None:
                done += 1
                log.info("to_stata: wrote %s", Path(p).with_suffix(".dta").name)
            if cut:
                log.warning("to_stata: %s: text cut to %d characters in %s", Path(p).name, STATA_TEXT, ", ".join(cut))
        except Exception as e:
            failed.append((Path(p).name, str(e) or type(e).__name__))
            log.error("to_stata failed %s: %s", *failed[-1])

    with read.pool(concurrency) as ex:
        await asyncio.gather(*(one(p) for p in parquets))
    return done, len(parquets) - done - len(failed), failed


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
        plans = await asyncio.to_thread(lambda: [plan(todo[n]) for n in names])  # reads every file's schema
        await pl.collect_all_async([p.sink_parquet(out / f"{n}.part.parquet", lazy=True) for p, n in zip(plans, names)])
    except Exception:  # find which group failed: run them one by one
        for n in names:
            (out / f"{n}.part.parquet").unlink(missing_ok=True)
        log.warning("consolidate: running the groups one by one to find the one that fails")
        return await asyncio.to_thread(consolidate, rows, root, pre, out, force)
    for n in names:
        _done(out, n, todo[n])
    return len(names), skipped, []
