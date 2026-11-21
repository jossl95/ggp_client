"""Read a downloaded datafile into polars: .parquet, .dta, .sav, .csv, or an FFS .sps (with its .dat).

    df = read("GGSII_Wave1_AT_V_1_1.dta")          # polars DataFrame
    lf = scan("GGSII_Wave1_AT_V_1_1.parquet")      # polars LazyFrame
    df = await read_async("GGSII_Wave1_AT_V_1_1.sav")
    dfs = await read_all_async([...], concurrency=4)   # several files at once
    m = meta("GGSII_Wave1_AT_V_1_1.dta")           # variable and value labels (dta and sav)

Values are the codes; value labels are not applied. In .sav, values SPSS marks as missing are null.
"""
import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import polars as pl
import pyreadstat

from . import ffs_conversion

FORMATS = (".parquet", ".dta", ".sav", ".csv", ".sps")  # also the preference when one file comes in several
LAZY = (".parquet", ".csv")  # polars scans these itself; the others are read whole, then made lazy
LABELLED = (".dta", ".sav")  # the types that carry variable and value labels


def _dat(sps):
    """The .dat of the same name, or the only .dat next to the .sps."""
    dats = [p for p in sps.parent.iterdir() if p.suffix.lower() == ".dat"]
    same = [p for p in dats if p.stem.lower() == sps.stem.lower()]
    if same or len(dats) == 1:
        return (same or dats)[0]
    raise ValueError(f"no .dat for {sps}")


def _readstat(p, **kw):
    f = pyreadstat.read_dta if p.suffix.lower() == ".dta" else pyreadstat.read_sav
    try:
        return f(p, **kw)
    except (pyreadstat.ReadstatError, UnicodeDecodeError):  # some GGS files mix encodings: read as their .sav twins
        return f(p, encoding="latin1", **kw)


def scan(path):
    """A LazyFrame of the file. Parquet and csv are scanned lazily; dta, sav and sps are read first."""
    p = Path(path)
    s = p.suffix.lower()
    if s == ".parquet":
        return pl.scan_parquet(p, glob=False)  # a folder may be named "[raw data]"
    if s == ".csv":
        return pl.scan_csv(p, glob=False, infer_schema_length=None)  # the whole file: a late text code must not fail the read
    return read(p).lazy()


def read(path):
    """A DataFrame of the file."""
    p = Path(path)
    s = p.suffix.lower()
    if s in LAZY:
        return scan(p).collect()
    if s in LABELLED:
        return _readstat(p, output_format="polars")[0]
    if s == ".sps":
        return pl.from_pandas(ffs_conversion.read_ffs(p, _dat(p))[0])
    raise ValueError(f"{p.name}: cannot read {s or 'files without a type'}; choose from {', '.join(FORMATS)}")


def pool(workers):
    """Processes for dta, sav and sps: pyreadstat holds the GIL, so threads would read them one at a time.

    spawn, not fork: a forked polars can deadlock. A script using this needs `if __name__ == "__main__":`.
    """
    return ProcessPoolExecutor(workers, mp_context=multiprocessing.get_context("spawn"))


async def read_all_async(paths, concurrency=4):
    """[read(p) for p in paths], concurrently: parquet and csv on polars' async engine, the others in
    `concurrency` processes."""
    loop = asyncio.get_running_loop()
    with pool(concurrency) as ex:  # processes start only when a dta/sav/sps comes along
        return await asyncio.gather(*(scan(p).collect_async() if Path(p).suffix.lower() in LAZY
                                      else loop.run_in_executor(ex, read, p) for p in paths))


async def read_async(path):
    """read(), without blocking the event loop."""
    return (await read_all_async([path], 1))[0]


def meta(path):
    """The labels of a dta or sav file as a JSON-ready dict (None for other types).

    column_labels {variable: label}, value_labels {variable: {code: label}}, missing_ranges (sav: user-defined
    missing values), original_types (Stata/SPSS formats) and file_label.
    """
    p = Path(path)
    if p.suffix.lower() not in LABELLED:
        return None
    m = _readstat(p, metadataonly=True)[1]
    code = lambda k: str(int(k)) if isinstance(k, float) and k.is_integer() else str(k)  # JSON keys are text: 1.0 -> "1"
    return dict(file_label=m.file_label, column_labels=m.column_names_to_labels,
                value_labels={c: {code(k): v for k, v in d.items()} for c, d in m.variable_value_labels.items()},
                missing_ranges=m.missing_ranges, original_types=m.original_variable_types)
