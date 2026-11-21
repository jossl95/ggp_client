"""Read a downloaded datafile into polars: .parquet, .dta, .sav, .csv, or an FFS .sps (with its .dat).

    df = read("GGSII_Wave1_AT_V_1_1.dta")          # polars DataFrame
    lf = scan("GGSII_Wave1_AT_V_1_1.parquet")      # polars LazyFrame
    df = await read_async("GGSII_Wave1_AT_V_1_1.sav")

Values are the codes; value labels are not applied. In .sav, values SPSS marks as missing are null.
"""
import asyncio
from pathlib import Path

import polars as pl
import pyreadstat

from . import ffs_conversion

FORMATS = (".parquet", ".dta", ".sav", ".csv", ".sps")  # also the preference when one file comes in several
LAZY = (".parquet", ".csv")  # polars scans these itself; the others are read whole, then made lazy


def _dat(sps):
    """The .dat of the same name, or the only .dat next to the .sps."""
    dats = [p for p in sps.parent.iterdir() if p.suffix.lower() == ".dat"]
    same = [p for p in dats if p.stem.lower() == sps.stem.lower()]
    if same or len(dats) == 1:
        return (same or dats)[0]
    raise ValueError(f"no .dat for {sps}")


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
    if s in (".dta", ".sav"):
        f = pyreadstat.read_dta if s == ".dta" else pyreadstat.read_sav
        try:
            return f(p, output_format="polars")[0]
        except (pyreadstat.ReadstatError, UnicodeDecodeError):  # some GGS files mix encodings: read as their .sav twins
            return f(p, output_format="polars", encoding="latin1")[0]
    if s == ".sps":
        return pl.from_pandas(ffs_conversion.read_ffs(p, _dat(p))[0])
    raise ValueError(f"{p.name}: cannot read {s or 'files without a type'}; choose from {', '.join(FORMATS)}")


async def read_async(path):
    """read(), without blocking the event loop: polars collects parquet and csv on its own threads."""
    if Path(path).suffix.lower() in LAZY:
        return await scan(path).collect_async()
    return await asyncio.to_thread(read, path)
