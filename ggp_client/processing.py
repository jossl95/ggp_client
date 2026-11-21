"""Processing downloaded data: FFS conversion (to labelled dta/sav) and preprocessing (to parquet + meta.json).

FFS conversion. Each FFS archive holds COUNTRY.DAT (the data, `records` lines per respondent), COUNTRY.SPS (SPSS
syntax: DATA LIST with the record and columns of every variable, VARIABLE LABELS, VALUE LABELS, MISSING VALUE) and
COUNTRY.TBL (the published frequency tables, to check results against; not needed to read the data).

    python -m ggp_client.processing AUSTRIA.SPS --ext sav dta     # writes AUSTRIA.sav and AUSTRIA.dta

Every $ in a variable name becomes _ (Stata does not allow $): IND01$01 becomes IND01_01. Downloads also
rewrite the raw .sps that way (see rewrite_sps), so syntax, converted files and documentation use the same names.
Missing value codes (e.g. 98) stay in the data; sav also marks them as missing, as SPSS would.
csv has the codes only: it cannot hold labels.

Preprocessing saves downloaded datafiles as parquet under data_dir/download/processed, with columns that say
where each row came from. One parquet per data file, at the same path as in download/raw, plus a .meta.json with
its labels (dta and sav):
download/raw/ggs-ii/w1/at/ggs2_w1_at_v1.1/x/GGSII_Wave1_AT_V_1_1.dta ->
    download/processed/ggs-ii/w1/at/ggs2_w1_at_v1.1/x/GGSII_Wave1_AT_V_1_1.parquet and GGSII_Wave1_AT_V_1_1.meta.json
A file that comes in several types (dta, sav and csv of one dataset) is read once, in the order of read.FORMATS.
FFS is read from its converted_data.dta/.sav, which the download puts in download/processed/ffs/<cc>/ already;
a country without one is read from the raw .sps/.dat.
Added columns: ggp_project, ggp_round, ggp_series, ggp_wave, ggp_country, ggp_version, ggp_file.
An existing parquet (and meta.json) is kept unless force.
"""
import argparse
import asyncio
import collections
import json
import logging
import os
import re
import tempfile
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import pandas as pd
import polars as pl
import pyreadstat

from . import extract, read

log = logging.getLogger("ggp_client")
FORMATS = ("sav", "dta", "csv")
CONVERTED = "converted_data"
RAW = {"dat", "sps", "tbl", "sav"}  # always kept; .sav because Estonia ships those instead of a .sps
DATA_FILES = {".dat", ".sps", ".tbl", ".sav", ".dta", ".csv"}  # renamed after the dataset; documents keep their names
SECTION = re.compile(r"^(DATA LIST|VARIABLE LABELS?|VALUE LABELS?|MISSING VALUES?)\b(.*)$", re.I)
# a label runs to the last quote on its line: some files have stray quotes where umlauts were ("Nieder"sterreich")
TOKEN = re.compile(r'"[^\n]*"|\'[^\n]*\'|\([^)]*\)|/|[^\s/()"\']+')


def _quoted(t):
    return t[:1] in "\"'"


def _unquote(t):
    return t[1:-1].replace(t[0] * 2, t[0]).strip()


def _number(t):
    try:
        v = float(t)
    except ValueError:
        return None
    return int(v) if v.is_integer() else v


def _name(n):
    return re.sub(r"[^A-Za-z0-9_]", "_", n)


def _sections(text):
    """{"DATA": (header rest, body), "VARIABLE": ..., "VALUE": ..., "MISSING": ...}; a command ends with a period."""
    out, cur = {}, None
    for line in text.splitlines():
        m = SECTION.match(line)
        if m:
            cur = m[1].split()[0].upper()
            out[cur] = [m[2], []]
            continue
        if cur is None:
            continue
        body = line.rstrip()
        end = body.endswith(".")  # labels end in a quote, so a final period is the command terminator
        out[cur][1].append(body[:-1] if end else body)
        if end:
            cur = None
    return {k: (head, TOKEN.findall("\n".join(lines))) for k, (head, lines) in out.items()}


def _groups(tokens):
    """Split on "/": the separator between variable groups in SPSS label commands."""
    group = []
    for t in tokens:
        if t == "/":
            if group:
                yield group
            group = []
        else:
            group.append(t)
    if group:
        yield group


def _missing(spec):
    """'(98)', '(8,9)', '(1 THRU 3)' -> pyreadstat missing_ranges entries."""
    toks = spec.strip("()").replace(",", " ").split()
    out, i = [], 0
    while i < len(toks):
        if i + 2 < len(toks) and toks[i + 1].upper() == "THRU":
            lo, hi = (_number(t) for t in (toks[i], toks[i + 2]))
            out.append({"lo": -1e300 if lo is None else lo, "hi": 1e300 if hi is None else hi})  # LO / HI
            i += 3
        else:
            out.append(_number(toks[i]))
            i += 1
    return [v for v in out if v is not None]


def parse_sps(text):
    """Layout and labels from FFS SPSS syntax.

    Returns (records, layout [(name, record, first col, last col, is_text)], variable labels, value labels, missing).
    """
    s = _sections(text)
    head, toks = s["DATA"]
    records = int(re.search(r"records\s*=\s*(\d+)", head, re.I)[1])
    layout, rec, i = [], 1, 0
    while i < len(toks):
        if toks[i] == "/":
            rec, i = int(toks[i + 1]), i + 2
            continue
        name, cols = toks[i], toks[i + 1]
        fmt = toks[i + 2].upper() if i + 2 < len(toks) and toks[i + 2].startswith("(") else None
        if not re.fullmatch(r"\d+(-\d+)?", cols) or fmt not in (None, "(A)"):
            raise ValueError(f"DATA LIST: unsupported entry {name} {cols} {fmt or ''}")
        a, _, b = cols.partition("-")
        layout.append((name, rec, int(a), int(b or a), fmt == "(A)"))
        i += 3 if fmt else 2

    var_labels = {}
    for g in _groups(s.get("VARIABLE", ("", []))[1]):
        names = []
        for t in g:
            if _quoted(t):
                var_labels.update({n: _unquote(t) for n in names})
                names = []
            else:
                names.append(t)

    value_labels = {}
    for g in _groups(s.get("VALUE", ("", []))[1]):
        k = next((j for j, t in enumerate(g) if _number(t) is not None), len(g))
        labels = {_number(v): _unquote(lab) for v, lab in zip(g[k::2], g[k + 1::2]) if _quoted(lab)}
        for n in g[:k]:
            value_labels.setdefault(n, {}).update(labels)

    missing = {}
    for g in _groups(s.get("MISSING", ("", []))[1]):
        names = []
        for t in g:
            if t.startswith("("):
                missing.update({n: _missing(t) for n in names})
                names = []
            else:
                names.append(t)
    return records, layout, var_labels, value_labels, missing


def read_ffs(sps, dat):
    """The data frame, with column labels, value labels and missing values as pyreadstat takes them."""
    records, layout, var_labels, value_labels, missing = parse_sps(Path(sps).read_text(encoding="latin-1"))
    lines = Path(dat).read_text(encoding="latin-1").splitlines()
    if len(lines) % records:
        raise ValueError(f"{dat}: {len(lines)} lines is not a multiple of {records} records per case")
    n = len(lines) // records
    rows = [lines[c * records:(c + 1) * records] for c in range(n)]
    cols, bad = {}, 0
    for name, rec, a, b, text in layout:
        s = pd.Series([r[rec - 1][a - 1:b].strip() or None for r in rows], dtype="object")
        if not text:  # like SPSS: a cell that is not a number (the files use * for overflow) is missing
            num = pd.to_numeric(s, errors="coerce")
            bad += int((num.isna() & s.notna()).sum())
            s = num
        cols[_name(name)] = s
    if bad:
        log.warning("%s: %d cells were not numbers (such as *) and are missing, as SPSS reads them", dat, bad)
    df = pd.DataFrame(cols)
    if len(df.columns) != len(layout):
        raise ValueError(f"{sps}: two variable names collide once $ is replaced")
    known = {n: _name(n) for n, *_ in layout}
    numeric = {_name(n) for n, *rest in layout if not rest[-1]}
    meta = dict(
        column_labels={known[n]: lab[:80] for n, lab in var_labels.items() if n in known},  # Stata allows 80 chars
        variable_value_labels={known[n]: v for n, v in value_labels.items() if n in known and v},
        missing_ranges={known[n]: v for n, v in missing.items() if n in known and v and known[n] in numeric},
    )
    return df, meta


def write(df, meta, target, force=False):
    """Save as target's type (sav, dta, csv) via a temporary name; an existing file is kept unless force.

    Returns True when the file was written.
    """
    target = Path(target)
    if target.exists() and not force:
        return False
    kind = target.suffix.lstrip(".").lower()
    part = target.with_name(target.stem + ".part" + target.suffix)  # pyreadstat wants the real extension
    labels = [meta["column_labels"].get(c) for c in df.columns]
    if kind == "sav":
        pyreadstat.write_sav(df, part, column_labels=labels, variable_value_labels=meta["variable_value_labels"],
                             missing_ranges=meta.get("missing_ranges") or None)
    elif kind == "dta":
        # Stata labels whole numbers only: drop labels on columns holding decimals
        vvl = {c: {int(k): v for k, v in lab.items() if float(k).is_integer()}
               for c, lab in meta["variable_value_labels"].items()
               if pd.api.types.is_numeric_dtype(df[c]) and (df[c].dropna() % 1 == 0).all()}
        pyreadstat.write_dta(df, part, column_labels=labels, variable_value_labels=vvl)
    elif kind == "csv":
        df.to_csv(part, index=False)
    else:
        raise ValueError(f"unknown format {kind!r}: choose from {', '.join(FORMATS)}")
    os.replace(part, target)
    return True


def convert(folder, exts, name=CONVERTED, force=False, out_dir=None):
    """Convert every FFS dataset unpacked in folder to `exts`, saved in out_dir (default: folder) as <name>.<ext>.

    A dataset is a .sps with the .dat of the same name (or the only .dat next to it). A country shipped
    without .sps but with .sav files (Estonia) is converted from those. Several datasets in one folder
    get the source name added: <name>_<source>.<ext>. Returns the number of matching output files.
    """
    folder = Path(folder)
    out = Path(out_dir or folder)
    files = sorted(p for p in folder.rglob("*") if p.is_file() and ".part" not in p.name)
    by_suffix = lambda s: [p for p in files if p.suffix.lower() == s]
    sources = []
    for sps in by_suffix(".sps"):
        dats = [d for d in by_suffix(".dat") if d.parent == sps.parent]
        dat = next((d for d in dats if d.stem.lower() == sps.stem.lower()), dats[0] if len(dats) == 1 else None)
        if dat is None:
            raise ValueError(f"no .dat for {sps.name}")
        sources.append((sps, lambda sps=sps, dat=dat: read_ffs(sps, dat)))
    if not sources:
        for sav in by_suffix(".sav"):
            def read(sav=sav):
                df, m = pyreadstat.read_sav(sav, user_missing=True)
                ren = lambda d: {_name(k): v for k, v in d.items()}
                return df.rename(columns=_name), dict(
                    column_labels=ren(m.column_names_to_labels), variable_value_labels=ren(m.variable_value_labels),
                    missing_ranges=ren(m.missing_ranges))
            sources.append((sav, read))
    if not sources:
        raise ValueError("no .sps (or .sav) to convert")
    n = 0
    for src, read in sources:
        base = name if len(sources) == 1 else f"{name}_{src.stem.lower()}"
        targets = [out / f"{base}.{e}" for e in exts]
        todo = [t for t in targets if force or not t.exists()]
        if todo:
            df, meta = read()
            for t in todo:
                write(df, meta, t, force)
        n += len(targets)
    return n


def rewrite_sps(text, dat_name=None):
    """The .sps with every $ in a variable name as _, and FILE= pointing at dat_name (the .dat next to it).

    Quoted text (labels, the FILE path) is left alone, so a $ in a label stays.
    """
    text = re.sub(r'("[^\n]*"|\'[^\n]*\')|\$', lambda m: m[1] or "_", text)
    if dat_name:  # the original points at C:\ISSA\...\COUNTRY.DAT; the .dat is renamed on download
        text = re.sub(r"(?i)(\bFILE\s*=\s*)(['\"])[^'\"\n]*\2", lambda m: f"{m[1]}'{dat_name}'", text, count=1)
    return text


def unpack(archive, filename, dest, stem, ext, metadata, convert_to=(), force=False, convert_dir=None):
    """Unpack an FFS download into dest, the country folder (one dataset per country: no folder of its own).

    Raw files are named after the dataset: AUSTRIA.DAT becomes <stem>.dat, and .sps and .tbl likewise; when the
    archive holds several of one type (Estonia) the original name is added: <stem>_est_f.dat. The .sps is
    rewritten: $ in names becomes _, FILE= names the renamed .dat; in the .tbl a $ inside a name (IND01$01)
    becomes _ too. Documents (metadata) keep their names.
    With convert_to the data is also saved as converted_data.<ext>, in convert_dir (default: dest).
    Existing files are kept unless force. Returns the number of matching files.
    """
    # shortcut: two versions of one country would share converted_data.*; the portal has one per country
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=dest.parent, prefix=".unpack-") as tmp:
        tmp = Path(tmp)
        n = extract.unpack_file(archive, filename, tmp, set(ext) | RAW, metadata, force=True)
        if n and convert_to:
            Path(convert_dir or dest).mkdir(parents=True, exist_ok=True)
            n += convert(tmp, convert_to, CONVERTED, force, out_dir=Path(convert_dir or dest))
        files = sorted(p for p in tmp.rglob("*") if p.is_file())
        count = collections.Counter(p.suffix.lower() for p in files)
        for sps in (p for p in files if p.suffix.lower() == ".sps"):
            dat = f"{stem}.dat" if count[".dat"] == 1 else None  # with several .dat files: which one is unknown
            sps.write_bytes(rewrite_sps(sps.read_bytes().decode("latin-1"), dat).encode("latin-1"))  # keeps CRLF
        for tbl in (p for p in files if p.suffix.lower() == ".tbl"):  # report text: only a $ between name characters
            tbl.write_bytes(re.sub(rb"(?<=[A-Za-z0-9])\$(?=[A-Za-z0-9])", b"_", tbl.read_bytes()))
        for p in files:
            rel, s = p.relative_to(tmp), p.suffix.lower()
            if s in DATA_FILES:
                rel = rel.with_name(f"{stem}{s}" if count[s] == 1 else f"{stem}_{p.stem.lower()}{s}")
            target = dest / rel
            if force or not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(p, target)
    return n


def datafiles(folder, types=read.FORMATS):
    """One file per dataset file in folder: the preferred type of each name.

    FFS: the converted files (labelled) when there are any, else the raw ones; never both, as they hold the same data.
    """
    files = [p for p in sorted(Path(folder).rglob("*"))
             if p.suffix.lower() in types and p.is_file() and ".part" not in p.name]
    converted = [p for p in files if p.name.lower().startswith(CONVERTED)]
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
    conv = [p for p in datafiles(out / lp, read.LABELLED) if p.name.lower().startswith(CONVERTED)]
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


def to_parquet(src, rel, target, r):
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
            to_parquet(src, rel, target, r)
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
            await loop.run_in_executor(ex, to_parquet, src, rel, target, dict(r))  # a sqlite3.Row cannot go to a process
            log.info("preprocessed %s", target.relative_to(out))
        except Exception as e:  # the worker's log lines would not reach this log: log here
            msg = ("a worker process died: out of memory (lower concurrency), or a script without "
                   "`if __name__ == \"__main__\":`") if isinstance(e, BrokenProcessPool) else str(e) or type(e).__name__
            failed.append((str(rel), msg))
            log.error("preprocess failed %s: %s", *failed[-1])

    with read.pool(concurrency) as ex:  # also bounds memory: dta and sav are read whole
        await asyncio.gather(*(one(*j) for j in todo))
    return len(todo) - len(failed), skipped, failed


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m ggp_client.processing",
                                 description="Convert an FFS dataset (.SPS and its .DAT) to sav, dta or csv, with labels.")
    ap.add_argument("sps", help="the .SPS file; the .DAT of the same name is read")
    ap.add_argument("--dat", help="the .DAT file, if its name differs")
    ap.add_argument("--ext", nargs="+", default=["sav"], choices=FORMATS)
    ap.add_argument("--force", action="store_true", help="overwrite existing files")
    a = ap.parse_args(argv)
    sps = Path(a.sps)
    dat = Path(a.dat) if a.dat else next(p for p in sps.parent.iterdir()
                                         if p.suffix.lower() == ".dat" and p.stem.lower() == sps.stem.lower())
    df, meta = read_ffs(sps, dat)
    for e in a.ext:
        t = sps.with_suffix("." + e)
        print(f"{t}: {'written' if write(df, meta, t, a.force) else 'exists, kept (use --force)'}, "
              f"{len(df)} cases, {len(df.columns)} variables")


if __name__ == "__main__":
    main()
