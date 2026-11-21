"""Read an FFS dataset (fixed-width .DAT described by SPSS syntax in .SPS) and save it as sav, dta or csv.

Each FFS archive holds COUNTRY.DAT (the data, `records` lines per respondent), COUNTRY.SPS (SPSS syntax: DATA LIST
with the record and columns of every variable, VARIABLE LABELS, VALUE LABELS, MISSING VALUE) and COUNTRY.TBL
(the published frequency tables, to check results against; not needed to read the data).

    python -m ggp_client.ffs_conversion AUSTRIA.SPS --ext sav dta     # writes AUSTRIA.sav and AUSTRIA.dta

Variable names keep their SPSS spelling except `$`, which Stata does not allow: IND01$01 becomes IND01_01.
Missing value codes (e.g. 98) stay in the data; sav also marks them as missing, as SPSS would.
csv has the codes only: it cannot hold labels.
"""
import argparse
import logging
import os
import re
from pathlib import Path

import pandas as pd
import pyreadstat

log = logging.getLogger("ggp_client")
FORMATS = ("sav", "dta", "csv")
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


def convert(folder, exts, stem, force=False):
    """Convert every FFS dataset unpacked in folder to `exts`, saved in folder as <stem>.<ext>.

    A dataset is a .sps with the .dat of the same name (or the only .dat next to it). A country shipped
    without .sps but with .sav files (Estonia) is converted from those. Several datasets in one folder
    get the source name added: <stem>_<name>.<ext>. Returns the number of matching output files.
    """
    folder = Path(folder)
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
        name = stem if len(sources) == 1 else f"{stem}_{src.stem.lower()}"
        targets = [folder / f"{name}.{e}" for e in exts]
        todo = [t for t in targets if force or not t.exists()]
        if todo:
            df, meta = read()
            for t in todo:
                write(df, meta, t, force)
        n += len(targets)
    return n


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m ggp_client.ffs_conversion", description=__doc__.split("\n")[0])
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
