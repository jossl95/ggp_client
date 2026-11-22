"""Variable and value labels: put them on the data, and harmonize them over the files of a consolidated dataset.

    from ggp_client import labels, read
    path = "data/download/consolidated/ggs-ii_w1.parquet"
    m = labels.meta(path)                         # its meta.json
    df = labels.label(read.read(path), m)         # codes replaced by their labels: 1 -> "Male"
    labels.variable_labels(df, m)                 # {"dem01": "Gender", ...}

Each file has a .meta.json next to its parquet (see processing.preprocess). merged_meta() combines them; its rules
are the contract both the Python and the R package follow (docs/contract.qmd).
"""
import json
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl

from . import read


def meta_path(parquet):
    return Path(parquet).with_suffix(".meta.json")


def meta(path):
    """The labels of a data file: the .meta.json next to a parquet, or those of a dta or sav (read.meta).

    None for a file without labels (csv, or a parquet without meta.json).
    """
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        return json.loads(meta_path(p).read_text(encoding="utf-8")) if meta_path(p).exists() else None
    return read.meta(p)


def _text(v):
    return str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)  # 1.0 shows as "1"


def _labelled(s, codes):
    """The polars Series s with its codes replaced by their labels, as an Enum in code order."""
    numeric = s.dtype.is_numeric()
    found = {}
    for k, v in codes.items():
        try:
            found[float(k) if numeric else str(k)] = v
        except ValueError:  # Stata's extended missing codes (.a, .b) are not values of a numeric column
            pass
    values = s.drop_nulls().unique().sort().to_list()
    new = [found.get(float(v) if numeric else str(v), _text(v)) for v in values]  # a code without a label: itself
    enum = pl.Enum(list(dict.fromkeys([found[k] for k in sorted(found)] + new)))  # two codes may share a label
    if not values:
        return pl.Series(s.name, [None] * len(s), enum)
    return s.replace_strict(values, new, return_dtype=enum)


def label(df, meta, columns=None):
    """df with the value labels of meta on it: every labelled column holds its labels instead of its codes.

    The column becomes categorical (a polars Enum, a pandas Categorical) with the labels in the order of their codes;
    a code without a label shows as itself ("7"), and null stays null. Unlabelled columns are left alone.
    df: a polars or pandas DataFrame; a pandas one also gets the variable labels in df.attrs["column_labels"].
    meta: from meta(), read.meta() or a .meta.json. columns: only these (default: all labelled columns).
    """
    pandas = not isinstance(df, pl.DataFrame)
    out = pl.from_pandas(df) if pandas else df
    value_labels = meta.get("value_labels") or meta.get("variable_value_labels") or {}
    todo = [c for c in out.columns if value_labels.get(c) and (columns is None or c in columns)]
    out = out.with_columns(_labelled(out[c], value_labels[c]) for c in todo)
    if not pandas:
        return out
    res = out.to_pandas()
    res.attrs["column_labels"] = variable_labels(res, meta)
    return res


def variable_labels(df, meta):
    """{column: variable label} for the columns of df that have one in meta."""
    names = meta.get("column_labels") or {}
    return {c: names[c] for c in df.columns if names.get(c)}


def _norm(label):
    return " ".join(label.split()).casefold()  # "Czech  Republic" and "czech republic" are one label


def _pick(spellings):
    """The label most files use (by its normalized form), in its most common spelling; ties go to the first file."""
    counts = Counter(_norm(s) for s in spellings)
    top = counts.most_common(1)[0][0]
    return Counter(s for s in spellings if _norm(s) == top).most_common(1)[0][0], counts


def _votes(found):
    """[label] with the first label of each voter only, from [(label, voter)]."""
    first = {}
    for label, voter in found:
        first.setdefault(voter, label)
    return list(first.values())


def merged_meta(files, columns):
    """The harmonized labels of a consolidated file, from its input files' meta.json, for the columns it kept.

    Each country has one vote, that of its first file: Belarus ships GGS-II wave 1 in English and in Russian.
    Variable label: the one most countries use. Value labels, per variable: a code gets the label more than half of
    the countries that label it use; with no such majority, all their labels joined by " / " (most used first).
    So every value has exactly one label; several values may share one (2102 and 3103 "Elementary school").
    Codes are never changed. original_labels keeps, for every code labelled differently in any file, each
    wording and the source files (under download/raw, or download/processed for FFS) that use it.
    """
    names, labels = defaultdict(list), defaultdict(lambda: defaultdict(list))  # labels[var][code] = [(label, src, voter)]
    for f, country, src in files:
        voter = country or src  # Harmonized Histories and Additional Datasets have no country: a vote per file
        if not meta_path(f).exists():  # csv sources have none
            continue
        m = json.loads(meta_path(f).read_text(encoding="utf-8"))
        for c, label in m["column_labels"].items():
            if label and c in columns:
                names[c].append((label, voter))
        for c, d in m["value_labels"].items():
            if c in columns:
                for k, v in d.items():
                    if v:
                        labels[c][k].append((v, src, voter))
    out = dict(column_labels={c: _pick(_votes(v))[0] for c, v in names.items()}, value_labels={}, original_labels={})
    for c, codes in labels.items():
        d = {}
        for k, found in codes.items():
            votes = _votes([(v, voter) for v, _, voter in found])
            label, counts = _pick(votes)
            if len(counts) > 1 and counts[_norm(label)] * 2 <= len(votes):  # no majority: say what each country means
                label = " / ".join(_pick([v for v in votes if _norm(v) == n])[0] for n, _ in counts.most_common())
            if len({_norm(v) for v, *_ in found}) > 1:
                orig = defaultdict(list)
                for v, f, _ in found:
                    orig[v].append(f)
                out["original_labels"].setdefault(c, {})[k] = dict(orig)
            d[k] = label
        out["value_labels"][c] = d
    return out
