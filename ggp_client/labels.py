"""Harmonized labels: the variable and value labels of the files of a consolidated dataset, merged into one set.

Each file has a .meta.json next to its parquet (see processing.preprocess). merged_meta() combines them, the rules
below are the contract both the Python and the R package follow (docs/contract.qmd).
"""
import json
from collections import Counter, defaultdict


def meta_path(parquet):
    return parquet.with_suffix(".meta.json")


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
