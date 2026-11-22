"""Write the shared fixtures of the Python and R packages: python tests/fixtures/regenerate.py

The portal pages and FFS files come from the test module; the expected results are what this package produces. Both
packages test against these files (the R package keeps a copy in tests/testthat/fixtures, checked by its CI), so a
change of a rule shows up in both test suites. Review the diff of expected/ before committing it.
"""
import json
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[1]))

import test_ggp_client as t  # noqa: E402
from ggp_client import labels, parse, processing  # noqa: E402


def write(path, obj):
    path = HERE / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def main():
    pages = {"catalog": t.CATALOG, "ggs2-at-wave1": t.AT, "ggs2-at-wave1-v1-0": t.AT10, "ggs2-nl-wave1": t.NL,
             "upcoming-ggs2-fr-wave1": t.FR, "ffs-us-thefamilyandfertilitysurvey": t.FFS, "form": t.FORM}
    for name, html in pages.items():
        (HERE / "portal").mkdir(exist_ok=True)
        (HERE / "portal" / f"{name}.html").write_text(html, encoding="utf-8")

    slugs = ["ggs2-at-wave1", "ggs2-at-wave1-v1-0", "ggs2-se-wave1register", "upcoming-ggs2-fr-wave1",
             "ffs-us-thefamilyandfertilitysurvey", "hh-harmonized-histories", "other-nkps", "weird"]
    parsed = {}
    for s in slugs:
        p = parse.parse_slug(s)
        rel_dir, stem = parse.naming(p, s, p["version"])
        parsed[s] = dict(p, rel_dir=rel_dir, stem=stem)
    write("expected/parse.json", dict(
        slugs=parsed,
        catalog=parse.parse_catalog(t.CATALOG),
        datasets={s: parse.parse_dataset(pages[s], s) for s in pages if s not in ("catalog", "form")},
        access=parse.parse_access(t.FORM)))

    case = []
    for name, var, values in [
            ("AT", "Country", {"203": "Czechia", "1": "East Austria", "3": "elementary school", "-1": "Don't know"}),
            ("HR", "Country", {"203": "czechia ", "1": "Pannonian Croatia", "4": "Elementary school", "-1": "Don't know"}),
            ("GB", "Land", {"203": "Czech Republic", "1": "East Midlands", "-1": "don't know"}),
            ("GB_RU", "Страна", {"203": "Чехия", "-1": "Не знаю"})]:  # GB's second file: no vote
        case.append(dict(country=name[:2], src=f"ggs/{name[:2].lower()}/{name}.dta",
                         meta=dict(column_labels={"cc": var, "gone": "x"}, value_labels={"cc": values})))
    case.append(dict(country="NL", src="NL.csv", meta=None))  # csv: no meta.json, skipped
    write("labels/case.json", dict(files=case, columns=["cc"]))
    with tempfile.TemporaryDirectory() as tmp:  # the input files as merged_meta reads them: a meta.json next to each
        files = []
        for i, c in enumerate(case):
            f = Path(tmp) / f"{i}.parquet"
            if c["meta"]:
                labels.meta_path(f).write_text(json.dumps(c["meta"]), encoding="utf-8")
            files.append((f, c["country"], c["src"]))
        write("expected/labels.json", labels.merged_meta(files, {"cc"}))

    (HERE / "ffs").mkdir(exist_ok=True)
    (HERE / "ffs" / "USA2.SPS").write_bytes(t.FFS_SPS)
    (HERE / "ffs" / "USA2.DAT").write_bytes(t.FFS_DAT)
    df, meta = processing.read_ffs(HERE / "ffs" / "USA2.SPS", HERE / "ffs" / "USA2.DAT")
    write("expected/ffs.json", dict(
        columns=list(df.columns),
        data={c: [None if v != v else v for v in df[c].tolist()] for c in df.columns},
        column_labels=meta["column_labels"],
        value_labels={k: {str(c): v for c, v in d.items()} for k, d in meta["variable_value_labels"].items()},
        missing_ranges=meta["missing_ranges"]))


if __name__ == "__main__":
    main()
