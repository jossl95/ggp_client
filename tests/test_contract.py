"""The shared fixtures (tests/fixtures): the R package tests against the same files, so the two stay in step.

After a change of a rule on purpose, run `python tests/fixtures/regenerate.py` and review the diff of expected/.
"""
import json
import shutil
from pathlib import Path

from ggp_client import labels, parse, processing

FIX = Path(__file__).parent / "fixtures"


def load(path):
    return json.loads((FIX / path).read_text(encoding="utf-8"))


def norm(x):  # what JSON makes of it: tuples are lists, int keys are text
    return json.loads(json.dumps(x, sort_keys=True))


def test_parse_matches_the_fixtures():
    want = load("expected/parse.json")
    html = lambda n: (FIX / "portal" / f"{n}.html").read_text(encoding="utf-8")
    got_slugs = {}
    for s in want["slugs"]:
        p = parse.parse_slug(s)
        rel_dir, stem = parse.naming(p, s, p["version"])
        got_slugs[s] = dict(p, rel_dir=rel_dir, stem=stem)
    assert norm(got_slugs) == want["slugs"]
    assert parse.parse_catalog(html("catalog")) == want["catalog"]
    assert norm({s: parse.parse_dataset(html(s), s) for s in want["datasets"]}) == want["datasets"]
    assert norm(parse.parse_access(html("form"))) == want["access"]


def test_labels_match_the_fixtures(tmp_path):
    case, files = load("labels/case.json"), []
    for i, c in enumerate(case["files"]):
        f = tmp_path / f"{i}.parquet"
        if c["meta"]:
            labels.meta_path(f).write_text(json.dumps(c["meta"]), encoding="utf-8")
        files.append((f, c["country"], c["src"]))
    assert norm(labels.merged_meta(files, set(case["columns"]))) == load("expected/labels.json")


def test_ffs_matches_the_fixtures(tmp_path):
    for n in ("USA2.SPS", "USA2.DAT"):
        shutil.copy(FIX / "ffs" / n, tmp_path / n)
    df, meta = processing.read_ffs(tmp_path / "USA2.SPS", tmp_path / "USA2.DAT")
    want = load("expected/ffs.json")
    assert list(df.columns) == want["columns"]
    assert {c: [None if v != v else v for v in df[c].tolist()] for c in df.columns} == want["data"]
    assert norm(meta["column_labels"]) == want["column_labels"]
    assert norm({k: {str(c): v for c, v in d.items()} for k, d in meta["variable_value_labels"].items()}) == want["value_labels"]
    assert norm(meta["missing_ranges"]) == want["missing_ranges"]
