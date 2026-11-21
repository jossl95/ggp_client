"""SQLite catalogue: one row per dataset page (version), one row per download file."""
import sqlite3
from datetime import datetime, timezone

from . import parse

SCHEMA = """
CREATE TABLE IF NOT EXISTS dataset (
  id INTEGER PRIMARY KEY AUTOINCREMENT,   -- stable catalogue number, never reused
  slug TEXT NOT NULL UNIQUE,
  project TEXT NOT NULL, round INTEGER, series TEXT NOT NULL, wave TEXT NOT NULL,
  country TEXT NOT NULL, country_name TEXT, version TEXT,
  is_latest INTEGER NOT NULL, upcoming INTEGER NOT NULL,
  title TEXT, collected_from TEXT, collected_to TEXT,
  colectica_url TEXT, page_url TEXT NOT NULL,
  rel_dir TEXT NOT NULL, stem TEXT NOT NULL, synced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file (
  download_id INTEGER PRIMARY KEY,
  dataset_id INTEGER NOT NULL REFERENCES dataset(id),
  label TEXT, size_mb REAL, url TEXT NOT NULL,
  local_path TEXT,                          -- folder the archive was unpacked into
  fetched TEXT,                             -- what was kept so far: datafile types plus "meta"
  downloaded_at TEXT
);
"""


def connect(path):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    if "fetched" not in {r[1] for r in con.execute("PRAGMA table_info(file)")}:  # made by an older version
        con.execute("ALTER TABLE file ADD COLUMN fetched TEXT")
    return con


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def upsert(con, slug, d):
    """Insert or refresh a dataset (keeping its id) and its files (keeping local_path)."""
    p = parse.parse_slug(slug)
    version = d["version"] or p["version"]
    rel_dir, stem = parse.naming(p, slug, version)
    row = dict(
        slug=slug, project=p["project"], round=p["round"], series=p["series"], wave=p["wave"],
        country=p["country"], country_name=d["country_name"], version=version,
        is_latest=int(p["version"] is None), upcoming=int(p["upcoming"]), title=d["title"],
        collected_from=d["collected_from"], collected_to=d["collected_to"],
        colectica_url=d["colectica_url"], page_url=d.get("page_url") or parse.page_url(slug),
        rel_dir=rel_dir, stem=stem, synced_at=_now(),
    )
    cols = ", ".join(row)
    marks = ", ".join(f":{c}" for c in row)
    sets = ", ".join(f"{c}=excluded.{c}" for c in row if c != "slug")
    con.execute(f"INSERT INTO dataset({cols}) VALUES({marks}) ON CONFLICT(slug) DO UPDATE SET {sets}", row)
    ds_id = con.execute("SELECT id FROM dataset WHERE slug=?", (slug,)).fetchone()[0]
    for f in d["files"]:
        con.execute(
            "INSERT INTO file(download_id, dataset_id, label, size_mb, url) VALUES(?,?,?,?,?) "
            "ON CONFLICT(download_id) DO UPDATE SET dataset_id=excluded.dataset_id, "
            "label=excluded.label, size_mb=excluded.size_mb, url=excluded.url",
            (f["download_id"], ds_id, f["label"], f["size_mb"], f["url"]),
        )
    if d["files"]:  # a link replaced on the page; a page with no link at all keeps its old rows
        keep = [f["download_id"] for f in d["files"]]
        con.execute(f"DELETE FROM file WHERE dataset_id=? AND download_id NOT IN ({','.join('?' * len(keep))})",
                    (ds_id, *keep))


def mark_downloaded(con, download_id, rel_path, kept):
    """Record the folder and add `kept` (datafile types, "meta") to what this file has been unpacked for."""
    old = con.execute("SELECT fetched FROM file WHERE download_id=?", (download_id,)).fetchone()[0] or ""
    con.execute("UPDATE file SET local_path=?, fetched=?, downloaded_at=? WHERE download_id=?",
                (rel_path, " ".join(sorted(set(old.split()) | set(kept))), _now(), download_id))
    con.commit()


def parse_ids(spec):
    """'3-7,10' -> [(3, 7), (10, 10)]; a reversed range '7-3' is read as 3-7."""
    out = []
    for part in spec.split(","):
        lo, _, hi = part.strip().partition("-")
        out.append(tuple(sorted((int(lo), int(hi or lo)))))
    return out


def _round(v):
    return {"i": 1, "ii": 2}.get(str(v).lower()) or int(v)


def _wave(v):
    v = str(v).lower()
    return "w" + v if v.isdigit() else v


def select(con, project=None, round=None, wave=None, country=None, ids=None,
           all_versions=False, downloadable=False):
    """Rows (one per file; datasets without a file appear once) matching all given filters.

    project, round, wave and country each take one value, a list of values, or None for all.
    Values are case-insensitive and are the names used in the download folders: project "ggs",
    round 2 (or "ii"), wave "w1" (or 1), country "nl". A value that matches nothing raises ValueError.
    """
    where, args = [], []

    def isin(name, norm, vals):
        vals = [norm(v) for v in parse.many(vals)]
        if not vals:
            return
        known = {r[0] for r in con.execute(
            f"SELECT DISTINCT {name} FROM dataset WHERE {name} IS NOT NULL AND {name} != ''")}
        if known and (bad := set(vals) - known):  # a typo would otherwise silently select nothing
            raise ValueError(f"unknown {name} {sorted(map(str, bad))}; "
                             f"choose from {sorted(map(str, known))}")
        where.append(f"d.{name} IN ({','.join('?' * len(vals))})")
        args.extend(vals)

    isin("project", str.upper, project)
    isin("round", _round, round)
    isin("wave", _wave, wave)
    isin("country", str.upper, country)
    if ids:
        where.append("(" + " OR ".join("d.id BETWEEN ? AND ?" for _ in ids) + ")")
        args += [x for pair in ids for x in pair]
    if not all_versions:
        where.append("d.is_latest = 1")
    if downloadable:
        where.append("f.url IS NOT NULL")
    sql = ("SELECT d.*, f.download_id, f.label, f.size_mb, f.url, f.local_path, f.fetched, "
           "(SELECT COUNT(*) FROM file WHERE dataset_id = d.id) AS n_files "
           "FROM dataset d LEFT JOIN file f ON f.dataset_id = d.id")
    if where:
        sql += " WHERE " + " AND ".join(where)
    return con.execute(sql + " ORDER BY d.id, f.download_id", args).fetchall()
