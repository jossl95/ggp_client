"""Offline checks. The HTML below is synthetic, modelled on the text of the live catalogue and
dataset pages (not raw markup from the site), served through httpx.MockTransport."""
import asyncio
import io
import os
import zipfile

import httpx
import py7zr
import pytest

from ggp_client import db, extract, parse
from ggp_client.client import LoginError, ggpClient

CATALOG = """<h2>GGS-II</h2>
<a href="https://www.ggp-i.org/dataset/ggs2-at-wave1/">Wave 1</a>
<a href="/dataset/ggs2-at-wave1/">View</a>
<a href="/dataset/ggs2-nl-wave1/">Explore Dataset</a>
<a href="/dataset/upcoming-ggs2-fr-wave1/">Wave 1</a>
<h2>FFS</h2><a href="/dataset/ffs-us-thefamilyandfertilitysurvey/">x</a>
<a href="/dataset/page/4/">older</a><a href="/dataset/">archive</a>
<a href="https://evil.example/dataset/ggs2-xx-wave1/">offsite</a>"""

AT = """<h1>Austria – Wave 1 v1.1</h1>
<a href="/form/">Login</a>
<a href="https://ggp.colectica.org/item/int.ggp/c7fc">Explore Dataset on Colectica</a>
<p>Data Collection Date: Monday, October 31, 2022 - Sunday, March 12, 2023</p>
<p>Version: 1.1</p>
<div>Downloads: <span>Austria - Wave 1 (version 1.1) v1.1, 15.3 MB</span>
 <a href="https://ggp.colectica.org/Item/int.ggp/dd79">Browse Dataset</a>
 <a href="https://www.ggp-i.org/form/download/95/">Login to Download</a></div>
<p>Other versions: <a href="/dataset/ggs2-at-wave1-v1-0/">View version 1.0</a>
<a href="/dataset/ggs2-at-wave1-v1-1/">View version 1.1</a></p>"""

AT10 = """<h1>Austria – Wave 1 v1.0</h1><p>Version: 1.0</p>
<div><span>Austria - Wave 1 (version 1.0) v1.0, 14.9 MB</span>
 <a href="/form/download/41/">Login to Download</a></div>
<a href="/dataset/ggs2-at-wave1/">View version 1.1</a>"""

NL = """<h1>Netherlands – Wave 1 v2.0</h1><p>Version: 2.0</p>
<div><span>Netherlands - Wave 1, 1.2 GB</span><a href="/form/download/96/">Login to Download</a></div>"""

FR = "<h1>France – Wave 1</h1><p>Coming soon</p>"

FFS = """<h1>USA – The Family and Fertility Survey</h1><p>Version: 1.0</p>
<div><span>USA FFS, 800 KB</span><a href="/form/download/7/">Login to Download</a></div>"""

PAGES = {
    "/data-catalog/": CATALOG,
    "/dataset/ggs2-at-wave1/": AT,
    "/dataset/ggs2-at-wave1-v1-1/": AT,  # the live site also gives the latest version its own page
    "/dataset/ggs2-at-wave1-v1-0/": AT10,
    "/dataset/ggs2-nl-wave1/": NL,
    "/dataset/upcoming-ggs2-fr-wave1/": FR,
    "/dataset/ffs-us-thefamilyandfertilitysurvey/": FFS,
}

def row(label, did):
    return f'<li><div>{label}</div><div><a href="/form/download/{did}/" target="_blank">Download</a></div></li>'


# the logged-in data access page: hidden sections, each list twice (all, then in columns), as on the live site
HH = f"<h3>Harmonized Histories </h3><ul>{row('Part 1', 1)}{row('Part 2', 84)}</ul>"
EXTRA = f"<h3>NKPS</h3><ul>{row('NKPS', 61)}</ul>"
FORM = f"""<h1>Data Access</h1><form action="/form/admin/logout/?next=/form/"><button>Logout</button></form>
<section><h2>Generations and Gender Survey - Round 2 (GGS-II)</h2><h3>Austria</h3><ul>{row('Wave 1', 95)}</ul></section>
<section x-data="{{ open: false }}"><h2>Harmonized Histories</h2><button>Show 2 datasets</button>
 <div x-show="open"><div>{HH}</div><div>{HH}</div></div></section>
<section x-data="{{ open: false }}"><h2>Additional Datasets</h2><button>Show 1 datasets</button>
 <div x-show="open"><div>{EXTRA}</div><div><div>{EXTRA}</div></div></div></section>"""
TOKEN = "https://www.ggp-i.org/form/accounts/login/email/s3cr3t/"


def make_zip(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in members.items():
            z.writestr(name, data)
    return buf.getvalue()


# datafiles directly in the zip, and a zip that holds another zip
AT_ZIP = make_zip({"Data/AT_w1.dta": b"dta", "Data/AT_w1.sav": b"sav", "Docs/Codebook.pdf": b"pdf"})
FFS_ZIP = make_zip({
    "FFS/Waves.zip": make_zip({"Wave1/us1.dta": b"dta", "Wave1/us1.sav": b"sav", "notes.txt": b"n"}),
    "readme.txt": b"r",
})


def make_7z(members):
    buf = io.BytesIO()
    with py7zr.SevenZipFile(buf, "w") as z:
        for name, data in members.items():
            z.writestr(data, name)
    return buf.getvalue()


def tree(d):
    return sorted(f.relative_to(d).as_posix() for f in d.rglob("*") if f.is_file())


def handler(req):
    p = req.url.path
    if p in PAGES:
        return httpx.Response(200, text=PAGES[p])
    if p == "/form/accounts/login/email/s3cr3t/":
        return httpx.Response(200, text="ok", headers={"set-cookie": "sessionid=s1; Path=/"})
    if p == "/form/":
        ok = "sessionid=s1" in req.headers.get("cookie", "")
        return httpx.Response(200, text=FORM if ok else "we'll send you a secure login link")
    if p in ("/form/download/1/", "/form/download/84/", "/form/download/61/"):
        return httpx.Response(200, content=make_zip({"x.dta": b"d"}), headers={"content-type": "application/zip"})
    if p == "/form/download/95/":
        return httpx.Response(200, content=AT_ZIP, headers={
            "content-type": "application/zip", "content-disposition": 'attachment; filename="AT_w1.zip"'})
    if p == "/form/download/7/":
        return httpx.Response(200, content=FFS_ZIP, headers={"content-type": "application/zip"})
    if p == "/form/download/96/":
        return httpx.Response(200, text="<html>sign the agreement</html>", headers={"content-type": "text/html"})
    return httpx.Response(404)


def portal(tmp_path, url=None, **kw):
    return ggpClient(tmp_path, url, transport=httpx.MockTransport(handler), **kw)


def test_slug_and_naming():
    p = parse.parse_slug("ggs2-at-wave1")
    assert (p["project"], p["round"], p["series"], p["wave"], p["country"]) == ("GGS", 2, "GGS-II", "w1", "AT")
    assert parse.naming(p, "ggs2-at-wave1", "1.1") == ("ggs-ii/w1/at", "ggs2_w1_at_v1.1")
    assert parse.parse_slug("ggs2-at-wave1-v1-0")["version"] == "1.0"
    assert parse.parse_slug("ggs2-se-wave1register")["wave"] == "w1register"
    assert parse.parse_slug("upcoming-ggs2-fr-wave1")["upcoming"]
    ffs = parse.parse_slug("ffs-us-thefamilyandfertilitysurvey")
    assert parse.naming(ffs, "x", "1.0") == ("ffs/us", "ffs_us_v1.0")
    assert parse.parse_slug("weird")["project"] == "OTHER"


def test_catalog_keeps_only_dataset_pages():
    assert parse.parse_catalog(CATALOG) == [
        "ggs2-at-wave1", "ggs2-nl-wave1", "upcoming-ggs2-fr-wave1", "ffs-us-thefamilyandfertilitysurvey"]
    assert parse.slug_from_href("/dataset/../x/") is None


def test_dataset_page():
    d = parse.parse_dataset(AT, "ggs2-at-wave1")
    assert d["version"] == "1.1" and d["country_name"] == "Austria"
    assert (d["collected_from"], d["collected_to"]) == ("2022-10-31", "2023-03-12")
    assert d["colectica_url"].endswith("/c7fc")
    assert d["other_versions"] == ["ggs2-at-wave1-v1-0", "ggs2-at-wave1-v1-1"]
    assert d["files"] == [dict(download_id=95, url="https://www.ggp-i.org/form/download/95/",
                               label="Austria - Wave 1 (version 1.1) v1.1", size_mb=15.3)]
    assert parse.parse_dataset(NL, "ggs2-nl-wave1")["files"][0]["size_mb"] == 1228.8
    assert parse.parse_dataset(FR, "x")["files"] == []


def test_ext_values():
    assert extract.norm_ext(None) == extract.norm_ext([]) == {"dta", "sav", "csv"}
    assert extract.norm_ext(".SAV") == {"sav"}
    assert extract.norm_ext(["dta", "CSV"]) == {"dta", "csv"}


def test_unpack_filters_mirrors_nested_zips_and_stays_inside(tmp_path):
    z = make_zip({
        "A/x.dta": b"1", "A/x.sav": b"2", "A/README": b"r",
        "A/Sub.ZIP": make_zip({"y.csv": b"3", "doc.pdf": b"4"}),
        "../evil.dta": b"x", "/abs.dta": b"x", "__MACOSX/A/._x.dta": b"x",
    })
    for name, ext, meta, expected in [
        ("d", {"dta", "csv"}, False, ["a/sub/y.csv", "a/x.dta"]),  # nested zip becomes a lowercase folder
        ("m", {"dta"}, True, ["a/README", "a/sub/doc.pdf", "a/x.dta"]),  # metadata: non-data files too
    ]:
        (tmp_path / "in.zip").write_bytes(z)
        assert extract.unpack(tmp_path / "in.zip", tmp_path / name, ext, meta) == len(expected)
        assert tree(tmp_path / name) == expected
    assert not (tmp_path / "evil.dta").exists()
    assert all(p.startswith(("d/", "m/", "in.zip")) for p in tree(tmp_path))  # nothing escaped


def test_7z_archives_like_the_portal_serves(tmp_path):
    # a 7z with a datafile, a document and a nested zip; and a 7z inside a zip
    seven = make_7z({"Data/AT.dta": b"1", "Docs/cb.pdf": b"2", "More.zip": make_zip({"y.sav": b"3"})})
    f = tmp_path / "dl.part"
    f.write_bytes(seven)
    assert extract.unpack_file(f, "GGS_Wave1_Austria_V.4.4.7z", tmp_path / "a", {"dta", "sav"}, False) == 2
    assert tree(tmp_path / "a") == ["data/AT.dta", "more/y.sav"]
    f.write_bytes(make_zip({"Inner.7z": seven}))
    assert extract.unpack_file(f, "x.zip", tmp_path / "b", {"dta"}, True) == 2
    assert tree(tmp_path / "b") == ["inner/data/AT.dta", "inner/docs/cb.pdf"]
    f.write_bytes(make_zip({"Wrong.7z": make_zip({"z.dta": b"4"})}))  # named .7z, really a zip
    assert extract.unpack_file(f, "x.7z", tmp_path / "c", {"dta"}, False) == 1
    assert not list(tmp_path.rglob("*.part.*")) and not list((tmp_path / "a").rglob("*.part"))


def test_plain_file_is_kept_only_if_it_matches(tmp_path):
    f = tmp_path / "raw.part"
    f.write_bytes(b"d")
    assert extract.unpack_file(f, "AT.dta", tmp_path / "o", {"dta"}, False) == 1
    assert tree(tmp_path / "o") == ["AT.dta"]
    assert extract.unpack_file(f, "AT.dta", tmp_path / "o2", {"sav"}, False) == 0
    assert extract.unpack_file(f, "..", tmp_path / "o3", {"dta"}, True) == 1  # unsafe name is replaced
    assert tree(tmp_path / "o3") == ["download"]
    f.write_bytes(b"new")
    assert extract.unpack_file(f, "AT.dta", tmp_path / "o", {"dta"}, False) == 1  # matches, but exists: kept
    assert (tmp_path / "o/AT.dta").read_bytes() == b"d"
    assert extract.unpack_file(f, "AT.dta", tmp_path / "o", {"dta"}, False, force=True) == 1
    assert (tmp_path / "o/AT.dta").read_bytes() == b"new"


def test_sync_download_select(tmp_path):
    con = db.connect(tmp_path / "catalogue.sqlite")

    async def first():
        async with portal(tmp_path, TOKEN) as p:
            found, failed = await p.sync()
            await p.login()
            rows = db.select(con, downloadable=True)
            return found, failed, rows, await p.download(rows, tmp_path / "dl")

    found, failed, rows, (done, skipped, bad) = asyncio.run(first())
    assert len(found) == 7 and not failed  # v1.0 via "other versions" (v1.1 is the latest: one row), HH, NKPS via the access page
    # ids follow project, round, wave, country: FFS first, then GGS-II w1 AT, AT v1.0, FR, NL, then HH, OTHER
    ids = {r["slug"]: r["id"] for r in db.select(con, all_versions=True)}
    assert ids == {"ffs-us-thefamilyandfertilitysurvey": 1, "ggs2-at-wave1": 2, "ggs2-at-wave1-v1-0": 3,
                   "upcoming-ggs2-fr-wave1": 4, "ggs2-nl-wave1": 5, "hh-harmonized-histories": 6, "other-nkps": 7}
    assert [r["slug"] for r in db.select(con)] == [
        "ffs-us-thefamilyandfertilitysurvey", "ggs2-at-wave1", "upcoming-ggs2-fr-wave1", "ggs2-nl-wave1",
        "hh-harmonized-histories", "hh-harmonized-histories", "other-nkps"]  # one row per file: HH has two
    assert [r["id"] for r in db.select(con, ids=db.parse_ids("2-3,5"), all_versions=True)] == [2, 3, 5]

    def slugs(**kw):
        return [r["slug"] for r in db.select(con, **kw)]

    nl, at, fr = "ggs2-nl-wave1", "ggs2-at-wave1", "upcoming-ggs2-fr-wave1"
    assert slugs(country="nl") == slugs(country="NL") == [nl]  # one value, any case
    assert slugs(country=["nl", "at"]) == [at, nl]  # a list
    assert len(slugs()) == len(slugs(project=None, country=[])) == 7  # default: all
    assert slugs(project=["hh", "other"]) == ["hh-harmonized-histories"] * 2 + ["other-nkps"]
    assert slugs(project="other", wave="NKPS") == ["other-nkps"]
    assert slugs(project="ggs", round=2, wave=1, country=["nl", "at"]) == [at, nl]  # bare 1 means w1
    assert slugs(round="ii") == slugs(round=2) == [at, fr, nl]
    with pytest.raises(ValueError, match="unknown round"):
        db.select(con, round=1)  # no GGS-I in this catalogue
    with pytest.raises(ValueError, match="unknown country"):
        db.select(con, country=["nl", "xx"])

    # default download: datafiles only, in a folder per file, zip contents and nested zips mirrored
    assert (done, skipped) == (5, 0)
    assert [b[0] for b in bad] == ["ggs2-nl-wave1"] and "web page" in bad[0][1]
    at_dir = tmp_path / "dl/ggs-ii/w1/at/ggs2_w1_at_v1.1"
    ffs_dir = tmp_path / "dl/ffs/us/ffs_us_v1.0"
    assert tree(at_dir) == ["data/AT_w1.dta", "data/AT_w1.sav"]
    assert tree(ffs_dir) == ["ffs/waves/wave1/us1.dta", "ffs/waves/wave1/us1.sav"]
    assert (at_dir / "data/AT_w1.dta").read_bytes() == b"dta"
    assert tuple(con.execute("SELECT local_path, fetched FROM file WHERE download_id=95").fetchone()) == (
        "ggs-ii/w1/at/ggs2_w1_at_v1.1", "csv dta sav")
    assert not list(tmp_path.rglob("*.part"))
    assert os.stat(tmp_path / ".session.json").st_mode & 0o777 == 0o600
    assert "s3cr3t" not in (tmp_path / ".session.json").read_text()

    def again(force=False, **kw):  # new process: no access url, the saved session is enough
        async def go():
            async with portal(tmp_path, force=force) as p:
                await p.login()
                res = await p.download(root=tmp_path / "dl", **kw)
                return res[:2]
        return asyncio.run(go())

    assert tree(tmp_path / "dl/hh/harmonized-histories") == [
        "hh_harmonized-histories_1/x.dta", "hh_harmonized-histories_84/x.dta"]  # two files: a folder each
    assert tree(tmp_path / "dl/other/nkps") == ["other_nkps/x.dta"]
    assert again() == (0, 5)  # same request: nothing to do
    assert again(ext="dta") == (0, 5)  # a subset of what was already unpacked
    (at_dir / "data/AT_w1.dta").write_bytes(b"my edits")
    assert again(metadata=True) == (5, 0)
    assert (at_dir / "data/AT_w1.dta").read_bytes() == b"my edits"  # fetched again for the docs: existing file kept  # documentation was not kept yet: fetched again, added
    assert tree(at_dir) == ["data/AT_w1.dta", "data/AT_w1.sav", "docs/Codebook.pdf"]
    assert tree(ffs_dir) == ["ffs/waves/notes.txt", "ffs/waves/wave1/us1.dta",
                             "ffs/waves/wave1/us1.sav", "readme.txt"]
    assert again(metadata=True) == (0, 5)
    assert again(force=True) == (5, 0)  # force: everything again, existing files overwritten
    assert (at_dir / "data/AT_w1.dta").read_bytes() == b"dta"


def test_ext_without_matches_fails_and_leaves_no_folder(tmp_path):
    con = db.connect(tmp_path / "catalogue.sqlite")

    async def go():
        async with portal(tmp_path, TOKEN) as p:
            await p.sync()
            await p.login()
            return await p.download(db.select(con, country="at", downloadable=True), tmp_path / "dl", ext="csv")

    done, _, bad = asyncio.run(go())
    assert done == 0 and "looked for csv" in bad[0][1]
    assert not (tmp_path / "dl/ggs-ii/w1/at/ggs2_w1_at_v1.1").exists()
    assert con.execute("SELECT local_path FROM file WHERE download_id=95").fetchone()[0] is None


def no_terminal(prompt):
    raise EOFError  # what getpass raises when there is nobody to ask


def login(tmp_path, url=None):
    async def go():
        async with portal(tmp_path, url) as p:
            await p.login()
    asyncio.run(go())


def test_login_errors_never_leak_the_token(tmp_path, monkeypatch):
    monkeypatch.setattr("getpass.getpass", no_terminal)
    monkeypatch.delenv("GGP_ACCESS_URL", raising=False)
    with pytest.raises(LoginError, match="no valid session"):
        login(tmp_path)
    with pytest.raises(LoginError) as e:
        login(tmp_path, "https://www.ggp-i.org/form/accounts/login/email/wrong-t0k3n/")
    assert "wrong-t0k3n" not in str(e.value)
    with pytest.raises(LoginError, match="must start with") as e:  # never request some other site
        login(tmp_path, "https://evil.example/t0k3n/")
    assert "t0k3n" not in str(e.value)


def test_login_asks_for_the_link_only_when_needed(tmp_path, monkeypatch):
    asked = []
    monkeypatch.delenv("GGP_ACCESS_URL", raising=False)
    monkeypatch.setattr("getpass.getpass", lambda prompt: asked.append(prompt) or f"  {TOKEN}\n")
    login(tmp_path)  # no argument, no environment variable, no session: asked once
    assert len(asked) == 1 and (tmp_path / ".session.json").exists()
    login(tmp_path)  # the saved session works: not asked again
    assert len(asked) == 1


def test_login_uses_the_environment_variable_before_asking(tmp_path, monkeypatch):
    monkeypatch.setattr("getpass.getpass", no_terminal)
    monkeypatch.setenv("GGP_ACCESS_URL", TOKEN)
    login(tmp_path)
    assert (tmp_path / ".session.json").exists()


def test_broken_session_file_means_log_in_again(tmp_path, monkeypatch):
    monkeypatch.setattr("getpass.getpass", no_terminal)
    (tmp_path / ".session.json").write_text("")  # a write that was cut off
    (tmp_path / ".session.json").chmod(0o644)
    login(tmp_path, TOKEN)
    assert os.stat(tmp_path / ".session.json").st_mode & 0o777 == 0o600
    login(tmp_path)  # and the new one is valid


def test_resync_drops_replaced_links_and_ids_accept_reversed_ranges():
    con = db.connect(":memory:")
    page = dict(version="1.0", country_name="AT", title="t", collected_from=None, collected_to=None,
                colectica_url=None)
    for files in ([95], [97], []):  # link 95 replaced by 97, then a page where no link was found
        db.upsert(con, "ggs2-at-wave1", {**page, "files": [
            dict(download_id=i, label="a", size_mb=1, url=f"u{i}") for i in files]})
    assert [(r["download_id"], r["n_files"]) for r in db.select(con)] == [(97, 1)]
    assert db.parse_ids("12-3, 5") == [(3, 12), (5, 5)]


def test_concurrency_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        ggpClient(tmp_path, concurrency=0)


def test_access_page_sections():
    found = parse.parse_access(FORM)
    assert sorted(found) == ["hh-harmonized-histories", "other-nkps"]  # GGS-II rows come from the public pages
    assert [(f["download_id"], f["label"]) for f in found["hh-harmonized-histories"]["files"]] == [
        (1, "Part 1"), (84, "Part 2")]  # the repeated list is merged
    p = parse.parse_slug("other-nkps")
    assert (p["project"], p["wave"], p["country"]) == ("OTHER", "nkps", "")
    assert parse.naming(parse.parse_slug("hh-harmonized-histories"), "x", None) == (
        "hh/harmonized-histories", "hh_harmonized-histories")


def test_sync_without_login_still_indexes_the_public_pages(tmp_path, monkeypatch):
    monkeypatch.setattr("getpass.getpass", no_terminal)
    monkeypatch.delenv("GGP_ACCESS_URL", raising=False)
    con = db.connect(tmp_path / "catalogue.sqlite")

    async def go():
        async with portal(tmp_path) as p:
            return await p.sync()

    found, failed = asyncio.run(go())
    assert len(found) == 5 and list(failed) == ["data access page"]
    assert "skipped" in failed["data access page"]


def test_client_holds_con_and_filtered_rows(tmp_path):
    async def go():
        async with ggpClient(tmp_path, TOKEN, project="ggs", round=2, wave=["w1"], country=["at", "nl"],
                             transport=httpx.MockTransport(handler)) as p:
            assert p.rows == [] and p.con is not None  # empty catalogue until the first sync
            await p.sync()
            return [r["slug"] for r in p.rows]
    assert asyncio.run(go()) == ["ggs2-at-wave1", "ggs2-nl-wave1"]  # refreshed by sync, filters applied
    assert ggpClient.__dataclass_fields__["data_dir"].default == "ggp_data"
