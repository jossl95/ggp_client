"""Command line: sync the catalogue, list datasets, download them."""
import argparse
import asyncio
import sys
from pathlib import Path

import httpx

from . import db, parse
from .client import LoginError, ggpClient


def add_filters(ap):
    ap.add_argument("--project", nargs="+", default=[], help="ggs, ffs (default: all)")
    ap.add_argument("--round", nargs="+", default=[], help="1 or 2, or i, ii (default: all)")
    ap.add_argument("--wave", nargs="+", default=[], help="w1 w2 pilot ... (default: all)")
    ap.add_argument("--country", nargs="+", default=[], help="country codes, e.g. nl at (default: all)")
    ap.add_argument("--ids", help="catalogue ids or ranges, e.g. 3-12,20")
    ap.add_argument("--all-versions", action="store_true", help="include superseded versions")


def pick(a, con, **extra):
    try:
        ids = db.parse_ids(a.ids) if a.ids else None
        return db.select(con, a.project, a.round, a.wave, a.country, ids, a.all_versions, **extra)
    except ValueError as e:
        sys.exit(str(e))


def show(rows):
    print(f"{'id':>4}  {'series':<7} {'wave':<9} {'cc':<3} {'ver':<5} {'size_mb':>8}  status")
    for r in rows:
        status = ("upcoming" if r["upcoming"] else "no file" if r["download_id"] is None
                  else r["local_path"] or "not downloaded")
        size = f"{r['size_mb']:.1f}" if r["size_mb"] is not None else "-"
        print(f"{r['id']:>4}  {r['series']:<7} {r['wave'] or '-':<9} {r['country']:<3} "
              f"{r['version'] or '-':<5} {size:>8}  {status}")
    print(f"{len(rows)} rows")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="ggp-client", description="Index and download GGP datasets.")
    ap.add_argument("--data-dir", default="ggp_files", help="catalogue, downloads and session live here")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync", help="index all datasets from the public pages (no login needed)")
    s.add_argument("--dump", metavar="DIR", help="also save the raw HTML pages, for debugging the parser")

    add_filters(sub.add_parser("list", help="show indexed datasets"))

    d = sub.add_parser("download", help="download and unpack datasets (needs the emailed access link)")
    add_filters(d)
    d.add_argument("--access-url", help="the emailed link; default: GGP_ACCESS_URL, else you are asked "
                                        "(a flag ends up in your shell history)")
    d.add_argument("--concurrency", type=int, default=3)
    d.add_argument("--ext", nargs="+", default=[], help="datafile types to keep: dta sav csv (default: all three)")
    d.add_argument("--metadata", action="store_true", help="also keep documentation and other non-data files")
    d.add_argument("--force", action="store_true", help="download again even if the file exists")

    a = ap.parse_args(argv)
    data = Path(a.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    con = db.connect(data / "catalogue.sqlite")

    if a.cmd == "sync":
        async def run():
            async with ggpClient(data) as p:
                return await p.sync(con, a.dump)
        try:
            found, failed = asyncio.run(run())
        except (RuntimeError, httpx.HTTPError) as e:  # the catalogue page itself could not be fetched
            sys.exit(f"sync failed: {e}")
        files = sum(len(v["files"]) for v in found.values())
        print(f"indexed {len(found)} datasets and {files} files in {data / 'catalogue.sqlite'}")
        nolink = [k for k, v in found.items() if not v["files"] and not parse.parse_slug(k)["upcoming"]]
        if nolink:
            print(f"no download link found on {len(nolink)} pages: {', '.join(nolink)}\n"
                  "if that is unexpected, rerun with --dump DIR to keep the raw pages for debugging")
        for slug, err in failed.items():
            print(f"failed: {slug}: {err}")
        return

    if a.cmd == "list":
        show(pick(a, con))
        return

    rows = pick(a, con, downloadable=True)
    if not rows:
        sys.exit("nothing to download (run 'ggp-client sync' first, or loosen the filters)")

    async def run():
        async with ggpClient(data, a.access_url, a.concurrency) as p:
            await p.login()
            return await p.download(con, rows, data / "downloads", a.force, a.ext, a.metadata)
    try:
        done, skipped, failed = asyncio.run(run())
    except LoginError as e:
        sys.exit(f"login failed: {e}")
    print(f"downloaded {done}, skipped {skipped} already present, failed {len(failed)}")
    for slug, err in failed:
        print(f"failed: {slug}: {err}")


if __name__ == "__main__":
    main()
