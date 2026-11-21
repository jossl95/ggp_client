"""Command line: sync the catalogue, list datasets, download them."""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

import httpx

from . import db, parse, processing, read
from .client import LoginError, ggpClient


def positive(v):
    if int(v) < 1:
        raise argparse.ArgumentTypeError("must be 1 or more")
    return int(v)


def add_access_url(ap):
    ap.add_argument("--access-url", help="the emailed link; default: GGP_ACCESS_URL, else you are asked "
                                         "(a flag ends up in your shell history)")


def add_filters(ap):
    ap.add_argument("--project", nargs="+", default=[], help="ggs, ffs, hh, other, teaching (default: all)")
    ap.add_argument("--round", nargs="+", default=[], help="1 or 2, or i, ii (default: all)")
    ap.add_argument("--wave", nargs="+", default=[], help="w1 w2 pilot ... (default: all)")
    ap.add_argument("--country", nargs="+", default=[], help="country codes, e.g. nl at (default: all)")
    ap.add_argument("--ids", help="catalogue ids or ranges, e.g. 3-12,20")
    ap.add_argument("--all-versions", action="store_true", help="include superseded versions")


def filters(a):
    try:
        ids = db.parse_ids(a.ids) if a.ids else None
    except ValueError as e:
        sys.exit(str(e))
    return dict(project=a.project, round=a.round, wave=a.wave, country=a.country, ids=ids,
                all_versions=a.all_versions)


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
    ap.add_argument("--data-dir", default="data", help="catalogue, session and log; downloads in download/raw, parquet in download/processed and download/consolidated")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync", help="index all datasets (Harmonized Histories and Additional Datasets need a login)")
    s.add_argument("--dump", metavar="DIR", help="also save the raw HTML pages, for debugging the parser")
    add_access_url(s)

    add_filters(sub.add_parser("list", help="show indexed datasets"))

    d = sub.add_parser("download", help="download and unpack datasets (needs the emailed access link)")
    add_filters(d)
    add_access_url(d)
    d.add_argument("--concurrency", type=positive, default=3)
    d.add_argument("--ext", nargs="+", default=[], help="datafile types to keep: dta sav csv (default: all three); "
                                                         "FFS data is converted to dta and/or sav (default: dta)")
    d.add_argument("--metadata", action="store_true", help="also keep documentation and other non-data files")
    d.add_argument("--force", action="store_true", help="download and overwrite even files that already exist")

    for cmd, text in [("preprocess", "save downloaded data files as parquet in <data-dir>/download/processed"),
                      ("consolidate", "stack preprocessed files per round and wave in <data-dir>/download/consolidated")]:
        c = sub.add_parser(cmd, help=text)
        add_filters(c)
        c.add_argument("--concurrency", type=positive, default=4)
        c.add_argument("--force", action="store_true", help="redo and overwrite files that already exist")

    c = sub.add_parser("convert", help="convert one FFS dataset (.SPS and its .DAT) to sav, dta or csv, with labels")
    c.add_argument("sps", help="the .SPS file; the .DAT of the same name (or the only .DAT next to it) is read")
    c.add_argument("--ext", nargs="+", default=["sav"], choices=processing.FORMATS)
    c.add_argument("--force", action="store_true", help="overwrite existing files")

    a = ap.parse_args(argv)
    data = Path(a.data_dir)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

    if a.cmd == "convert":  # one dataset, outside the catalogue: written next to the .SPS
        sps = Path(a.sps)
        try:
            df, meta = processing.read_ffs(sps, read._dat(sps))
        except (OSError, ValueError) as e:
            sys.exit(f"convert failed: {e}")
        for e in a.ext:
            t = sps.with_suffix("." + e)
            print(f"{t}: {'written' if processing.write(df, meta, t, a.force) else 'exists, kept (use --force)'}, "
                  f"{len(df)} cases, {len(df.columns)} variables")
        return

    if a.cmd == "sync":
        async def run():
            async with ggpClient(data, a.access_url) as p:
                return await p.sync(a.dump)
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
        data.mkdir(parents=True, exist_ok=True)
        try:
            show(db.select(db.connect(data / "catalogue.sqlite"), **filters(a)))
        except ValueError as e:
            sys.exit(str(e))
        return

    if a.cmd in ("preprocess", "consolidate"):
        async def work():
            async with ggpClient(data, concurrency=a.concurrency, force=a.force, **filters(a)) as p:
                return await getattr(p, a.cmd)()
        try:
            done, skipped, failed = asyncio.run(work())
        except ValueError as e:
            sys.exit(str(e))
        print(f"{a.cmd}: {done} written, {skipped} skipped (up to date), failed {len(failed)}")
        for name, err in failed:
            print(f"failed: {name}: {err}")
        return

    async def run():
        async with ggpClient(data, a.access_url, a.concurrency, force=a.force, **filters(a)) as p:
            if not p.rows:
                sys.exit("nothing to download (run 'ggp-client sync' first, or loosen the filters)")
            await p.login()
            return await p.download(ext=a.ext, metadata=a.metadata)
    try:
        done, skipped, failed = asyncio.run(run())
    except ValueError as e:  # a filter value that is not in the catalogue
        sys.exit(str(e))
    except LoginError as e:
        sys.exit(f"login failed: {e}")
    print(f"downloaded {done}, skipped {skipped} already present, failed {len(failed)}")
    for slug, err in failed:
        print(f"failed: {slug}: {err}")


if __name__ == "__main__":
    main()
