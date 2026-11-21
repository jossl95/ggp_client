#!/usr/bin/env python3
"""Download all GGS data, rounds 1 and 2 (latest versions), keeping every datafile type: dta, sav and csv.

    python download_ggs.py              # into ggp_data/
    python download_ggs.py other_dir    # elsewhere

Refreshes the catalogue first (asks for the emailed access link if there is no valid session; see README,
"Logging in"). Files already downloaded are kept. Progress is shown and logged in <data_dir>/ggp_client.log.
Exits with 1 if any file failed.
"""
import asyncio
import logging
import sys

from ggp_client import ggpClient

EXT = ["dta", "sav", "csv"]


async def main(data_dir="ggp_data", **client_options):
    async with ggpClient(data_dir, project="ggs", **client_options) as p:  # project GGS is rounds 1 and 2
        await p.sync()
        await p.login()
        done, skipped, failed = await p.download(ext=EXT)
    print(f"GGS rounds 1 and 2: downloaded {done}, skipped {skipped} already present, failed {len(failed)}")
    for slug, err in failed:
        print(f"failed: {slug}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    sys.exit(asyncio.run(main(*sys.argv[1:2])))
