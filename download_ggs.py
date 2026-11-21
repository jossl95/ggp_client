#!/usr/bin/env python3
"""Download all GGS data, rounds 1 and 2 (latest versions), as Stata files, and consolidate it per round and wave.

    python download_ggs.py              # into data/
    python download_ggs.py other_dir    # elsewhere

1. sync the catalogue and download every GGS dataset, keeping the .dta files   -> data/download/raw
2. preprocess each to parquet, with its labels in a .meta.json                 -> data/download/processed
3. consolidate per round and wave: <series>_<wave>.parquet and .meta.json      -> data/download/consolidated
4. the same as Stata, with the variable and value labels applied: <series>_<wave>.dta

Asks for the emailed access link if there is no valid session (see README, "Logging in"). Work already done is
kept. Progress is shown and logged in <data_dir>/ggp_client.log. Exits with 1 if any step failed for any file.
"""
import asyncio
import logging
import sys

from ggp_client import consolidate, ggpClient


async def main(data_dir="data", **client_options):
    async with ggpClient(data_dir, project="ggs", **client_options) as p:  # project GGS is rounds 1 and 2
        await p.sync()
        await p.login()
        steps = {"download": await p.download(ext="dta"), "preprocess": await p.preprocess(),
                 "consolidate": await p.consolidate()}
        names = consolidate.groups(p.rows, p.downloads, p.processed)
        steps["stata"] = await consolidate.to_stata_async([p.consolidated / f"{n}.parquet" for n in names], p.force)
    failed = [(step, *f) for step, (_, _, fails) in steps.items() for f in fails]
    for step, (done, skipped, fails) in steps.items():
        print(f"{step}: {done} done, {skipped} skipped (already there), {len(fails)} failed")
    for step, name, err in failed:
        print(f"failed: {step}: {name}: {err}")
    print(f"consolidated files in {p.consolidated}: " + ", ".join(f"{n}.parquet/.dta/.meta.json" for n in sorted(names)))
    return 1 if failed else 0


if __name__ == "__main__":  # also needed by the worker processes of preprocess and to_stata
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    sys.exit(asyncio.run(main(*sys.argv[1:2])))
