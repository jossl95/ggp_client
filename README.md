# ggp-client

Index and download datasets from the GGP data portal (https://www.ggp-i.org). Async, built on `httpx`.

```bash
pip install -e .

ggp-client sync                    # index every dataset (logs in for Harmonized Histories and Additional Datasets)
ggp-client list --project ggs --round 2 --country nl at
ggp-client list --ids 10-25          # a range of catalogue ids

ggp-client download                  # everything (latest versions), datafiles only
                                   # the first time you are asked to paste the emailed access link
ggp-client download --project ggs --round 2 --wave w1 --country nl
ggp-client download --ext dta        # only Stata files (dta, sav, csv; one or several)
ggp-client download --metadata       # also keep documentation and other non-data files
ggp-client download --ids 3-12,20 --all-versions
```

`list` takes the same filters as `download`, so use it as a dry run.

## Filters

`project`, `round`, `wave` and `country` each take one value, several values, or nothing (all).
They are the folder names: project `ggs`, `ffs`, `hh` (Harmonized Histories), `other` (Additional Datasets) or
`teaching`, round `1` or `2` (or `i`, `ii`), wave `w1`, `w2`, `pilot`, ...,
country `nl`, `at`, ... For `hh` and `other` the dataset name takes the place of the wave
(`--project other --wave nkps`) and there is no country. Case does not matter. A value that is not in the catalogue is an error, not an empty result.

From Python the filters are arguments of `ggpClient`. It opens the catalogue as `self.con` and keeps the
matching downloadable rows in `self.rows`, refreshed after `sync()` and `download()`:

```python
import asyncio
from ggp_client import ggpClient

async def main():
    async with ggpClient(project="ggs", round=2, wave=["w1", "w2"]) as p:  # data_dir defaults to ggp_data
        await p.sync()                     # see "Logging in" for the access link
        print(len(p.rows), "files to get")
        print(await p.download(ext=["dta", "sav"], metadata=False))  # p.rows into ggp_data/downloads

asyncio.run(main())
```

## What gets stored

Everything lives in `--data-dir` (default `ggp_data/`):

- `catalogue.sqlite`: table `dataset` (project, round, wave, country, version, collection dates,
  page and Colectica links, target folder and file stem) and table `file` (download id, label, size, URL,
  folder, what was unpacked). Ids are assigned once and never change.
- `downloads/<series>/<wave>/<cc>/<key>_<wave>_<cc>_v<version>/`, all lowercase, one folder per downloaded file,
  for example `downloads/ggs-ii/w1/at/ggs2_w1_at_v1.1/`. FFS has no wave level: `ffs/us/ffs_us_v1.0/`.
  Harmonized Histories and Additional Datasets: `hh/harmonized-histories/hh_harmonized-histories_<id>/`
  (one folder per part), `other/nkps/other_nkps/`.
- `.session.json`: your login session (mode 600), so the access link is used once, not on every run.
- `ggp_client.log`: every sync and download with a time stamp: logins, each file downloaded (size, files kept,
  folder), skipped or failed (with the reason), and retries. The access link is never logged.

## Logging

The client logs to the standard `logging` logger `"ggp_client"` (level INFO unless you set one) and appends the
same lines to `ggp_client.log` in the data folder. The command line also shows them on the terminal.
For more or less detail from Python: `logging.getLogger("ggp_client").setLevel(logging.WARNING)`.

## Unpacking

Most downloads are zip files; most GGS-I wave 1 files, the Harmonized Histories and several Additional Datasets
are 7z. The file contents decide, not the name (some `.7z` downloads are zips). Their contents are unpacked into that
folder, keeping the structure inside the archive. An archive inside the archive becomes a folder named after it,
for example `ggs2_w1_at_v1.1/data/stata.zip` becomes `ggs2_w1_at_v1.1/data/stata/`. Folder names are lowercased, file names
are kept as they are in the archive. The archive itself is not kept. Each file is written under a temporary name and renamed when complete.

- `--ext` (default `dta sav csv`): only datafiles of these types are kept. Data files of another type are skipped.
- `--metadata` (default off): also keep every file that is not a datafile type (codebooks, readmes, xml, ...).
- Asking for more later (another `--ext`, or `--metadata`) downloads the file again and adds to the folder;
  nothing is deleted. The same or a smaller request is skipped. A file that already exists is never overwritten,
  so your own changes to an unpacked file survive. `--force` (`ggpClient(force=True)`) downloads everything again
  and overwrites existing files.
- A download with no matching file is reported as failed and leaves no folder behind.

## Logging in

Downloads need the emailed access link, and so does the part of `sync` that indexes Harmonized Histories and
Additional Datasets: they are listed only on the logged-in data access page, behind a "Show N datasets" button.
Without a login `sync` still indexes everything else and reports what it skipped. `ggpClient.login()` (and so `ggp-client download`) looks for a login in this order:

1. the saved session in `.session.json`, if it still works
2. the `access_url` argument (`--access-url` on the command line)
3. the `GGP_ACCESS_URL` environment variable
4. otherwise you are asked to paste the link in the terminal; the input is hidden

With no terminal to ask on (a scheduled job, say) it stops with a `LoginError` instead of waiting.
The link must start with `https://www.ggp-i.org/`. Treat it like a password: prefer the prompt or the
environment variable over the flag, which ends up in your shell history, and do not commit it.
