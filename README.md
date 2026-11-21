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

ggp-client preprocess --project ggs  # downloaded data files as parquet in ggp_data/preprocessed
ggp-client consolidate --project ggs # stacked per round and wave in ggp_data/consolidated
```

`list` takes the same filters as `download`, so use it as a dry run.

To get all GGS data, rounds 1 and 2 with every datafile type (dta, sav, csv), run the script at the root:
`python download_ggs.py` (or `python download_ggs.py some_dir`). In November 2026 that was 69 files, a 0.5 GB
download and 7.6 GB unpacked, in about 2 minutes. Three GGS-I files (Australia waves 1 and 2, Norway wave 1) give
"404 Not Found" on the portal itself, so the script reports them and exits with 1.

Some GGS-I files mix text encodings in their labels; if Python's pyreadstat refuses one, pass
`encoding="latin1"` (or use `pandas.read_stata`). Stata and SPSS open them as they are.

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
  for example `downloads/ggs-ii/w1/at/ggs2_w1_at_v1.1/`. FFS has no wave level and no folder per dataset:
  `ffs/at/ffs_at.dat`, see "FFS data". Harmonized Histories and Additional Datasets:
  `hh/harmonized-histories/hh_harmonized-histories_<id>/` (one folder per part), `other/nkps/other_nkps/`.
- `.session.json`: your login session (mode 600), so the access link is used once, not on every run.
- `preprocessed/` and `consolidated/`: parquet files, see "Reading, preprocessing and consolidating".
- `ggp_client.log`: every sync and download with a time stamp: logins, each file downloaded (size, files kept,
  folder), skipped or failed (with the reason), and retries. The access link is never logged.

## FFS data (.dat, .sps, .tbl)

Every Family and Fertility Survey archive holds three files:

| file | what it is |
|---|---|
| `COUNTRY.DAT` | the data: fixed-width text, `records` lines per respondent (44 to 83, depending on the country) |
| `COUNTRY.SPS` | SPSS syntax that reads it: `DATA LIST` (record and columns of each variable), `VARIABLE LABELS`, `VALUE LABELS`, `MISSING VALUE` |
| `COUNTRY.TBL` | the published frequency tables, to check your results against; not needed to read the data |

Exceptions: Estonia ships ready-made `.SAV` files instead of a `.SPS`, Greece has no `.TBL` and its `.SPS` has no labels.

FFS has one dataset per country, so it has no folder of its own: the raw files are renamed after the dataset,
and with `--ext` the data is also converted, with variable and value labels, to `converted_data.<ext>`:

```
downloads/ffs/at/ffs_at.dat           ggp-client download --project ffs --country at
downloads/ffs/at/ffs_at.sps
downloads/ffs/at/ffs_at.tbl
downloads/ffs/at/converted_data.sav   ... --ext sav dta
downloads/ffs/at/converted_data.dta
```

Estonia has two samples (and a total), so its names get the sample added: `ffs_ee_est_f.dat`,
`converted_data_est_f.sav`, ...
From Python: `await p.download(ext=["sav"])`. A single dataset: `python -m ggp_client.ffs_conversion AUSTRIA.SPS --ext dta`.

Conversion replaces every `$` in every variable name with `_` (Stata does not allow `$`), in all three formats:
`IND01$01` is `IND01_01`, `V004$12` is `V004_12`. Other names are unchanged. The raw `.sps` and `.tbl` are rewritten the
same way on download (a `$` inside a quoted label, or one not inside a name such as `US$ 100`, stays), so syntax,
tables and converted files use the same names; only Estonia's raw `.sav` files keep the original names.
Missing-value codes such as 98 stay in the data, are labelled, and in `.sav` are marked missing as SPSS would.
A cell that is not a number (the files use `*` for overflow) is read as missing, as SPSS does, and logged.
`.csv` has the codes only.

**In SPSS** the raw files work directly: open `ffs_at.sps`. Its first line already reads `FILE='ffs_at.dat'`
(the original pointed at `C:\ISSA\...\AUSTRIA.DAT`); make that the full path to the file if SPSS does not find it,
add `SAVE OUTFILE='ffs_at.sav'.` at the end and run all. SPSS reads the columns and applies the labels and
missing values.

**In R** there is no reader for SPSS syntax, so convert first (`--ext sav` or `dta`) and read the result with haven:

```r
library(haven)
ffs <- read_sav("ggp_data/downloads/ffs/at/converted_data.sav", user_na = TRUE)  # keep 98 etc. as labelled missing
table(as_factor(ffs$RESID))                                                     # value labels as factor levels
attr(ffs$V917, "label")                                                         # "Religion respondent"
```

## Reading, preprocessing and consolidating (polars)

**Reading.** `ggp_client.read` reads any downloaded data file into a polars DataFrame: `.parquet`, `.dta`, `.sav`,
`.csv`, and an FFS `.sps` (with its `.dat`, no conversion needed). Values are the codes (value labels are not
applied); in `.sav`, values SPSS marks as missing are null. Files with broken text encoding (several GGS-I files,
GGS-II Finland and Uruguay `.dta`) are read as latin1, which gives the same text as their `.sav` twins.

```python
from ggp_client import read
df = read.read("ggp_data/downloads/ggs-ii/w1/at/ggs2_w1_at_v1.1/ggsii_wave1_at_v_1_1/GGSII_Wave1_AT_V_1_1.dta")
lf = read.scan("....parquet")              # LazyFrame: parquet and csv are scanned, dta/sav/sps read first
df = await read.read_async("....sav")      # does not block the event loop
```

**Preprocessing** saves every downloaded data file of the selected rows as parquet in `ggp_data/preprocessed`, at
the same path as in `downloads` (`.../GGSII_Wave1_AT_V_1_1.parquet`). A file that comes as dta, sav and csv is read
once (preference: parquet, dta, sav, csv, sps). FFS is read from the raw `.sps`/`.dat` (Estonia: its `.sav`), never
from `converted_data.*`. Columns added: `ggp_project`, `ggp_round`, `ggp_series`, `ggp_wave`, `ggp_country`,
`ggp_version` and `ggp_file` (the source file under `downloads`).

**Consolidating** stacks the preprocessed files per round and wave into `ggp_data/consolidated/<series>_<wave>.parquet`:
`ggs-ii_w1.parquet`, `ggs-i_w2.parquet`, `ffs.parquet` (no waves), `hh_harmonized-histories.parquet`, ...
Columns are matched by name; a column a country lacks is null there, and a column that is a number in one country
and text in another becomes text. It holds the countries of the rows you select, so filter by country only if you
want a smaller file of that name. A dataset shipped as two files is in it twice; tell them apart by `ggp_file`
(GGS-II Belarus wave 1 comes in English and Russian, `..._RU.dta`).

Both keep existing files unless `force`. From the client they run concurrently (async); the modules also have plain
functions:

```python
async with ggpClient(project="ggs", concurrency=4) as p:
    await p.preprocess()          # 4 files at a time; dta/sav in threads, polars writes lazily (sink_parquet)
    await p.consolidate()         # all groups in one lazy polars run (collect_all_async)

from ggp_client import consolidate, preprocess   # without asyncio, one at a time
preprocess.preprocess(p.rows, "ggp_data/downloads", "ggp_data/preprocessed")
consolidate.consolidate(p.rows, "ggp_data/downloads", "ggp_data/preprocessed", "ggp_data/consolidated")
```

All GGS (69 files, 0.5 GB download) took 2 minutes to preprocess (133 MB of parquet) and 12 seconds to consolidate
into 10 files, for example GGS-II wave 1: 185,660 rows of 20 countries, 5,642 columns.

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
