# ggp-client

Download data from the [GGP data portal](https://www.ggp-i.org) (Generations and Gender Programme): GGS rounds 1
and 2, FFS, Harmonized Histories and Additional Datasets. It indexes the portal, downloads and unpacks the files,
converts them to parquet with their labels, and stacks the countries of each round and wave into one dataset.

Documentation: **https://jossl95.github.io/ggp_client/** (source in [`docs/`](docs/), built with Quarto).

## Getting started

```bash
pip install -e .
ggp-client sync                                  # index the portal into data/catalogue.sqlite
ggp-client list --project ggs --round 2          # what is there (the same filters work for every command)
```

Downloading needs the access link the portal emails you. The first download asks you to paste it (the input is
hidden), then the session is saved in `data/.session.json` and reused. The link is valid for seven days and is only asked for, never kept: there is no environment variable for it.
Treat the link like a password: do not commit it or pass it on the command line, where it stays in your history.

## Use case: GGS-II in Stata format, consolidated to parquet

```bash
ggp-client download    --project ggs --round 2 --ext dta   # every GGS-II country, .dta only
ggp-client preprocess  --project ggs --round 2             # each file to parquet + meta.json
ggp-client consolidate --project ggs --round 2             # one parquet per wave, all countries stacked
```

The same from Python:

```python
import asyncio
from ggp_client import ggpClient

async def main():
    async with ggpClient(project="ggs", round=2) as p:
        await p.sync()
        await p.download(ext="dta")
        await p.preprocess()
        await p.consolidate()

if __name__ == "__main__":  # needed: preprocessing runs in worker processes
    asyncio.run(main())
```

Result, in `data/`:

```
download/raw/ggs-ii/w1/at/ggs2_w1_at_v1.1/.../ggsii_wave1_at_v_1_1.dta          as downloaded
download/processed/ggs-ii/w1/at/ggs2_w1_at_v1.1/.../ggsii_wave1_at_v_1_1.parquet one file as parquet
download/processed/.../ggsii_wave1_at_v_1_1.meta.json                           its labels
download/consolidated/ggs-ii_w1.parquet                                         all countries of wave 1
download/consolidated/ggs-ii_w1.meta.json                                       their harmonized labels
```

GGS-II wave 1 is 185,660 rows of 20 countries and 4,001 variables. The columns `ggp_country`, `ggp_round`,
`ggp_wave`, `ggp_version` and `ggp_file` say where each row came from. Country-specific variables (named after the
GGP country number, such as `aut01a_2101` for Austria) are left out of the consolidated file, but kept in the
per-country files. `python download_ggs.py` does all of this for both GGS rounds and also writes each consolidated
dataset as a labelled Stata file (`ggs-ii_w1.dta`).

## Labels (meta.json)

The parquet files hold the codes. Their labels, as pyreadstat reads them from the `.dta` and `.sav` files, are in
the `.meta.json` next to them:

```python
import json
import polars as pl

df = pl.read_parquet("data/download/consolidated/ggs-ii_w1.parquet")
meta = json.load(open("data/download/consolidated/ggs-ii_w1.meta.json", encoding="utf-8"))
meta["column_labels"]["dem01"]   # 'Gender'                                   variable -> variable label
meta["value_labels"]["dem01"]    # {'1': 'Male', '2': 'Female', '3': 'Other', ...}  variable -> value -> label
df.select(pl.col("dem01").cast(pl.Int64).cast(pl.String).replace(meta["value_labels"]["dem01"]))
```

Codes are JSON text (`"1"`); use `int(code)` to hand them back to pyreadstat (`variable_value_labels`).

The consolidated labels are harmonized over the countries, each country voting once:

- every variable has one label: the one most countries use;
- every value has exactly one label: the one more than half of the countries use (ISO 203 is "Czechia", not
  "Czech Republic"); if there is no majority, all labels joined into one, most used first (`region` 1:
  "East Austria / East Midlands / Pannonian Croatia / ..."). Several values may share a label;
- codes are never changed;
- `original_labels` lists, for every value countries label differently, each wording and the files that use it.

## Other data

- **R**: use the native R package [ggpclientR](https://github.com/jossl95/ggpclientR) (`pak::pak("jossl95/ggpclientR")`, [docs](https://jossl95.github.io/ggpclientR/)).
- **FFS** comes as fixed-width `.dat` with SPSS syntax (`.sps`) and frequency tables (`.tbl`), kept in
  `download/raw/ffs/<cc>/`. The download also converts it, with labels, to `download/processed/ffs/<cc>/converted_data.dta`
  (`--ext sav` for `.sav`). In SPSS you can run the `.sps` itself. One dataset by hand: `ggp-client convert AUSTRIA.SPS --ext dta`.
- **Harmonized Histories and Additional Datasets** are listed only after login; `sync` logs in to index them
  (`--project hh` or `--project other`).
- **Filters** (`--project`, `--round`, `--wave`, `--country`, `--ids`, `--all-versions`) work for `list`,
  `download`, `preprocess` and `consolidate`. Run `list` first as a dry run.

## Good to know

- Nothing is downloaded or written twice: existing files are kept. `--force` redoes everything, and replaces datafiles saved earlier under an uppercase name (`download --force`, then `preprocess --force`). A consolidated
  file is rebuilt by itself when its countries or input files change.
- Every step is logged to `data/ggp_client.log`. The access link never is.
- Three GGS-I files (Australia waves 1 and 2, Norway wave 1) give "404 Not Found" on the portal itself.
- GGS-II Belarus wave 1 comes in English and Russian: it is in the consolidated file twice. Filter on `ggp_file`.
- Labelled Stata files are large (GGS-II wave 1: 3.3 GB, against 36 MB as parquet); `compress` in Stata helps.
  Text longer than 2,045 characters is cut in them.
- Some GGS files have broken text encoding in their labels; they are read as latin1, as their `.sav` versions are.

Run the tests with `pip install -e .[test] && pytest`.
