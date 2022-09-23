## duckdb + NC midterm data

This repository contains a proof-of-concept for lightweight data "infrastructure" to analyze NC midterm data out-of-memory with `duckdb` and `pyarrow`. 

It is not intended to illustrate best engineering practices but rather the power of duckdb as part of a "good enough" workflow.

### Data sources

The ultimate data tables accessible via the `duckdb` database are:

- NC 2022 midterm early vote data from [NCSBE](https://www.ncsbe.gov/results-data)
- NC voter registration file from [NCSBE](https://www.ncsbe.gov/results-data)
- NC 10-year voter history file from [NCSBE](https://www.ncsbe.gov/results-data)
- Current Population Survey 2022 November voting supplement from [US Census Bureau](https://www.census.gov/data/datasets/time-series/demo/cps/cps-supp_cps-repwgt/cps-voting.html)
- County-level past election results from [MIT Election Lab via Harvard Dataverse](https://dataverse.harvard.edu/file.xhtml?fileId=6104822&version=10.0)

(Note: In the spirit of "a minimalistic infrastructure for a low resource environment", the static dataset of past county-level election results was downloaded manually. Otherwise, Harvard Dataverse required obtaining an API key for programmatic interaction.)

### Database tables

The data sources mentioned above are queryable via `nc.duckdb` in the respective tables:

- `early_vote`
- `register`
- `hist_gen` + `hist_oth` (these tables have the same schema but were split for semantic reasons since elections that are not statewide general elections are not equally applicable to all voters)
- `cps_suppl`
- `county_results`

### Key scripts

Core scripts are in the `etl` subdirectory:

- `extract-*.py` scripts: Download (and unzip) different data sources from NCSBE and US Census Bureau and write results to `data/raw/`
- `transform-*.py` scripts: Wrangle downloaded data sources from `csv` to `parquet` with light transformation with `pyarrow`. Results are written to `data/`
- `load-db.py`: Loads nothings! It creates a `nc.duckdb` file which references the external parquet files in views
