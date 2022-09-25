## duckdb + NC midterm data

This repository contains a proof-of-concept for lightweight data "infrastructure" to analyze NC midterm data out-of-memory with `duckdb` and `pyarrow`. 

**This repo is not intended to illustrate best engineering practices but rather the power of duckdb as part of a "good enough" workflow.**

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

- `early_vote` (~6K records as-of 2022-09-24 and growing fast!)
- `register` (~8.6M records)
- `hist_gen` (~22M records) + `hist_oth` (~11M records) (these tables have the same schema but were split for analytical reasons since elections that are not statewide general elections are not equally applicable to all voters)
- `cps_suppl`
- `county_results`

### Key scripts

Core scripts are in the `etl` subdirectory:

- `extract-*.py` scripts: Download (and unzip) different data sources from NCSBE and US Census Bureau and write results to `data/raw/`
- `transform-*.py` scripts: Wrangle downloaded data sources from `csv` to `parquet` with light transformation with `pyarrow`. Results are written to `data/`
- `load-db.py`: Loads nothings! It creates a `nc.duckdb` file which references the external parquet files in views

## Running on Codespaces

1. Launch on Codespaces

2. Set-up environment:

```
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
```

3. Pull all raw data:

```
chmod +x etl/extract-all.sh
etl/extract-all.sh
```

4. Transform all raw data:

```
chmod +x etl/transform-all.sh
etl/transform-all.sh
```

5. Create duckdb database:

```
python etl/load-db.py
```

6. (Optional) Install duckdb CLI

```
chmod +x get-duckdb-cli.sh
./get-duckdb-cli.sh
```

7. Run sample queries

7a. Run sample queries in CLI

Launch the CLI:

```
./duckdb nc.duckdb
.timer on
```

(Note: You can exit the DuckDB CLI with `Ctrl + D`)

Try out some sample queries. For example, we might wonder how many past general elections that early voters have voted in before:

```
with voter_general as (
select early_vote.ncid, count(1) as n
from 
  early_vote 
  left join 
  hist_gen 
  on early_vote.ncid = hist_gen.ncid 
group by 1)
select n, count(1) as freq
from voter_general
group by 1
order by 1
;
```

And, this question is more interesting if we join on registration data to learn how many prior general elections each voter was eligible to vote in:

```
with voter_general as (
select 
  early_vote.ncid, 
  extract('year' from register.registr_dt) as register_year, 
  count(1) as n
from 
  early_vote 
  left join 
  hist_gen 
  on early_vote.ncid = hist_gen.ncid 
  left join
  register
  on early_vote.ncid = register.ncid
group by 1,2)
select
  n, 
  case 
  when register_year < 2012 then 'Pre-2012'
  else register_year
  end as register_year,
  count(1) as freq
from voter_general
group by 1,2
order by 1,2
;
```

(Yes, of course *date* matters more than year here, etc. etc. This is purely to demonstrate `duckdb` not rigorous analysis!)

7b. Run sample queries in python

In python: See sample queries in `test-query.py` file
 
8. Run `free` in the terminal to marvel at what 8GB of RAM can do!
