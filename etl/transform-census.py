import os
import pyarrow as pa
from pyarrow import csv
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Define columns ----
## original columns to read in
cols = ["gestfips", "gtco", "PRTAGE", "PESEX", 
        "PEEDUCA", "PRPERTYP", "PRHRUSL", "PESCHFT", "PESCHLVL", 
        "PWSSWGT", "PES1", "PES2", "PES3", "PES4", "PES5", 
        "PES6", "PES7", "HEFAMINC"]

## column mapping to rename
cols_to = ["state", "county", "age", "sex",
           "educ", "person_type", "hours_worked", "in_school", "hs_college",
           "weight", "did_vote", "did_register", "why_not_register", "why_not_vote",
           "person_or_mail", "ed_or_early", "register_where", "family_income"]

# Read raw ----
opts_conv = csv.ConvertOptions(include_columns = cols)
tbl = csv.read_csv('data/raw/cps_suppl.csv', convert_options = opts_conv)

# Tranform ----
nc_adults = pc.and_(pc.equal(tbl['gestfips'], 37), pc.equal(tbl['PRPERTYP'], 2))
tbl = tbl. \
    filter(nc_adults). \
    rename_columns(cols_to). \
    drop(['state', 'person_type'])

# Write output ----
pq.write_table(tbl,
               "data/cps_suppl.parquet",
               use_dictionary = [c for c in cols_to if c != 'age'])
