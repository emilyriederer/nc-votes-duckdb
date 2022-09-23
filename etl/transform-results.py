import os
import pyarrow as pa
from pyarrow import csv
import pyarrow.compute as pc
import pyarrow.parquet as pq

cols = ['year', 'state_po', 'county_name', 'county_fips', 'party', 'candidatevotes']
tbl = csv.read_csv('data/raw/results_county.csv')
tbl = tbl.filter(pc.equal(tbl['state_po'], 'NC'))
tbl = tbl. \
  append_column('n_rep', pc.if_else(pc.equal(tbl['party'], 'REPUBLICAN'), tbl['candidatevotes'], 0)). \
  append_column('n_dem', pc.if_else(pc.equal(tbl['party'], 'DEMOCRAT'), tbl['candidatevotes'], 0)). \
  append_column('n_gre', pc.if_else(pc.equal(tbl['party'], 'GREEN'), tbl['candidatevotes'], 0)). \
  append_column('n_lib', pc.if_else(pc.equal(tbl['party'], 'LIBERTARIAN'), tbl['candidatevotes'], 0)). \
  append_column('n_oth', pc.if_else(pc.equal(tbl['party'], 'OTHER'), tbl['candidatevotes'], 0)). \
  append_column('n_tot', tbl['candidatevotes']). \
  group_by(['year', 'county_name', 'county_fips']). \
  aggregate([('n_rep', 'sum'), ('n_dem', 'sum'), ('n_tot', 'sum')])
pq.write_table(tbl, 'data/results_county.parquet', use_dictionary = ['year', 'county_name', 'county_fips'])
