import duckdb
import os

# clean-up if already exists
if os.path.exists('nc.duckdb'):
  os.remove('nc.duckdb')
if os.path.exists('nc-rel.duckdb'):
  os.remove('nc-rel.duckdb')

# create new duckdb files 
## con: will hardcode global filepath. Needed to work with dBeaver but less portable
## con_rel: will use relative filepaths. Works with R/python connections
con = duckdb.connect('nc.duckdb')
con_rel = duckdb.connect('nc-rel.duckdb')

# generate SQL to register tables
template = "CREATE VIEW {view_name} as (select * from read_parquet('{path}'{opts}))"
data_dict = {
  'early_vote': 'data/early_vt.parquet',
  'hist_gen': 'data/history_general/*/*.parquet',
  'hist_oth': 'data/history_other/*/*.parquet',
  'register': 'data/register/*/*.parquet',
  'cps_suppl': 'data/cps_suppl.parquet'
}
partitioned = ['hist_gen', 'hist_pri', 'register']

for k,v in data_dict.items():

  print("Loading {view_name} data...".format(view_name = k))
  opt = ', HIVE_PARTITIONING=1' if k in partitioned else ''

  # connection with global path
  path = os.getcwd() + "//" + v
  query = template.format(view_name = k, path = path, opts = opt)
  con.execute(query)

  # connection with relative path
  query_rel = template.format(view_name = k, path = v, opts = opt)
  con_rel.execute(query_rel)

con.close()
con_rel.close()
