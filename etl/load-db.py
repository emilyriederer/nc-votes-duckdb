import duckdb
import os

# clean-up if already exists
if os.path.exists('nc.duckdb'):
  os.remove('nc.duckdb')

# create new duckdb files 
con = duckdb.connect('nc.duckdb')

# generate SQL to register tables
template = """
  CREATE VIEW {view_name} as 
  (select * from read_parquet('{path}'{opts}))
  """
data_dict = {
  'early_vote': 'data/early_vt.parquet',
  'hist_gen': 'data/history_general/*/*.parquet',
  'hist_oth': 'data/history_other/*/*.parquet',
  'register': 'data/register/*/*.parquet',
  'cps_suppl': 'data/cps_suppl.parquet'
}
partitioned = ['hist_gen', 'hist_oth', 'register']

for k,v in data_dict.items():

  print("Loading {view_name} data...".format(view_name = k))
  opt = ', HIVE_PARTITIONING=1' if k in partitioned else ''
  cvas = template.format(view_name = k, path = v, opts = opt)
  con.execute(cvas)

con.close()
