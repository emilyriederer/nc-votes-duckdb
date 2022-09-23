import duckdb
import os

if os.path.exists('nc.duckdb'):
  os.remove('nc.duckdb')

con = duckdb.connect('nc.duckdb')
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
  path = os.getcwd() + "\\" + v
  opt = ', HIVE_PARTITIONING=1' if k in partitioned else ''
  query = template.format(view_name = k, path = path, opts = opt)
  con.execute(query)

con.close()
