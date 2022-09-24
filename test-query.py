import duckdb

# define some basic queries ----
query1 = """
-- do early voters tend to be more recent or tenured voters?
with 
votes as (select ncid from early_vote where ballot_rtn_status = 'ACCEPTED'),
regis as (select ncid, registr_dt, birth_year, party_cd from register)
select year(registr_dt), count(1) 
from votes left join regis 
on votes.ncid = regis.ncid 
group by 1
order by 2 desc
"""

query2 = """
-- for which party are early voters registered?
with 
votes as (select ncid from early_vote where ballot_rtn_status = 'ACCEPTED'),
regis as (select ncid, registr_dt, birth_year, party_cd from register)
select party_cd, count(1) 
from votes left join regis 
on votes.ncid = regis.ncid 
group by 1
order by 2 desc
"""

con = duckdb.connect('nc.duckdb')

# connect and execute
out = con.execute(query1).fetch_df()
print(out.head())

# execute while logging 
def execute_with_profile(db_con, query):
  db_con.execute("PRAGMA enable_profiling")
  db_con.execute("PRAGMA profiling_output='out.log'")
  tbl = db_con.execute(query).fetchdf()
  db_con.execute("PRAGMA disable_profiling")
  return tbl

out = execute_with_profile(con, query2)
print(out.head())
