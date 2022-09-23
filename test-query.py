import duckdb

con = duckdb.connect('nc.duckdb')

query1 = """
-- how many general elections has each early voter participated in?
with 
votes as (select ncid from early_vote where ballot_rtn_status = 'ACCEPTED'),
hist as (select * from hist_gen where year(election_lbl) in (2012, 2014, 2016, 2018, 2020)),
freq as (
select
  votes.ncid,
  min(election_lbl) as min_date,
  max(election_lbl) as max_date,
  count(*) as n_votes
from 
  votes left join hist
  on
  votes.ncid = hist.ncid
group by 1
)
select n_votes, count(1) from freq group by 1 order by 1
"""

query2 = """
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

query3 = """
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

query4 = """
-- which counties have the most early votes?
with 
votes as (select ncid from early_vote where ballot_rtn_status = 'ACCEPTED'),
regis as (select ncid, county_desc, registr_dt, birth_year, party_cd from register)
select county_desc, count(1) 
from votes left join regis 
on votes.ncid = regis.ncid 
group by 1
order by 2 desc
"""
out = con.execute(query).fetch_df()
print(out.head())

'''
def run_and_profile_query(query):
  con.execute("PRAGMA enable_profiling")
  con.execute("PRAGMA profiling_output='out.log'")
  con.execute(query)
  con.execute("PRAGMA disable_profiling")
  with open('out.log', 'r') as f:
    output = f.read()
  print(output)
  
run_and_profile_query(query)
'''