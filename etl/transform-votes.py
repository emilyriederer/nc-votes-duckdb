import os
from pyarrow import csv
import pyarrow.parquet as pq

# columns to read in
cols_ev = ['ncid', 
           'county_desc', 
           'race', 
           'ethnicity', 
           'gender', 
           'age', 
           'voter_city', 
           'voter_zip',
           'ballot_mail_city', 
           'ballot_mail_zip',
           'relative_request_city', 
           'relative_request_zip',
           'election_dt', 
           'voter_party_code', 
           'precinct_desc', 
           'cong_dist_desc', 
           'nc_house_desc', 
           'nc_senate_desc',
           'ballot_req_delivery_type',
           'ballot_req_type',
           'ballot_request_party',
           'ballot_req_dt',
           'ballot_send_dt',
           'ballot_rtn_dt',
           'ballot_rtn_status',
           'mail_veri_status']

'''
cols_keep = {
           'ncid': 'id_nc', 
           'county_desc': 'nm_county', 
           'race': 'cd_race', 
           'ethnicity': 'cd_ethnicity', 
           'gender': 'cd_gender', 
           'age': 'n_age', 
           'voter_city': 'voter_city', 
           'voter_zip': 'voter_zip',
           'ballot_mail_city': 'ballot_city', 
           'ballot_mail_zip': 'ballot_zip',
           'election_dt': 'dt_election', 
           'voter_party_code': 'cd_party', 
           'precinct_desc': 'nm_precincty', 
           'cong_dist_desc': 'nm_house_federal', 
           'nc_house_desc': 'nm_house_state', 
           'nc_senate_desc': 'nm_senate_state',
           'ballot_req_delivery_type': 'cd_ballot_delivery',
           'ballot_req_type': 'cd_ballot_req',
           'ballot_request_party': 'cd_party_ballot',
           'ballot_req_dt': 'dt_ballot_requet',
           'ballot_send_dt': 'dt_ballot_send',
           'ballot_rtn_dt': 'dt_ballot_return',
           'ballot_rtn_status': 'cd_ballot_status',
           'mail_veri_status': 'cd_ballot_status_mail'}
'''

# columns to exclude from dict encoding
cols_ex = ['ncid', 
           'age', 
           'ballot_req_dt',
           'ballot_send_dt',
           'ballot_rtn_dt']

opts_ev = csv.ConvertOptions(null_values = "", 
                                strings_can_be_null = True,
                                include_columns = cols_ev,
                                timestamp_parsers=["%m/%d/%Y"]
                                )

tbl = csv.read_csv('data/raw/absentee_20221108.csv', 
                   convert_options = opts_ev)
pq.write_table(tbl, 
               'data/early_vt.parquet', 
               use_dictionary = list(set(cols_ev) - set(cols_ex)))
