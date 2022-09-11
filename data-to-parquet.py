import pyarrow as pa
from pyarrow import csv 
import pyarrow.parquet as pq

cols = ['county_id', 'county_desc', 'voter_reg_num', 'ncid', 
        'status_cd', 'voter_status_desc', 'reason_cd', 
        'voter_status_reason_desc', 'res_street_address', 'res_city_desc', 
        'zip_code',  
        'registr_dt', 'race_code', 'ethnic_code', 'party_cd', 'gender_code', 'birth_year', 
        'age_at_year_end', 'birth_state', 'drivers_lic', 'precinct_abbrv', 
        'precinct_desc', 'cong_dist_abbrv']
opts_parse = csv.ParseOptions(delimiter = '\t')
opts_convr = csv.ConvertOptions(null_values = "", 
                                strings_can_be_null = True,
                                include_columns = cols)

with csv.open_csv("data/register.csv", 
                  convert_options=opts_convr, 
                  parse_options = opts_parse) as reader:
    for next_chunk in reader:
        if next_chunk is None:
            break
        pq.write_to_dataset(
                pa.Table.from_batches([next_chunk]),
                root_path = "data/register",
                partition_cols = ['county_id']
        )

with csv.open_csv("data/history.csv", 
                  parse_options = opts_parse) as reader:
    for next_chunk in reader:
        if next_chunk is None:
            break
        pq.write_to_dataset(
                pa.Table.from_batches([next_chunk]),
                root_path = "data/history",
                partition_cols = ['county_id']
        )