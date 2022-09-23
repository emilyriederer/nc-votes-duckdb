import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow.compute as pc
import shutil
import os

# paths 
path_raw = 'data/raw/ncvoter_Statewide.txt'
path_temp = 'data/register_temp'
path_dest = 'data/register'

# read configs
cols_reg = ['county_id', 
            'county_desc', 
            'ncid', 
            'status_cd', 
            'voter_status_desc', 
            'reason_cd', 
            'voter_status_reason_desc', 
            'registr_dt', 
            'race_code', 
            'ethnic_code', 
            'party_cd', 
            'gender_code', 
            'birth_year', 
            'birth_state', 
            'drivers_lic', 
            'precinct_abbrv', 
            'precinct_desc', 
            'cong_dist_abbrv']
cols_reg_excl = ['ncid', 'registr_dt', 'birth_year', 'drivers_lic']
cols_reg_dict = [c for c in cols_reg if c not in ['ncid', 'drivers_lic']]
opts_parse = csv.ParseOptions(delimiter = '\t')
opts_convr_reg = csv.ConvertOptions(null_values = "", 
                                strings_can_be_null = True,
                                include_columns = cols_reg,
                                timestamp_parsers=["%m/%d/%Y"])
opts_read_reg = csv.ReadOptions(block_size = 250000000)

print('** Begin Registration Data Conversion **')

# convert to hive-partitioned parquet
if os.path.exists(path_temp):
    shutil.rmtree(path_temp)

with csv.open_csv(path_raw, 
                  convert_options= opts_convr_reg, 
                  parse_options = opts_parse,
                  read_options = opts_read_reg) as reader:

    i = 0
    for next_chunk in reader:
        if next_chunk is None:
            break
        tbl = pa.Table.from_batches([next_chunk])
        pq.write_to_dataset(
                tbl,
                root_path = path_temp,
                use_dictionary = cols_reg_dict,
                partition_cols= ['county_id']
        )
        print(i)
        i += 1

# compact parquet fragments
if os.path.exists(path_dest):
    shutil.rmtree(path_dest)

pq.write_to_dataset(pq.read_table(path_temp), 
                    root_path = path_dest, 
                    compression = 'SNAPPY',
                    use_dictionary = cols_reg_dict,
                    partition_cols = ['county_id'])

# clean-up
shutil.rmtree(path_temp)
