import pyarrow as pa
from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow.compute as pc
import shutil
import os

# paths 
path_raw = 'data/raw/ncvhis_Statewide.txt'
path_temp_gen = 'data/history_general_temp'
path_temp_oth = 'data/history_other_temp'
path_dest_gen = 'data/history_general'
path_dest_oth = 'data/history_other'

# read configs
cols_hist = ['election_lbl', 
             'election_desc',
             'voting_method', 
             'voted_party_cd', 
             'ncid']
cols = {'election_lbl': 'dt_election', 
        'election_desc': 'nm_election',
        'voting_method': 'cat_method', 
        'voted_party_cd': 'cd_party', 
        'ncid': 'id_voter'}
cols_hist_dict = [c for c in cols_hist if c != 'ncid']
opts_parse = csv.ParseOptions(delimiter = '\t')
opts_convr_hist = csv.ConvertOptions(null_values = "", 
                                strings_can_be_null = True,
                                include_columns = cols_hist,
                                timestamp_parsers=["%m/%d/%Y"]
                                )
opts_read_hist = csv.ReadOptions(block_size = 250000000)

print('** Begin History Data Conversion **')

if os.path.exists(path_temp_gen):
    shutil.rmtree(path_temp_gen)
if os.path.exists(path_temp_oth):
    shutil.rmtree(path_temp_oth)

with csv.open_csv(path_raw,
                  convert_options = opts_convr_hist,
                  parse_options = opts_parse,
                  read_options = opts_read_hist) as reader:
    i = 0
    for next_chunk in reader:
        if next_chunk is None:
            break
        print(f"Processing chunk {i}")
        tbl = pa.Table.from_batches([next_chunk])
        year = pc.year(tbl['election_lbl'])
        type = pc.replace_substring_regex(tbl['election_desc'], 
                                          pattern = '^\d{2}/\d{2}/\d{4} ', 
                                          replacement = '')
        tbl = tbl.append_column('year', year)
        tbl = tbl.append_column('type', type)
        gen_or_oth = pc.equal(tbl['type'], 'GENERAL')
        pq.write_to_dataset(
                tbl.filter(gen_or_oth),
                root_path = path_temp_gen,
                partition_cols = ['year']
        )
        pq.write_to_dataset(
                tbl.filter(pc.invert(gen_or_oth)),
                root_path = path_temp_oth,
                partition_cols = ['year']
        )
        i += 1

# compact parquet fragments
if os.path.exists(path_dest_gen):
    shutil.rmtree(path_dest_gen)
pq.write_to_dataset(pq.read_table(path_temp_gen),
                    root_path = path_dest_gen, 
                    partition_cols = ['year'],
                    compression = 'SNAPPY',
                    use_dictionary = cols_hist_dict)
shutil.rmtree(path_temp_gen)

if os.path.exists(path_dest_oth):
    shutil.rmtree(path_dest_oth)
pq.write_to_dataset(pq.read_table(path_temp_oth),
                    root_path = path_dest_oth, 
                    partition_cols = ['year'],
                    compression = 'SNAPPY',
                    use_dictionary = cols_hist_dict)
shutil.rmtree(path_temp_oth)
