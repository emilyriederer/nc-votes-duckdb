mkdir data

# data: voter history #
wget -O data/history.zip https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvhis_Statewide.zip
unzip data/history.zip -d data
rm data/history.zip
mv data/ncvhis_Statewide.txt data/history.csv

# data: voter registration #
wget -O data/registered.zip https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip
unzip data/registered.zip -d data
rm data/registered.zip
mv data/ncvoter_Statewide.txt data/register.csv

# data: absentee voters #
wget -O data/absentee.csv https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2022_11_08/absentee_20221108.csv

# set-up database #
wget -O duckdb.zip https://github.com/duckdb/duckdb/releases/download/v0.5.0/duckdb_cli-linux-amd64.zip
unzip duckdb.zip 
rm duckdb.zip