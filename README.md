```
pip install pyarrow==9.0.*
wget -O duckdb.zip https://github.com/duckdb/duckdb/releases/download/v0.5.0/duckdb_cli-linux-amd64.zip
unzip duckdb.zip
duckdb
duckdb nc.duckdb 'select 1 as n'
```


```
chmod +x data.sh
data.sh
```

```
duckdb
D> select * from read_parquet('data/register/*/*.parquet', HIVE_PARTITIONING = 1) limit 10;
```
