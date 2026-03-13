# Split Parquet File

Uses duckdb library for splitting the file into smaller chunks.

`target_size_mb = 128MB`

Setting to default block size.

`row_group_rows` = 122,880

122K rows per Row Group is general best practice. If Spark results in OOM, 61,440 can be used to reduce the row group size.

`ZSTD` Compression.

Snappy compression is fast and good for standard tables. For wide tables ZSTD offers better compression. As we are reducing the file size to 128MB CPU usage will not be high.

----

[Download data from NYC Taxi Website](https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2026-01.parquet)

----

## Sample results

```uv run python main.py```

> Processing: fhvhv_tripdata_2026_01.parquet
> 
> DuckDB split/verify time: 2.30 seconds
> 
> File move time: 0.00 seconds
> 
> Total execution time: 2.31 seconds

----

```ls -lh *.parquet```

**Original File**

-rw-r--r--@ 1 staff   **482M** Mar 12 20:53 fhvhv_tripdata_2026_01.parquet

**After Splitting**

> -rw-r--r--  1 staff    26M Mar 12 22:40 split_fhvhv_tripdata_2026_01_0.parquet
> 
> -rw-r--r--  1 staff   138M Mar 12 22:40 split_fhvhv_tripdata_2026_01_1.parquet
> 
> -rw-r--r--  1 staff   126M Mar 12 22:40 split_fhvhv_tripdata_2026_01_2.parquet
> 
> -rw-r--r--  1 staff   130M Mar 12 22:40 split_fhvhv_tripdata_2026_01_3.parquet
> 
> -rw-r--r--  1 staff    77M Mar 12 22:40 split_fhvhv_tripdata_2026_01_4.parquet
