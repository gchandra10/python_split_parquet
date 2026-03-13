import duckdb
import os
import shutil
import logging
import time

def split_parquet(input_file, target_size_mb=128, row_group_rows=122880):
    """
    Splits a Parquet file into smaller files using DuckDB, ensuring no data loss, and moves the generated files to the current directory.
    """
    
    total_start = time.perf_counter()
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    temp_dir = f"tmp_split_{base_name}"
    final_dir = os.getcwd()
    
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    try:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        
        logging.info(f"Processing: {input_file}")
        duck_start = time.perf_counter()
        
        with duckdb.connect() as con:
            orig_count = con.execute(f"SELECT count(*) FROM '{input_file}'").fetchone()[0]
            
            con.execute(f"""
                COPY (SELECT * FROM '{input_file}') 
                TO '{temp_dir}' 
                (FORMAT PARQUET, 
                FILE_SIZE_BYTES '{target_size_mb}MB', 
                ROW_GROUP_SIZE {row_group_rows}, 
                COMPRESSION 'ZSTD',
                FILENAME_PATTERN 'split_{base_name}_{{i}}')
            """)
            
            new_count = con.execute(f"SELECT count(*) FROM '{temp_dir}/*.parquet'").fetchone()[0]
            
            if orig_count != new_count:
                raise Exception(f"Data Loss! Expected {orig_count}, got {new_count}")

        duck_duration = time.perf_counter() - duck_start
        logging.info(f"DuckDB split/verify time: {duck_duration:.2f} seconds")

        # 2. File Movement
        move_start = time.perf_counter()
        generated_files = os.listdir(temp_dir)
        for f in generated_files:
            shutil.move(os.path.join(temp_dir, f), os.path.join(final_dir, f))
        
        move_duration = time.perf_counter() - move_start
        
        total_duration = time.perf_counter() - total_start
        logging.info(f"File move time: {move_duration:.2f} seconds")
        logging.info(f"Total execution time: {total_duration:.2f} seconds")

    except Exception as e:
        logging.error(f"Execution failed after {time.perf_counter() - total_start:.2f}s: {e}")
        raise
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


split_parquet('fhvhv_tripdata_2026_01.parquet')