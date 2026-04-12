-- DataFusion table setup for correctness verification.
-- Usage: datafusion-cli -f create_df_table.sql
-- The DATA_PATH placeholder must be replaced with the actual parquet location
-- before running, or use the run_correctness.sh script which handles this.

SET datafusion.execution.listing_table_ignore_subdirectory = false;

CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION '__DATA_PATH__'
OPTIONS ('binary_as_string' 'true');
