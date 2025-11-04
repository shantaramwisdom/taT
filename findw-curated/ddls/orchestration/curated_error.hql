DROP TABLE IF EXISTS ${databasename}.curated_error;
CREATE EXTERNAL TABLE ${databasename}.curated_error
(
recorded_timestamp timestamp,
original_recorded_timestamp timestamp,
original_cycle_date date,
original_batch_id int,
error_classification_name string,
error_message string,
error_record_aging_days int,
error_record string
)
PARTITIONED BY (
table_name string,
reprocess_flag string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/orchestration/curated_error';
MSCK REPAIR TABLE ${databasename}.curated_error SYNC PARTITIONS;
