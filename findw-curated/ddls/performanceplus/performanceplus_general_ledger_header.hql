DROP TABLE IF EXISTS ${databasename}.performanceplus_general_ledger_header;
CREATE EXTERNAL TABLE ${databasename}.performanceplus_general_ledger_header
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 transaction_number string,
 source_system string,
 ledger_name string,
 event_type_code string,
 subledger_short_name string,
 contractnumber string,
 contractsourcesystemname string,
 transaction_date date,
 gl_reversal_date date,
 data_type string
)
PARTITIONED BY (
 cycle_date date,
 batch_id int
)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
 '${s3bucketname}/${projectname}/curated/erpdw/performanceplus/performanceplus_general_ledger_header';
MSCK REPAIR TABLE ${databasename}.performanceplus_general_ledger_header SYNC PARTITIONS;
