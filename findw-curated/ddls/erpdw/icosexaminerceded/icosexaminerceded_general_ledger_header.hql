DROP TABLE IF EXISTS ${databasename}.icosexaminerceded_general_ledger_header;

CREATE EXTERNAL TABLE ${databasename}.icosexaminerceded_general_ledger_header (
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 src_desc string,
 transaction_number string,
 source_system_nm string,
 ledger_name string,
 contractsourcesystemname string,
 contractnumber string,
 event_type_code string,
 subledger_short_name string,
 transaction_date date,
 data_type string,
 gl_reversal_date date
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
 '${s3bucketname}/${projectname}/curated/erpdw/icosexaminerceded/icosexaminerceded_general_ledger_header';
MSCK REPAIR TABLE ${databasename}.icosexaminerceded_general_ledger_header SYNC PARTITIONS;
