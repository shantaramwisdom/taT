DROP TABLE IF EXISTS ${databasename}.lifepro119_general_ledger_header;

CREATE EXTERNAL TABLE ${databasename}.lifepro119_general_ledger_header
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 transaction_number string,
 source_system_nm string,
 ledger_name string,
 event_type_code string,
 subledger_short_name string,
 transaction_date date,
 secondary_ledger_code string,
 data_type string,
 contractnumber string,
 contractsourcesystemname string
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
 '${s3bucketname}/${projectname}/curated/erpdw/lifepro119/lifepro119_general_ledger_header';
MSCK REPAIR TABLE ${databasename}.lifepro119_general_ledger_header SYNC PARTITIONS;
