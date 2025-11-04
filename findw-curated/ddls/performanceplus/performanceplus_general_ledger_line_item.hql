DROP TABLE IF EXISTS ${databasename}.performanceplus_general_ledger_line_item;
CREATE EXTERNAL TABLE ${databasename}.performanceplus_general_ledger_line_item
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 activity_accounting_id string,
 transaction_number string,
 line_number int,
 contractsourcesystemname string,
 contractnumber string,
 default_amount decimal(18,2),
 default_currency string,
 activityreversalcode string,
 activitysourcetransactioncode string,
 plancode string,
 checknumber string,
 contractissuestatecode string,
 contractsourcesmarketingorganizationcode string,
 contractdurationinyears int,
 contractsourceaccountingcenterdeviator string,
 contractproducttypecode string,
 transactionclass string,
 sourceagentidentifier string,
 sourcelegalentitycode string,
 managementcode string,
 contractparticipationindicator string,
 majorlineofbusiness string
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
 '${s3bucketname}/${projectname}/curated/erpdw/performanceplus/performanceplus_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.performanceplus_general_ledger_line_item SYNC PARTITIONS;
