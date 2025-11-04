DROP TABLE IF EXISTS ${databasename}.genesysifrs17_general_ledger_line_item;

CREATE EXTERNAL TABLE ${databasename}.genesysifrs17_general_ledger_line_item
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 activity_accounting_id string,
 transaction_number string,
 line_number int,
 activitydepositsourcebatchidentifier string,
 contractsourcesystemname string,
 checknumber string,
 contractnumber string,
 debit_credit_indicator string,
 default_amount decimal(18,2),
 groupcontractnumber string,
 ifrs17cohort string,
 ifrs17grouping string,
 ifrs17measurementmodel string,
 ifrs17portfolio string,
 ifrs17profitability string,
 ifrs17reportingcashflowtype string,
 orig_gl_account string,
 orig_gl_center string,
 orig_gl_company string,
 plancode string,
 statutoryresidentcountrycode string,
 statutoryresidentstatecode string,
 sourcesystemactivitydescription string
)
PARTITIONED BY (cycle_date date,batch_id int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
 '${s3bucketname}/${projectname}/curated/erpdw/genesysifrs17/genesysifrs17_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.genesysifrs17_general_ledger_line_item SYNC PARTITIONS;
