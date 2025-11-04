DROP TABLE IF EXISTS ${databasename}.arr_general_ledger_line_item;
CREATE EXTERNAL TABLE ${databasename}.arr_general_ledger_line_item
(
recorded_timestamp timestamp,
source_system_name string,
original_cycle_date date,
original_batch_id int,
src_desc string,
activity_accounting_id string,
transaction_number string,
line_number int,
contractnumber string,
contractsourcesystemname string,
default_amount decimal(18,2),
debit_credit_indicator string,
orig_gl_company string,
orig_gl_account string,
orig_gl_center string,
ifrs17cohort string,
ifrs17grouping string,
ifrs17portfolio string,
ifrs17profitability string,
activityreversalcode string,
contractissuedatecode string,
plancode string,
sessionidentifier string,
contractissueage integer,
contractdurationinyears integer,
contractissuedate date,
contractparticipationindicator string,
reinsuranceaccountflag string,
reinsuranceassumedcededflag string,
reinsurancecounterpartytype string,
reinsurancegroupindividualflag string,
reinsurancetreatybasis string,
reinsurancetreatycomponentidentifier string,
reinsurancetreatynumber string,
sourcelegalentitycode string,
sourcesystemreinsurancecounterpartycode string,
statutoryresidentcountrycode string,
statutoryresidentstatecode string,
typeofbusinessinsured string,
reinsurancemultiplier string,
treatyruleset string,
transactionclass string
)
PARTITIONED BY (
cycle_date date,
batch_id int)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/curated/erpdw/arr/arr_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.arr_general_ledger_line_item SYNC PARTITIONS;
