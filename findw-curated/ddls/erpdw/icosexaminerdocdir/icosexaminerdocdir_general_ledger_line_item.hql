DROP TABLE IF EXISTS ${databasename}.icosexaminerdocdir_general_ledger_line_item;

CREATE EXTERNAL TABLE ${databasename}.icosexaminerdocdir_general_ledger_line_item (
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 src_desc string,
 activity_accounting_id string,
 transaction_number string,
 line_number int,
 activitysourceclaimidentifier string,
 activitywithholdingtaxjurisdiction string,
 activitywithholdingtaxjurisdictioncountrycode string,
 contractsourcesystemname string,
 contractnumber string,
 default_amount decimal(18,2),
 default_currency string,
 plancode string,
 reinsuranceassumedcededflag string,
 reinsurancecounterpartypye string,
 reinsurancetreatybasis string,
 reinsurancetreatynumber string,
 reinsurancetreatycomponentidentifier string,
 reinsurancegroupindividualflag string,
 reinsuranceaccountflag string,
 transactionclass string,
 treatyruleset string,
 reinsurancecmultiplier string,
 statutoryresidentcountrycode string,
 statutoryresidentstatecode string,
 sourcesystemreinsurancecounterpartycode string,
 contractsourceaccountingcenterdeviator string,
 sourcelegalentitycode string,
 majorlineofbusiness string,
 contractparticipationindicator string,
 claimsproductidentifier string,
 contractissuage int
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
 '${s3bucketname}/${projectname}/curated/erpdw/icosexaminerdocdir/icosexaminerdocdir_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.icosexaminerdocdir_general_ledger_line_item SYNC PARTITIONS;
