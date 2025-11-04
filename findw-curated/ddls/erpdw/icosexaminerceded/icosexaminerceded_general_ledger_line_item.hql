DROP TABLE IF EXISTS ${databasename}.icosexaminerceded_general_ledger_line_item;
CREATE EXTERNAL TABLE ${databasename}.icosexaminerceded_general_ledger_line_item (
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 src_desc string,
 activity_accounting_id string,
 contractsourcesystemname string,
 contractnumber string,
 contractissuedate date,
 contractsourcemarketingorganizationcode string,
 default_amount decimal(18,2),
 default_currency string,
 line_number int,
 orig_gl_center string,
 plancode string,
 reinsuranceassumedcededflag string,
 reinsurancecounterpartypye string,
 reinsurancetreatybasis string,
 reinsurancetreatynumber string,
 sourcesystemreinsurancecounterpartycode string,
 transaction_number string,
 reinsurancetreatycomponentidentifier string,
 reinsurancegroupindividualflag string,
 reinsuranceaccountflag string,
 contractsourceaccountingcenterdeviator string,
 majorlineofbusiness string,
 claimsproductidentifier string,
 contractparticipationindicator string,
 sourcelegalentitycode string,
 transactionalclass string,
 reinsuranceyear string,
 reinsurancemultiplier int
)
PARTITIONED BY (cycle_date date,batch_id int)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
 '${s3bucketname}/${projectname}/curated/erpdw/icosexaminerceded/icosexaminerceded_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.icosexaminerceded_general_ledger_line_item SYNC PARTITIONS;
