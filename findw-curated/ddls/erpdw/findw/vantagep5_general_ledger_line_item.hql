DROP TABLE IF EXISTS ${databasename}.vantagep5_general_ledger_line_item;

CREATE EXTERNAL TABLE ${databasename}.vantagep5_general_ledger_line_item
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 activity_accounting_id string,
 transaction_number string,
 line_number int,
 sourceaccountglid string,
 contractsourcesystemname string,
 contractnumber string,
 contractstatuscode string,
 contractissuedate date,
 contractissuestatecode string,
 contractsourcemarketingorganizationcode string,
 contractsourceaccountingcenterdeviator string,
 contractadministrationlocationcode string,
 contracteffectivedate date,
 contractdurationinyears int,
 contractbillingmode string,
 contractgroupindicator string,
 contractmodelpremium decimal(18,6),
 contractproductfundmortalityandexpensecode string,
 contractproducttypecode string,
 plancode string,
 payoutoptionindicator string,
 activitytypegroup string,
 activitytype string,
 activityamounttype string,
 activitydepositsourcebatchidentifier string,
 activityreversalcode string,
 activitysourceclaimidentifier string,
 activitysourceoriginatinguserid string,
 activitysourcetransactioncode string,
 activitywithholdingtaxjurisdiction string,
 activitywithholdingtaxjurisdictioncountrycode string,
 activitysourceaccountingemcode string,
 activitysourceaccountingdeviator string,
 activityaccountingbalancedentryindicator string,
 activityeffectivedate date,
 activitymoneymethodcode string,
 checknumber string,
 distributionreasoncode string,
 fundclassindicator string,
 fundnumber string,
 fundunits decimal(18,6),
 1035exchangeindicator string,
 default_currency string,
 default_amount decimal(18,2),
 iceinputflag string,
 ifrs17cohort string,
 ifrs17grouping string,
 ifrs17measurementmodel string,
 ifrs17portfolio string,
 ifrs17profitability string,
 ifrs17reportingcashflowtype string,
 modifiedcompanytryflag string,
 statutoryresidentstatecode string,
 statutoryresidentcountrycode string,
 primaryinsuredprimarypostalcode string,
 primaryannuitantnonresidentalientaxstatus string,
 ownernonresidentalientaxstatus string,
 sourceagentidentifier string,
 sourcefundidentifier string,
 sourcelegalentitycode string,
 transactionclass string,
 transactionidertype string,
 transactionchargetype string,
 transactionfeetype string,
 transactiontaxtype string
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
 '${s3bucketname}/${projectname}/curated/erpdw/findw/vantagep5_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.vantagep5_general_ledger_line_item SYNC PARTITIONS;
