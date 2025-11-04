DROP TABLE IF EXISTS ${databasename}.erpdw_generalledgerjournaldetail;
CREATE EXTERNAL TABLE ${databasename}.erpdw_generalledgerjournaldetail
(
recorded_timestamp timestamp,
source_system_name string,
generalledgerledgername string,
geacledgercode string,
generalledgerjournalnamedescription string,
generalledgerjournallinedescription string,
generalledgertransactioncurrencycode string,
generalledgertransactioncurrencyamount Decimal(18,6),
generalledgerreportingcurrencycode string,
generalledgerreportingcurrencyamount Decimal(18,6),
generalledgerstatisticalamount Decimal(18,6),
generalledgercurrencyconversiondate date,
generalledgercurrencyconversionrate string,
generalledgerreportingcurrencyexchangerate Decimal(18,6),
generalledgerjournalbatchname string,
generalledgerjournalname string,
generalledgerjournallinenumber int,
generalledgerjournalsource string,
generalledgerjournalsourcecategory string,
generalledgeraccountingperiod string,
generalledgeraccountingyear int,
generalledgeraccountingmonth int,
activityreportdate timestamp,
generalledgerjournalstatus string,
generalledgersubledgername string,
chartofaccountscompanycode string,
chartofaccountsbusinesscode string,
chartofaccountsdepartmentcode string,
chartofaccountsaccountcode string,
chartofaccountsproductcode string,
chartofaccountsintercompanycode string,
chartofaccountssummaryproductcode string,
generalledgerreversalindicator string,
generalledgeroriginaljournalname string,
generalledgerreversaldate timestamp,
generalledgerjournalcreateddate timestamp,
generalledgerjournalpostdate timestamp,
generalledgerapprovalstatus string,
legalcompanycode string,
generalledgeraccountnumber string,
generalledgeraccountsuffix string,
centernumber string
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
'${s3bucketname}/${projectname}/curated/erpdw/erpdw/erpdw_generalledgerjournaldetail';
MSCK REPAIR TABLE ${databasename}.erpdw_generalledgerjournaldetail SYNC PARTITIONS;
