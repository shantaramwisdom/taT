DROP TABLE IF EXISTS ${databasename}.oraclegl_generalledgerjournaldetail;
CREATE EXTERNAL TABLE ${databasename}.oraclegl_generalledgerjournaldetail
(
 recorded_timestamp timestamp,
 source_system_name string,
 generalledgerledgername string,
 generalledgercode string,
 generalledgerjournaltitledescription string,
 generalledgertransactioncurrencycode string,
 generalledgertransactioncurrencyamount Decimal(18,6),
 generalledgerreportingcurrencycode string,
 generalledgerreportingcurrencyamount Decimal(18,6),
 generalledgerstatisticalamount Decimal(18,6),
 generalledgercurrencyconversiondate date,
 generalledgercurrencyconversionratetype string,
 generalledgercurrencyexchange rate Decimal(18,6),
 generalledgerjournalbatchname string,
 generalledgerjournalname string,
 generalledgerjournallinenumber int,
 generalledgerjournalsource string,
 generalledgerjournalsourcecategory string,
 generalledgeraccountingperiod string,
 generalledgeraccountingyear int,
 generalledgeraccountingmonth int,
 activityreporteddate timestamp,
 generalledgerjournalstatus string,
 generalledgersubledgername string,
 chartofaccountscompanycode string,
 chartofaccountslineofbusinesscode string,
 chartofaccountsdepartmentcode string,
 chartofaccountsaccountcode string,
 chartofaccountsproductcode string,
 chartofaccountsintercompanycode string,
 chartofaccountssummaryproductcode string,
 generalledgerjournalcreator string,
 generalledgerreversalindicator string,
 generalledgerjournallinename string,
 generalledgerjournalcreateddate timestamp,
 generalledgerjournalpostdate timestamp,
 generalledgerjournalapprovalstatus string,
 legalcompanycode string,
 generalledgeraccountnumber string,
 generalledgeraccountsuffix string,
 centernumber string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oraclegl/oraclegl_generalledgerjournaldetail';
MSCK REPAIR TABLE ${databasename}.oraclegl_generalledgerjournaldetail SYNC PARTITIONS;
