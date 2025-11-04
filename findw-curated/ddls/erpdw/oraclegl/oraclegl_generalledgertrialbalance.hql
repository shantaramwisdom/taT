DROP TABLE IF EXISTS ${databasename}.oraclegl_generalledgertrialbalance;
CREATE EXTERNAL TABLE ${databasename}.oraclegl_generalledgertrialbalance
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 generalledgeraccountingperiod string,
 generalledgerledgername string,
 chartofaccountscompanycode string,
 chartofaccountslineofbusinesscode string,
 chartofaccountsdepartmentcode string,
 chartofaccountsaccountcode string,
 chartofaccountsproductcode string,
 chartofaccountsintercompanycode string,
 chartofaccountssummaryproductcode string,
 generalledgertransactioncurrencycode string,
 generalledgeraccountingyear Int,
 generalledgeraccountingmonth Int,
 glscaledgercode string,
 glscaledgerdescription string,
 legalcompanycode string,
 legalcompanyname string,
 generalledgerreportingcurrencycode string,
 trialbalancetransactionbeginningbalance Decimal(18,6),
 trialbalancetransactiontotaldebits Decimal(18,6),
 trialbalancetransactiontotalcredits Decimal(18,6),
 trialbalancetransactionendingbalance Decimal(18,6),
 trialbalancereportingbeginningbalance Decimal(18,6),
 trialbalancereportingtotaldebits Decimal(18,6),
 trialbalancereportingtotalcredits Decimal(18,6),
 trialbalancereportingendingbalance Decimal(18,6),
 trialbalancestatisticalbeginningbalance Decimal(18,6),
 trialbalancestatisticaltotaldebits Decimal(18,6),
 trialbalancestatisticaltotalcredits Decimal(18,6),
 trialbalancestatisticalendingbalance Decimal(18,6),
 generalledgeraccountsuffix string,
 generalledgeraccountnumber string,
 centernumber string,
 generalledgerlineofbusinessdescription string,
 generalledgeraccountdescription string,
 centerdescription string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oraclegl/oraclegl_generalledgertrialbalance';
MSCK REPAIR TABLE ${databasename}.oraclegl_generalledgertrialbalance SYNC PARTITIONS;
