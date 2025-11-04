DROP TABLE IF EXISTS ${databasename}.erpdw_generalledgertrialbalance;
CREATE EXTERNAL TABLE ${databasename}.erpdw_generalledgertrialbalance
(
recorded_timestamp timestamp,
source_system_name string,
documentid string,
sourcesystemname string,
generalledgeraccountingperiod string,
generalledgerledgername string,
chartofaccountscompanycode string,
chartofaccountsbusinesscode string,
chartofaccountsdepartmentcode string,
chartofaccountsaccountcode string,
chartofaccountsproductcode string,
chartofaccountsintercompanycode string,
chartofaccountssummaryproductcode string,
generalledgertransactioncurrencycode string,
generalledgeraccountingyear Int,
generalledgeraccountingmonth Int,
geacledgercode string,
geacledgerdescription string,
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
trialbalancetransactionnetbalance Decimal(18,6),
generalledgeraccountsuffix string,
generalledgeraccountnumber string,
centernumber string,
generalledgerlineofbusinessdescription string,
generalledgeraccountdescription string,
centerdescription string
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
'${s3bucketname}/${projectname}/curated/erpdw/erpdw/erpdw_generalledgertrialbalance';
MSCK REPAIR TABLE ${databasename}.erpdw_generalledgertrialbalance SYNC PARTITIONS;
