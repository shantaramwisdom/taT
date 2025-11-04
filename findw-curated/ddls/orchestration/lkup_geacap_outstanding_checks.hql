DROP TABLE IF EXISTS ${databasename}.lkup_geacap_outstanding_checks;
CREATE EXTERNAL TABLE ${databasename}.lkup_geacap_outstanding_checks (
bankaccount string,
paymentamount decimal(15,2),
paymentdate date,
transactiontype string,
reference string,
businessunit string,
accounttransaction string,
cashaccountsegment1 string,
cashaccountsegment2 string,
cashaccountsegment3 string,
cashaccountsegment4 string,
cashaccountsegment5 string,
cashaccountsegment6 string,
cashaccountsegment7 string,
cashaccountsegment8 string,
cashaccountsegment9 string,
cashaccountsegment10 string,
offsetaccountsegment1 string,
offsetaccountsegment2 string,
offsetaccountsegment3 string,
offsetaccountsegment4 string,
offsetaccountsegment5 string,
offsetaccountsegment6 string,
offsetaccountsegment7 string,
offsetaccountsegment8 string,
offsetaccountsegment9 string,
offsetaccountsegment10 string,
description string,
valuedate date,
clearingdate date,
attributecategory string,
textattribute1 string,
textattribute2 string,
textattribute3 string,
textattribute4 string,
textattribute5 string,
textattribute6 string,
textattribute7 string,
textattribute8 string,
textattribute9 string,
textattribute10 string,
sourcebankaccount string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
'separatorChar'=',',
'quoteChar'='"')
STORED AS TEXTFILE
LOCATION
'${s3bucketname}/${projectname}/orchestration/lkup_geacap_outstanding_checks'
TBLPROPERTIES (
'skip.header.line.count'='1');
MSCK REPAIR TABLE ${databasename}.lkup_geacap_outstanding_checks SYNC PARTITIONS;