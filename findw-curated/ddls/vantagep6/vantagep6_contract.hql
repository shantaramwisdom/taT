DROP TABLE IF EXISTS ${databasename}.vantagep6_contract;
CREATE EXTERNAL TABLE ${databasename}.vantagep6_contract
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 contractnumber string,
 contractadministrationlocationcode string,
 contractissuedate date,
 contractissuemonth integer,
 contractissueyear integer,
 contracteffectivemonth integer,
 contracteffectiveyear integer,
 contracteffectivedate date,
 contractpayoutstartdate1 date,
 contractpayoutstartmonth1 integer,
 contractpayoutstartyear1 integer,
 contractpayoutstartdate2 date,
 contractpayoutstartmonth2 integer,
 contractpayoutstartyear2 integer,
 contractpayoutenddate1 timestamp,
 contractpayoutenddate2 timestamp,
 contractlastfinancetransactiondate timestamp,
 contractissuage integer,
 contractstatuscode string,
 contractplancode string,
 contractnumberrununiqueid bigint,
 contractgroupindicator string,
 contractresidentstatecode string,
 contractproducttypename string,
 contractreportinglineofbusiness string,
 contractparticipationindicator string,
 contractnumber string,
 sourcesystemlegalentitycode string,
 contractgroup string,
 originatinglegalentitycode string,
 contractlegalcompanycode string,
 contracttotalaccumvalue decimal(18,6),
 surrendervalueamount decimal(18,6),
 contractpolicyyearordertaxwithdrawals decimal(18,6),
 contractdeclinedbenefitreductionmethodindicator integer,
 initialpremium decimal(18,6),
 contracttotalseparateaccountvalue decimal(18,6),
 contractpayoutmode1 string,
 contractpayoutmode2 string,
 contractpayoutamount1 decimal(18,6),
 contractpayoutamount2 decimal(18,6),
 contractpayoutmodecode1 string,
 contractpayoutmodecode2 string,
 contractpayoutoptionindicator1 string,
 contractpayoutoptionindicator2 string,
 contractpayoutpartialwithdrawalmethod1 integer,
 contractpayoutpartialwithdrawalmethod2 integer,
 contractmodelinginsuranceidentifier string,
 contractsourcemarketingorganizationcode string,
 contractoldbusinessunitgroup string,
 contractifrs17portfolio string,
 contractifrs17grouping string,
 contractifrs17cohort string,
 contractifrs17profitability string,
 contractissuerscountrycode string,
 contractissuerstatecode string,
 assigneepartydocumentid string,
 loilionerpartydocumentid string,
 ownerpartydocumentid string,
 primaryannuitantpartydocumentid string,
 primaryinsuredpartydocumentid string,
 secondaryannuitantpartydocumentid string,
 secondaryinsuredpartydocumentid string,
 spousepartydocumentid string
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
 '${s3bucketname}/${projectname}/curated/vantagep6/vantagep6_contract';
MSCK REPAIR TABLE ${databasename}.vantagep6_contract SYNC PARTITIONS;
