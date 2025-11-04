DROP TABLE IF EXISTS ${databasename}.vantagep5_contract;
CREATE EXTERNAL TABLE ${databasename}.vantagep5_contract
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,sourcesystemname string
,contractnumber string
,contractadministrationlocationcode string
,contractissuedate DATE
,contractissuemonth INTEGER
,contractissueyear INTEGER
,contracteffectiveyear INTEGER
,contracteffectivemonth INTEGER
,contracteffectivedate DATE
,contractissueage INTEGER
,contractdurationinyears INTEGER
,contractgroupindicator string
,contractparticipationindicator string
,contractreportinglineofbusiness string
,contractresidentstatecode string
,contractstatuscode string
,contractplancode string
,contractlastfinancetransactiondate TIMESTAMP
,contractproducttypename string
,contractformcodespecialneeds string
,contractnumbernumericid BIGINT
,centernumber string
,sourcelegalentitycode string
,contractqualifiedindicator string
,contractadagroup string
,contractlegalcompanycode string
,originatinglegalentitycode string
,contracttotalaccountvalue DECIMAL(18,6)
,surrendervalueamount DECIMAL(18,6)
,contractcumulativewithdrawals DECIMAL(18,6)
,contractpolicyyeartodatecumulativewithdrawals DECIMAL(18,6)
,contracttotalseparateaccountvalue DECIMAL(18,6)
,modelingrenewalpremium DECIMAL(18,6)
,contractfreewithdrawalpercentage DECIMAL(18,6)
,contractsurrenderchargepercentage DECIMAL(18,6)
,initialpremium DECIMAL(18,6)
,contractfundvalueat8 BIGINT
,contractsourceMarketingorganizationcode string
,contractmodelingbusinessunitgroup string
,contractifrs17portfolio string
,contractifrs17grouping string
,contractifrs17cohort string
,contractifrs17profitability string
,contractmodelingreinsuranceidentifier string
,assigneepartydocumentid string
,jointownerpartydocumentid string
,ownerpartydocumentid string
,primaryannuitantpartydocumentid string
,primaryinsuredpartydocumentid string
,secondaryannuitantpartydocumentid string
,secondaryinsuredpartydocumentid string
,spousepartydocumentid string
,contractissuestatuscode string
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
'${s3bucketname}/${projectname}/curated/vantagep5/vantagep5_contract';
MSCK REPAIR TABLE ${databasename}.vantagep5_contract SYNC PARTITIONS;