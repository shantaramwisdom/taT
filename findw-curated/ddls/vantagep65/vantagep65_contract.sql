DROP TABLE IF EXISTS ${databasename}.vantagep65_contract;
CREATE EXTERNAL TABLE ${databasename}.vantagep65_contract
(
 recorded_timestamp timestamp
,source_system_name string
,assignedepartrydocumentid string
,jointpartydocumentid string
,primaryannuitantpartydocumentid string
,primaryinsuredpartydocumentid string
,secondaryannuitantpartydocumentid string
,secondaryinsuredpartydocumentid string
,spousetpartydocumentid string
,spousesystemname string
,contractnumber string
,contractadministrationlocationcode string
,contractissuedate DATE
,contractissueyear INTEGER
,contractissueage INTEGER
,contractpayoutstartdate1 DATE
,contractpayoutstartdate2 DATE
,contractpayoutstartmonth1 INTEGER
,contractpayoutstartmonth2 INTEGER
,contractpayoutstartyear1 INTEGER
,contractpayoutstartyear2 INTEGER
,contractpayoutenddate1 DATE
,contractpayoutenddate2 DATE
,contractpayoutenddate1 TIMESTAMP
,contractpayoutenddate2 TIMESTAMP
,contracteffectivemonth INTEGER
,contracteffectiveyear INTEGER
,contracteffectivedate DATE
,contractimmediatecommencementdate TIMESTAMP
,contractlastfinancetransactiondate TIMESTAMP
,originalinsurancesourcename string
,originalsourcesystemname string
,contractissueage INTEGER
,contractstatuscode string
,contractplancode string
,contractnumbernumericid BIGINT
,contractsourceproductspecialindicator string
,contractrowindicator string
,contractdurationinyears INTEGER
,contractresidentstatecode string
,contractproducttypename string
,contractreportinglineofbusiness string
,contractspecialindicator string
,contractparticipationindicator string
,contractfirstsimestateproductgrouping string
,contractfirstsourcecode INTEGER
,contractcustomeraccounttype string
,contractsourcequalifierindicator string
,contractlegalcompanycode string
,contractpayoutamount DECIMAL(18,6)
,contractpayoutamount2 DECIMAL(18,6)
,contractpayoutmodel string
,contractpayoutmodel2 string
,contractpayoutmemo string
,contractpayoutmemo2 string
,contractpayoutpositionindicator string
,contractpayoutpositionindicator2 string
,contractpayoutpartialwithdrawalmethod INTEGER
,contractmodelinglinebusinessunitgroup string
,contractsourcemarketingorganizationcode string
,contractfirst7portfolio string
,contractfirst7grouping string
,contractfirst7cohort string
,contractfirst7profitability string
,first7contractissuetennyearinterest rate DECIMAL(18,6)
,contractmodelingreinsuranceidentifier string
,contractissuecountrycode string
,contractissuestatecode string
,contracttotalaccountvalue DECIMAL(18,6)
,surrendervalueamount DECIMAL(18,6)
,contractcurrentamortizationremainingterm INTEGER
,contractcumulativewithdrawals DECIMAL(18,6)
,contractpolicyyearcumulativewithdrawals DECIMAL(18,6)
,contractchargepercent DECIMAL(18,6)
,contractchargeremaining DECIMAL(18,6)
,contracttotalseparateaccountvalue DECIMAL(18,6)
,contractrollingrenewalpremium DECIMAL(18,6)
,contractcommittedpremium DECIMAL(18,6)
,contractcalculatedsurrendervalueamount DECIMAL(18,6)
,initialpremium DECIMAL(18,6)
,contractsurrenderchargepercentage DECIMAL(18,6)
,contractfreewithdrawalpercentage DECIMAL(18,6)
,contractfundvalue BIGINT
,contractcumulativepremium DECIMAL(18,6)
,contractsourcepremiumsourcecode string
,contractunaccruedcreditadvantagefee DECIMAL(18,6)
)
PARTITIONED BY (
 cycle_date date,
 batch_id int)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';
