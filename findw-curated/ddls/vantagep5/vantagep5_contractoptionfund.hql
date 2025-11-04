DROP TABLE IF EXISTS ${databasename}.vantagep5_contractoptionfund;
CREATE EXTERNAL TABLE ${databasename}.vantagep5_contractoptionfund
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractoptiondocumentid string
,fkcontractdocumentid string
,fkfunddocumentid string
,sourcesystemname string
,contractnumber string
,contractoptionsourcesystemordinalposition INTEGER
,contractoptionfundnumber string
,contractadministrationlocationcode string
,contractoptionfundclassindicator VARCHAR(1)
,contractoptionfundbundledrevenueShareamount DECIMAL(18,8)
,contractoptionfundbundledinvestmentmanagementfeeamount DECIMAL(18,8)
,contractoptionfundbundledrevenueshareamount DECIMAL(18,8)
,contractoptionfundbundledinvestmentmanagementfeeamount DECIMAL(18,8)
,contractoptionfundbundledfundfacilitationfeeamount DECIMAL(18,8)
,contractoptionfundbundledgainlossamount DECIMAL(18,8)
,contractoptionfundsaferenvenueshareamount DECIMAL(18,8)
,contractoptionfundsafeinvestmentmanagementfeeamount DECIMAL(18,8)
,contractoptionfundgapvalue DECIMAL(18,6)
,contractoptionfundvalue DECIMAL(18,6)
,contractoptionfunddefaultfundindicator string
,contractoptionfundseparateaccountvalueindex1 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex2 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex3 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex4 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex5 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex6 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex7 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex8 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex9 DECIMAL(18,8)
,contractoptionfundseparateaccountvalueindex10 DECIMAL(18,8)
,contractoptionfundsharefundvalue DECIMAL(18,8)
,contractoptionfunddynamicseparateaccountvalue DECIMAL(18,8)
,contractoptionfundsafeshgingseparateaccountvalue DECIMAL(18,8)
,initialpremium DECIMAL(18,6)
,surrendervalueamount DECIMAL(18,6)
,modelingrenewalpremium DECIMAL(18,6)
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
'${s3bucketname}/${projectname}/curated/vantagep5/vantagep5_contractoptionfund';
MSCK REPAIR TABLE ${databasename}.vantagep5_contractoptionfund SYNC PARTITIONS;
