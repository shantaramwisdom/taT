
vantagep5_contract.hql
vantagep5_contractoption.hql
vantagep5_contractoptionfund.hql



DROP TABLE IF EXISTS ${databasename}.vantagep5_activity;
CREATE EXTERNAL TABLE ${databasename}.vantagep5_activity
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,fkfunddocumentid string
,sourcesystemname string
,contractnumber string
,activitysourcesystemactivityid string
,sourceactivityid string
,activitytype string
,activityamounttype string
,activityeffectivedate DATE
,activityreporteddate DATE
,fundnumber string
,activitygeneralledgerapplicationareacode string
,activitygeneralledgersourcecode string
,activitysourcetransactioncode string
,activitymoneymethod string
,activityfirstyearrenewalindicator string
,activitytypegroup string
,activityamount DECIMAL(18,6)
,activitysourceactivityparentid string
,activitysourceoriginatinguserid string
,activitysourcesuspensereferencenumber string
,activity1035exchangeindicator string
,activitysourceclaimidentifier string
,contractadministrationlocationcode string
,fundsourcefundidentifier string
,activitywithholdingtaxjurisdiction string
,activitysourceaccountingmecode string
,activityreversalcode string
,activitylegalcompanycode string
,activitysourcelegalentitycode string
,activitydepositsourcebatchidentifier string
,activitysourceaccountingdeviator string
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
'${s3bucketname}/${projectname}/curated/vantagep5/vantagep5_activity';
MSCK REPAIR TABLE ${databasename}.vantagep5_activity SYNC PARTITIONS;
