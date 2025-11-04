DROP TABLE IF EXISTS ${databasename}.ltcg_activity;
CREATE EXTERNAL TABLE ${databasename}.ltcg_activity
(
recorded_timestamp timestamp
,source_system_name string
,sourcecd string
,documentid string
,fkclaimbenefitdocumentid string
,fkclaimdocumentid string
,activityamounttype string
,activitytype string
,sourceactivityid string
,contractnumber string
,contractadministrationlocationcode string
,activityaccountingbalancetryindicator string
,activityaccountingid string
,activityamount decimal(18,6)
,activitydepositsourcebatchidentifier string
,activityeffectivedate date
,activitygeneralledgerallocationareacode string
,activitygeneralledgersourcecode string
,activitymoneymethod string
,activityreporteddate date
,activityreversalcode string
,activitysourceaccountingdeviator string
,activitysourceaccountingitemcode string
,activitysourcedepositidentifier string
,activitysourcedisbursementidentifier string
,activitysourceoriginatinguserid string
,activitysourceparentsuspenseidentifier string
,activitysourcesuspensematchingreferencenumber string
,activitysourcesuspenseperson string
,activitysourcesuspensereason string
,activitysourcesystemaccountinstruction string
,activitysourcetransactioncode string
,claimbenefitlineindicator int
,fundclassindicator string
,fundnumber string
,activitylegalcompanycode string
,activitysourceactivityparentid string
,activitysourceclaimid string
,fundsourcefundidentifier string
,activitysourcesystemactivityid string
,activitytaxreportingeffectivedate date
,activitypaymentupdatedate date
,activitychecknumber string
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_activity';
MSCK REPAIR TABLE ${databasename}.ltcg_activity SYNC PARTITIONS;

