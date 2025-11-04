DROP TABLE IF EXISTS ${databasename}.vantagep65_activity;
CREATE EXTERNAL TABLE ${databasename}.vantagep65_activity
(
 recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,fkfunddocumentid string
,fksourcestystemid string
,sourceactivityid string
,activityunittype string
,activitytype string
,contractnumber string
,contractadministrationlocationcode string
,activitysourcestystemactivityid string
,activityeffectivedate DATE
,activityreportdate DATE
,fundnumber string
,activitygeneralledgerapplicationareacode string
,activitygeneralledgerglsourcecode string
,activitysourcetransactioncode string
,activitymoneymethod string
,activityfirstyearrenewalindicator string
,activitytypegroup string
,activityunits DECIMAL(18,6)
,activitysourceactivityparentid string
,activitysourceoriginatinguserid string
,activitysourcesuspensereference number string
,activity1035exchangeindicator string
,activitysourceclaimidentifier string
,fundsourcefundidentifier string
,activitywithholdingtaxtjurisdiction string
,activitysourceaccountingsegnocode string
,activityreversalcocode string
,activitylegalcompanycode string
,activitysourcelegalentitycode string
,activityoriginalsourcesbatchidentifier string
,activitysourceaccounttype string
,activityunits DECIMAL(18,6)
,activityresidentstatecode string
)
PARTITIONED BY (
 cycle_date date,
 batch_id int)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${bucketname}/${projectname}/curated/vantagep65/vantagep65_activity';
