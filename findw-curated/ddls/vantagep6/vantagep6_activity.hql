DROP TABLE IF EXISTS ${databasename}.vantagep6_activity;
CREATE EXTERNAL TABLE ${databasename}.vantagep6_activity
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 ifxcontractdocumentid string,
 ifxfunddocumentid string,
 sourcesystemname string,
 contractnumber string,
 activitysourcesystemactivityid string,
 sourceactivityid string,
 activitytype string,
 activityamounttype string,
 activityeffectivedate date,
 activityreporteddate date,
 fundnumber string,
 activitygeneralledgerapplicationareacode string,
 activitygeneralledgersourcecode string,
 activitysourcetransactioncode string,
 activitymoneymethod string,
 activityfirstyearrenewalindicator string,
 activitytypegroup string,
 activityamount decimal(18,6),
 activitysourcesystempaymentid string,
 activitysourcesystemagentissuerid string,
 activitysourcesuspensenreferencenumber string,
 activity1035exchangeindicator string,
 activitysourceclaimidentifier string,
 contractadministrationlocationcode string,
 fundsourcefundidentifier string,
 activitywithholdingtaxjurisdiction string,
 activitysourceaccountingunitecode string,
 activityreversalcode string,
 activitylegalcompanycode string,
 activitysourcelegalentitycode string,
 activitydepositsourcebatchidentifier string,
 activitysourceaccountingdeviator string
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
 '${s3bucketname}/${projectname}/curated/vantagep6/vantagep6_activity';
MSCK REPAIR TABLE ${databasename}.vantagep6_activity SYNC PARTITIONS;
