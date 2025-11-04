DROP TABLE IF EXISTS ${databasename}.bestow_activity;
CREATE EXTERNAL TABLE ${databasename}.bestow_activity
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 fkcontractdocumentid string,
 sourcesystemname string,
 activitycontractid string,
 sourceactivityid string,
 activitysourcesystemactivityid string,
 activityamounttype string,
 activitytype string,
 contractnumber string,
 contractadministrationlocationcode string,
 activityaccountingimbalancedentryindicator string,
 activityamount DECIMAL(18,6),
 activityreporteddate date,
 activityeffectivedate date,
 activitygeneralledgerapplicationareacode string,
 activitygeneralledgersourcecode string,
 activitysourcelegalentitycode string,
 activitylegalcompanycode string,
 activityreversalcode string,
 activitysourcetransactioncode string,
 activitysourceactivityparentid string,
 fundsourcefundidentifier string,
 fundclassindicator string,
 activitysourceaccountingdeviator string,
 activitysourcedepositidentifier string,
 activityfirstyearrenewalindicator string,
 activitytypegroup string,
 activitymoneymethod string,
 activitysourceaccountingmemocode string
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
 '${s3bucketname}/${projectname}/curated/bestow/bestow_activity';
MSCK REPAIR TABLE ${databasename}.bestow_activity SYNC PARTITIONS;