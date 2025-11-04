DROP TABLE IF EXISTS ${databasename}.vantagep5_party;
CREATE EXTERNAL TABLE ${databasename}.vantagep5_party
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,sourcesystemname string
,contractnumber string
,contractadministrationlocationcode string
,partyfirstname string
,partylastname string
,dateofbirth DATE
,companyindividualcode string
,gender string
,partylocationstatecode string
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
'${s3bucketname}/${projectname}/curated/vantagep5/vantagep5_party';
MSCK REPAIR TABLE ${databasename}.vantagep5_party SYNC PARTITIONS;
