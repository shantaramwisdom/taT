DROP TABLE IF EXISTS ${databasename}.bestow_partycontractrelationship;
CREATE EXTERNAL TABLE ${databasename}.bestow_partycontractrelationship
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 fkcontractdocumentid string,
 fkpartydocumentid string,
 sourcesystemname string,
 contractnumber string,
 contractadministrationlocationcode string,
 sourcepartyid string,
 relationshiptype string
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
 '${s3bucketname}/${projectname}/curated/bestow/bestow_partycontractrelationship';
MSCK REPAIR TABLE ${databasename}.bestow_partycontractrelationship SYNC PARTITIONS;
