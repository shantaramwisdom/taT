DROP TABLE IF EXISTS ${databasename}.ltcg_partycontractrelationship;
CREATE EXTERNAL TABLE ${databasename}.ltcg_partycontractrelationship
(
recorded_timestamp timestamp,
source_system_name string,
documentid string,
fkcontractdocumentid string,
fkpartydocumentid string,
sourcepartyid string,
relationshiptype string,
sourcesystemname string,
contractnumber string,
contractadministrationlocationcode string
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_partycontractrelationship';
MSCK REPAIR TABLE ${databasename}.ltcg_partycontractrelationship SYNC PARTITIONS;
