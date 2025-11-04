DROP TABLE IF EXISTS ${databasename}.ltcg_partyclaimrelationship;
CREATE EXTERNAL TABLE ${databasename}.ltcg_partyclaimrelationship
(
recorded_timestamp timestamp,
source_system_name string,
documentid string,
fkpartydocumentid string,
fkclaimdocumentid string,
sourceclaimidentifier string,
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_partyclaimrelationship';
MSCK REPAIR TABLE ${databasename}.ltcg_partyclaimrelationship SYNC PARTITIONS;
