DROP TABLE IF EXISTS ${databasename}.expiredcontract;
CREATE EXTERNAL TABLE ${databasename}.expiredcontract
(
recorded_timestamp timestamp,
policy_no string,
policy_id string,
batch_id int
)
PARTITIONED BY (
source_system string,
domain_name string,
cycle_date date)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/orchestration/expiredcontract'
TBLPROPERTIES (
'parquet.compression'='SNAPPY');
MSCK REPAIR TABLE ${databasename}.expiredcontract SYNC PARTITIONS;
