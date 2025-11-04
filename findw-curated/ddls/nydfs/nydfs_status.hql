DROP TABLE IF EXISTS ${databasename}.nydfs_status;
CREATE EXTERNAL TABLE ${databasename}.nydfs_status(
recorded_timestamp timestamp,
start_timestamp timestamp,
table_name string,
status string,
impact_count bigint,
source_count bigint,
expected_source_count bigint,
target_count bigint,
backup_count bigint,
cut_off_date date,
failure_reason string)
PARTITIONED BY (
cycle_date date,
hop_name string,
source_system string,
emr_name string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
'field.delim'='|',
'serialization.format'='|')
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/nydfs/nydfs_status';
MSCK REPAIR TABLE ${databasename}.nydfs_status SYNC PARTITIONS;
