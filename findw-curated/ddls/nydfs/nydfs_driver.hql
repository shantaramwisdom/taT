DROP TABLE IF EXISTS ${databasename}.nydfs_driver;
CREATE EXTERNAL TABLE ${databasename}.nydfs_driver
(
recorded_timestamp timestamp,
contractnumber string,
contractadministrationlocationcode string,
other_key_name string,
other_key_value string,
total_records bigint,
total_impacted_records bigint,
load_date date,
cut_off_date date
)
PARTITIONED BY (
cycle_date date,
hop_name string,
source_system string,
table_name string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/nydfs/nydfs_driver';
MSCK REPAIR TABLE ${databasename}.nydfs_driver SYNC PARTITIONS;
