DROP TABLE IF EXISTS ${databasename}.sailpoint_oracleworkercountry;
CREATE EXTERNAL TABLE ${databasename}.sailpoint_oracleworkercountry
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 employeecountryname string,
 includeexclude string
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
 '${s3bucketname}/${projectname}/curated/erpdw/sailpoint/sailpoint_oracleworkercountry';
MSCK REPAIR TABLE ${databasename}.sailpoint_oracleworkercountry SYNC PARTITIONS;
