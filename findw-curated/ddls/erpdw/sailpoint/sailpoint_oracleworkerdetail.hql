DROP TABLE IF EXISTS ${databasename}.sailpoint_oracleworkerdetail;
CREATE EXTERNAL TABLE ${databasename}.sailpoint_oracleworkerdetail
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 sailpointemployeeid string,
 employeelastname string,
 employeefirstname string,
 employeemiddlename string,
 employeeemailaddress string,
 employeecountryname string,
 employeetype string,
 employeesupervisorisil string,
 supervisorsailpointemployeeid string
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
 '${s3bucketname}/${projectname}/curated/erpdw/sailpoint/sailpoint_oracleworkerdetail';
MSCK REPAIR TABLE ${databasename}.sailpoint_oracleworkerdetail SYNC PARTITIONS;
