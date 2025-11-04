DROP TABLE IF EXISTS ${databasename}.ahd_contracttoreinsurancetreatycompidassignment;
CREATE EXTERNAL TABLE ${databasename}.ahd_contracttoreinsurancetreatycompidassignment
(
recorded_timestamp timestamp,
source_system_name string,
documentid string,
sourcesystemname string,
contractsourcesystemname string,
contractnumber string,
reinsurancetreatycomponentidentifier string,
reinsurancetreatynumber string
)
PARTITIONED BY (
cycle_date date,
batch_id int,
cdc_action string)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/curated/erpdw/ahd/ahd_contracttoreinsurancetreatycompidassignment';
MSCK REPAIR TABLE ${databasename}.ahd_contracttoreinsurancetreatycompidassignment SYNC PARTITIONS;
