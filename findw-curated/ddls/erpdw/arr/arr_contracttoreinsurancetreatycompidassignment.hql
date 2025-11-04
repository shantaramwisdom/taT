DROP TABLE IF EXISTS ${databasename}.arr_contracttoreinsurancetreatycompidassignment;
CREATE EXTERNAL TABLE ${databasename}.arr_contracttoreinsurancetreatycompidassignment
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
'${s3bucketname}/${projectname}/curated/erpdw/arr/arr_contracttoreinsurancetreatycompidassignment';
MSCK REPAIR TABLE ${databasename}.arr_contracttoreinsurancetreatycompidassignment SYNC PARTITIONS;
