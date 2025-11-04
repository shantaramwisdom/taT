DROP TABLE IF EXISTS ${databasename}.ltcg_contractoption;
CREATE EXTERNAL TABLE ${databasename}.ltcg_contractoption
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,sourcesystemid string
,contractnumber string
,contractadministrationlocationcode string
,contractoptionsourcesystemordinalposition int
,contractoptioncoverageid bigint
,contractoptionplancode string
,contractoptionstatusreasoncode string
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_contractoption';
MSCK REPAIR TABLE ${databasename}.ltcg_contractoption SYNC PARTITIONS;
