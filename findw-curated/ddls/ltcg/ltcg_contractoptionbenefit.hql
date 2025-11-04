DROP TABLE IF EXISTS ${databasename}.ltcg_contractoptionbenefit;
CREATE EXTERNAL TABLE ${databasename}.ltcg_contractoptionbenefit
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,fkcontractoptiondocumentid string
,sourcesystemname string
,contractnumber string
,contractadministrationlocationcode string
,contractoptionbenefitordinalposition int
,contractoptionbenefittype string
,contractoptionbenefitdailybenefitamount decimal(18,6)
,contractoptionbenefitinitialunit string
,contractoptionbenefiteliminationvalue int
,contractoptionbenefiteliminationunit string
,contractoptionbenefitlocationofcarecode string
,contractoptionbenefitmaximumbenefitallowed decimal(18,6)
,contractoptionbenefitmaximumbenefitunit string
,contractoptionbenefitcoverageunitid bigint
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_contractoptionbenefit';
MSCK REPAIR TABLE ${databasename}.ltcg_contractoptionbenefit SYNC PARTITIONS;
