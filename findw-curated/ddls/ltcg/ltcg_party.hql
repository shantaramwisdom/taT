DROP TABLE IF EXISTS ${databasename}.ltcg_party;
CREATE EXTERNAL TABLE ${databasename}.ltcg_party
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,sourcepartyid string
,sourcesystemname string
,contractadministrationlocationcode string
,partyfirstname string
,partylastname string
,partymiddlename string
,partyprimaryaddressline1 string
,partyprimaryaddressline2 string
,partyprimaryaddressline3 string
,partyprimarycityname string
,partylocationstatecode string
,partyprimarypostalcode string
,primarycountryname string
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_party';
MSCK REPAIR TABLE ${databasename}.ltcg_party SYNC PARTITIONS;
