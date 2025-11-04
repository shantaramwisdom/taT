DROP TABLE IF EXISTS ${databasename}.bestow_party;
CREATE EXTERNAL TABLE ${databasename}.bestow_party
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 sourcepartyid string,
 partyfirstname string,
 partylastname string,
 partymiddlename string,
 gender string,
 dateofbirth date,
 partyprimaryaddressline1 string,
 partyprimarycityname string,
 partyprimarypostalcode string,
 partylocationstatecode string,
 primarycountryname string
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
 '${s3bucketname}/${projectname}/curated/bestow/bestow_party';
MSCK REPAIR TABLE ${databasename}.bestow_party SYNC PARTITIONS;