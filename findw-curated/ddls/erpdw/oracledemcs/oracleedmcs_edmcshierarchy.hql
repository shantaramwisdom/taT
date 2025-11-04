DROP TABLE IF EXISTS ${databasename}.oracleedmcs_edmcshierarchy;
CREATE EXTERNAL TABLE ${databasename}.oracleedmcs_edmcshierarchy
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 hierarchyname string,
 name string,
 description string,
 parent string,
 enabled string,
 summary string,
 allowposting string,
 allowbudgeting string,
 accounttype string,
 thirdpartyaccountcontrol string,
 financialcategory string,
 reconcile string,
 level0 string,
 level1 string,
 level2 string,
 level3 string,
 level4 string,
 level5 string,
 level6 string,
 level7 string,
 level8 string,
 level9 string,
 level10 string,
 level11 string,
 level12 string,
 level13 string,
 level14 string,
 level15 string,
 level16 string,
 level17 string,
 level18 string,
 level19 string,
 level20 string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oracleedmcs/oracleedmcs_edmcshierarchy';
MSCK REPAIR TABLE ${databasename}.oracleedmcs_edmcshierarchy SYNC PARTITIONS;
