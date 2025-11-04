DROP TABLE IF EXISTS ${databasename}.alm_fund;
CREATE EXTERNAL TABLE ${databasename}.alm_fund
(
 recorded_timestamp timestamp,
 source_system_name varchar(50),
 documentid string,
 sourcesystemname string,
 fundnumber string,
 fundregion string,
 productfundticker string,
 fundname string,
 fundloadablefile string,
 fundindicator string,
 fundstudydate date,
 fundinforcedate date,
 fundcompany string,
 fundcusip string,
 fundcusipticker string,
 fundtypeid int,
 fundcategory string,
 fundclass string,
 funddynamicseparateaccountflag int,
 fundselfhedgingseparateaccountflag int,
 fundgainlossfee decimal(18,6),
 fundrevenuesharingfee decimal(18,6),
 fundfacilitationfee decimal(18,6),
 fundinvestmentmanagementfee decimal(18,6),
 fundmerrillynchrevenuesharingfee decimal(18,6),
 fundmerrillynchfundfacilitationfee decimal(18,6),
 fundisharefundfacilitationfee decimal(18,6),
 fundseparateaccountweightindex1 decimal(18,6),
 fundseparateaccountweightindex2 decimal(18,6),
 fundseparateaccountweightindex3 decimal(18,6),
 fundseparateaccountweightindex4 decimal(18,6),
 fundseparateaccountweightindex5 decimal(18,6),
 fundseparateaccountweightindex6 decimal(18,6),
 fundseparateaccountweightindex7 decimal(18,6),
 fundseparateaccountweightindex8 decimal(18,6),
 fundseparateaccountweightindex9 decimal(18,6),
 fundseparateaccountweightindex10 decimal(18,6)
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
 '${s3bucketname}/${projectname}/curated/alm/alm_fund';
MSCK REPAIR TABLE ${databasename}.alm_fund SYNC PARTITIONS;