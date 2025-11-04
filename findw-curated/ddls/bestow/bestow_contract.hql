DROP TABLE IF EXISTS ${databasename}.bestow_contract;
CREATE EXTERNAL TABLE ${databasename}.bestow_contract
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 contractnumber string,
 contractadministrationlocationcode string,
 contracteffective date,
 contractlegalcompanycode string,
 contractplancode string,
 contractreportinglineofbusiness string,
 contractstatusreasoncode string,
 contractstatusidindicator string,
 contractqualifiedindicator string,
 contractsop51indicator string,
 contractgroupindicator string,
 contractcoverageid string,
 contractparticipationindicator string,
 contractsourcemarketingorganizationcode string,
 contractproducttypename string,
 originatingcontractnumber string,
 originatingsourcesystemname string,
 contractifrs17portfolio string,
 contractifrs17grouping string,
 contractifrs17cohort string,
 contractifrs17profitability string
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
 '${s3bucketname}/${projectname}/curated/bestow/bestow_contract';
MSCK REPAIR TABLE ${databasename}.bestow_contract SYNC PARTITIONS;