DROP TABLE IF EXISTS ${databasename}.mkdb_currency_rates;
CREATE EXTERNAL TABLE ${databasename}.mkdb_currency_rates
(
 recorded_timestamp timestamp,
 source_system_name string,
 sourcesystemname string,
 documentid string,
 generalledgertransactioncurrencycode string,
 generalledgerreportingcurrencycode string,
 generalledgerreportingcurrencyconversionratetype string,
 generalledgerreportingcurrencyexchangeeffectiveperiodstartdate date,
 generalledgerreportingcurrencyexchangeeffectiveperiodstopdate date,
 generalledgerreportingcurrencyexchangerate decimal(18,6)
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
 '${s3bucketname}/${projectname}/curated/erpdw/mkdb/mkdb_currency_rates';
MSCK REPAIR TABLE ${databasename}.mkdb_currency_rates SYNC PARTITIONS;
