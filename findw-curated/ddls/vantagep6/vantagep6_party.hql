DROP TABLE IF EXISTS ${database}.vantage8_party;

CREATE EXTERNAL TABLE ${database}.vantage8_party
(
    recorded_timestamp string,
    source_system_name string,
    documentid string,
    caseextracttimestamp string,
    sourcecasecode string,
    contractprovidinglocationlincode string,
    partyfirstname string,
    partylastname string,
    partybirthdate DATE,
    secondaryindividualinccode string,
    gender string,
    partylocationlincode string
)
PARTITIONED BY (
    cycle_date DATE,
    batch_id string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    'dbfs:/mnt/curated/vantage8/vantage8_party';

MSCK REPAIR TABLE ${database}.vantage8_party SYNC PARTITIONS;