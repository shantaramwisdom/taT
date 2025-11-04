DROP TABLE IF EXISTS ${databasename}.jewirebot_general_ledger_line_item;

CREATE EXTERNAL TABLE ${databasename}.jewirebot_general_ledger_line_item
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 activity_accounting_id string,
 transaction_number string,
 default_currency string,
 line_number int,
 debit_credit_indicator string,
 orig_gl_company string,
 orig_gl_account string,
 orig_gl_center string,
 contractnumber string,
 orig_gl_description_1 string,
 orig_gl_description_2 string,
 orig_gl_description_3 string,
 orig_gl_project_code string,
 orig_gl_source_document_id string,
 default_amount decimal(18,2)
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
 '${s3bucketname}/${projectname}/curated/erpdw/jewirebot/jewirebot_general_ledger_line_item';
MSCK REPAIR TABLE ${databasename}.jewirebot_general_ledger_line_item SYNC PARTITIONS;
