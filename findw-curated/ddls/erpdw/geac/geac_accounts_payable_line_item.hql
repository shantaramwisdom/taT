DROP TABLE IF EXISTS ${databasename}.geac_accounts_payable_line_item;

CREATE EXTERNAL TABLE ${databasename}.geac_accounts_payable_line_item
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 activity_accounting_id string,
 invoiceidentifier string,
 src_desc string,
 invoicelinenumber int,
 activityamount decimal(18,2),
 originatinggeneralledgerdescription1 string,
 originatinggeneralledgerdescription2 string,
 originatinggeneralledgerdescription3 string,
 originatinggeneralledgerprojectcode string,
 originatinggeneralledgeraccountnumber string,
 originatinggeneralledgercenternumber string,
 originatinggeneralledgercompanycode string,
 originatinggeneralledgerexpenseindicator string,
 legalcompanyname string,
 bapcode string,
 partnumber string,
 exp_ind_il string,
 pay_entity_il string,
 expense_ind_ih string,
 pay_entity_ih string,
 expense_ind_cg string,
 entity_number_cg string,
 exp_account_il string,
 exp_account_ih string,
 exp_cntr_il string,
 exp_center_ih string,
 exp_company_il string,
 exp_company_ih string
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
 '${s3bucketname}/${projectname}/curated/erpdw/geac/geac_accounts_payable_line_item';
MSCK REPAIR TABLE ${databasename}.geac_accounts_payable_line_item SYNC PARTITIONS;
