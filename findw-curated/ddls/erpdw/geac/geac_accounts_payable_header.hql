DROP TABLE IF EXISTS ${databasename}.geac_accounts_payable_header;

CREATE EXTERNAL TABLE ${databasename}.geac_accounts_payable_header
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 invoiceidentifier string,
 src_desc string,
 activityreporteddate date,
 generalledgerapplicationareacode string,
 sourcesystembatchdate date,
 sourcesystembatchidentifier string,
 sourcesystempaygroup string,
 invoiceissuedate date,
 invoicenumber string,
 sourcesystemsupplieridentifier string,
 checknumber string,
 escheatmentpropertytype string,
 invoicepaygroup string,
 invoiceamount decimal(18,2),
 invoicepaymentdate date,
 invoicepaymentspecialhandlingcode string,
 invoicetype string,
 remittancemessage string,
 sourcesystempayorbankaccountcode string,
 bankaccountnumber string,
 businessunit string,
 generalledgersourcecode string,
 invoicestatus string,
 legalcompanyname string,
 sourcesystemname string,
 sourcesystemvendorgroup string,
 supplieridentifier string,
 suppliersiteidentifier string,
 suppliername string,
 invoicepaymentmethodcode string,
 src_nmbr string,
 seq_nmbr string,
 record_type string
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
 '${s3bucketname}/${projectname}/curated/erpdw/geac/geac_accounts_payable_header';
MSCK REPAIR TABLE ${databasename}.geac_accounts_payable_header SYNC PARTITIONS;
