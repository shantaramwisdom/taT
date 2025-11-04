DROP TABLE IF EXISTS ${databasename}.geac_accounts_payable_vendor;

CREATE EXTERNAL TABLE ${databasename}.geac_accounts_payable_vendor
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 src_desc string,
 sourcesystemsupplieridentifier string,
 suppliersiteidentifier string,
 incometaxtypecode string,
 invoicepaymentmethodcode string,
 invoicepaymentterms string,
 sourcesystemsuppliername string,
 sourcesystemvendorgroup string,
 supplieraddressline1 string,
 supplieraddressline2 string,
 supplieraddressline3 string,
 supplieraddressline4 string,
 suppliercityname string,
 supplierstatecode string,
 supplierpostalcode string,
 supplierresidentcountrycode string,
 suppliertype string,
 taxidentificationnumber string,
 taxorganizationtype string,
 invoicepayaloneidentifier string,
 sourcesystemincometaxtypecode string,
 sourcesysteminvoicepaymentterms string,
 remittanceemailidentifier string,
 sourcesystempayeebankaccounttype string,
 sourcesysteminvoicepayaloneidentifier string,
 sourcesystempayorbankaccountcode string,
 businessunit string,
 invoicepaygroup string,
 payeeautomatedclearinghousestandardentryclasscode string,
 payeebankaccountnumber string,
 payeebankaccounttype string,
 payeebankroutingtransitnumber string,
 taxreportingname string
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
 '${s3bucketname}/${projectname}/curated/erpdw/geac/geac_accounts_payable_vendor';
MSCK REPAIR TABLE ${databasename}.geac_accounts_payable_vendor SYNC PARTITIONS;
