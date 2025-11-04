DROP TABLE IF EXISTS ${databasename}.oracleeap_accounts_payable_vendor;
CREATE EXTERNAL TABLE ${databasename}.oracleeap_accounts_payable_vendor
(
 recorded_timestamp timestamp,
 source_system_name string,
 original_cycle_date date,
 original_batch_id int,
 documentid string,
 sourcesystemsupplieridentifier string,
 supplierstatedentifier string,
 businessunit string,
 incometaxprecode string,
 invoicepaygroup string,
 invoicepaymentmethodcode string,
 invoicepaymentterms string,
 payeeautomatedclearinghousestandardentryclasscode string,
 payeebankaccountnumber string,
 payeebankaccounttype string,
 payeebankroutingtransitnumber string,
 sourcesystemsuppliername string,
 supplieraddressline1 string,
 supplieraddressline2 string,
 supplieraddressline3 string,
 supplieraddressline4 string,
 suppliercityname string,
 supplierpostalcode string,
 supplierresidentcountrycode string,
 supplierstatecode string,
 suppliertype string,
 taxidentificationnumber string,
 taxorganizationtype string,
 taxreportingname string,
 invoicepayaloneidentifier string,
 sourcesystemincometaxprecode string,
 sourcesysteminvoicepaymentterms string,
 remittancemessageidentifier string,
 sourcesystempayeebankaccounttype string,
 sourcesysteminvoicepayaloneidentifier string,
 sourcesystempayeebankaccountcode string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oracleeap/oracleeap_accounts_payable_vendor';
MSCK REPAIR TABLE ${databasename}.oracleeap_accounts_payable_vendor SYNC PARTITIONS;
