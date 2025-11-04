DROP TABLE IF EXISTS ${databasename}.erpdw_accounts_payable_vendor_master;
CREATE EXTERNAL TABLE ${databasename}.erpdw_accounts_payable_vendor_master
(
recorded_timestamp timestamp,
source_system_name string,
documentid string,
sourcesystemname string,
sourcesystemsupplieridentifier string,
supplierstieridentifier string,
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
sourcesystempayeebankaccountcode string,
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
batch_id bigint)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/curated/erpdw/erpdw/erpdw_accounts_payable_vendor_master';
MSCK REPAIR TABLE ${databasename}.erpdw_accounts_payable_vendor_master SYNC PARTITIONS;
