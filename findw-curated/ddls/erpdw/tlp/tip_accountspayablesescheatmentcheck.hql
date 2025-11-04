DROP TABLE IF EXISTS ${databasename}.tip_accountspayablesescheatmentcheck;
CREATE EXTERNAL TABLE ${databasename}.tip_accountspayablesescheatmentcheck
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 sourcesystemname string,
 accountspayablesescheatmentcheckid string,
 checkidentifier string,
 externaltransactionidentifier string,
 checkamount decimal(18,2),
 checknumber string,
 escheatmentcheckstatus string,
 extendedsupplierresidentcountrycode string,
 invoicepaygroup string,
 invoicepaymentdate date,
 remittancemessage string,
 sourcesystemsupplieridentifier string,
 sourcesystemsuppliername string,
 supplieraddressline1 string,
 supplieraddressline2 string,
 supplieraddressline3 string,
 suppliercityname string,
 supplierpostalcode string,
 supplierstatecode string,
 taxidentificationnumber string
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
 '${s3bucketname}/${projectname}/curated/erpdw/tip/tip_accountspayablesescheatmentcheck';
MSCK REPAIR TABLE ${databasename}.tip_accountspayablesescheatmentcheck SYNC PARTITIONS;
