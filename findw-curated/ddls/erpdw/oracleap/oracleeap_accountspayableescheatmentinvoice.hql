DROP TABLE IF EXISTS ${databasename}.oracleeap_accountspayableescheatmentinvoice;
CREATE EXTERNAL TABLE ${databasename}.oracleeap_accountspayableescheatmentinvoice
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 accountspayableescheatmentinvoiceid string,
 checkididentifier string,
 externaltransactionidentifier string,
 invoiceidentifier string,
 accountspayableescheatmentcheckid string,
 adminsystembupupflag string,
 businessunit string,
 chartofaccountsaccountcode string,
 chartofaccountscompanycode string,
 chartofaccountsdepartmentcode string,
 chartofaccountsintercompanycode string,
 chartofaccountsblineofbusinesscode string,
 chartofaccountsproductcode string,
 chartofaccountssummaryproductcode string,
 escheatmentpropertytype string,
 generalledgerapplicationareacode string,
 generalledgerledgertype string,
 generalledgerledgername string,
 invoiceamount decimal(18,2),
 invoiceissuedate date,
 invoicenumber string,
 invoicetype string,
 originatinggeneralledgerdescription1 string,
 originatinggeneralledgerdescription2 string,
 originatinggeneralledgerdescription3 string,
 originatinggeneralledgerprojectcode string,
 replacementcheckidentifier string,
 sourcesystemname string,
 sourcesystempaygroup string,
 sourcesystemsupplieridentifier string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oracleeap/oracleeap_accountspayableescheatmentinvoice';
MSCK REPAIR TABLE ${databasename}.oracleeap_accountspayableescheatmentinvoice SYNC PARTITIONS;
