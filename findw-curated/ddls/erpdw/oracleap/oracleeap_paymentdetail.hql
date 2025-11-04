DROP TABLE IF EXISTS ${databasename}.oracleeap_paymentdetail;
CREATE EXTERNAL TABLE ${databasename}.oracleeap_paymentdetail
(
 recorded_timestamp timestamp,
 source_system_name string,
 sourcesystemsupplieridentifier string,
 invoicenumber string,
 invoiceissuedate date,
 invoicesequencenumber integer,
 invoicelinenumber integer,
 businessunit string,
 sourcesystemsuppliername string,
 invoicepaymentstatus string,
 remittancemessage string,
 invoicepaymentdate date,
 invoicepaymentamount decimal(18,2),
 checknumber string,
 invoicepaymenttype string,
 priorinvoicepaymentdate date,
 priorchecknumber string,
 priorinvoicepaymenttype string,
 activityreporteddate timestamp,
 originatinggeneralledgerdescription1 string,
 activityamount decimal(18,2),
 originatinggeneralledgerdescription2 string,
 chartofaccountslineofbusinesscode string,
 chartofaccountsdepartmentcode string,
 chartofaccountsaccountcode string,
 chartofaccountsproductcode string,
 chartofaccountsintercompanycode string,
 chartofaccountssummaryproductcode string,
 chartofaccountscompanycode string,
 chartofaccountslineofbusinesscode string,
 chartofaccountsdepartmentcode string,
 chartofaccountsaccountcode string,
 chartofaccountsproductcode string,
 chartofaccountsintercompanycode string,
 chartofaccountssummaryproductcode string,
 legalcompanycode string,
 generalledgercode string,
 generalledgeraccountnumber string,
 generalledgeraccountsuffix string,
 centernumber string,
 invoicelinedistributionpackeddecimalamount string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oracleeap/oracleeap_paymentdetail';
MSCK REPAIR TABLE ${databasename}.oracleeap_paymentdetail SYNC PARTITIONS;
