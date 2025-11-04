DROP TABLE IF EXISTS ${databasename}.oracleeap_subledgerjournallinedetail;
CREATE EXTERNAL TABLE ${databasename}.oracleeap_subledgerjournallinedetail
(
 recorded_timestamp timestamp,
 source_system_name string,
 generalledgersubledgerjournalheaderid string,
 generalledgersubledgerjournallinenumber BIGINT,
 generalledgerledgername string,
 generalledgeraccountingperiod string,
 activityreporteddate timestamp,
 generalledgersubledgerpostdate timestamp,
 generalledgersubledgername string,
 generalledgersubledgereventtypecode string,
 chartofaccountscompanycode string,
 chartofaccountslineofbusinesscode string,
 chartofaccountsdepartmentcode string,
 chartofaccountsaccountcode string,
 chartofaccountsproductcode string,
 chartofaccountsintercompanycode string,
 chartofaccountssummaryproductcode string,
 generalledgertransactioncurrencycode string,
 generalledgertransactioncurrencyamount decimal(18,6),
 generalledgercurrencyconversionratedate date,
 generalledgercurrencyconversionratetype string,
 generalledgerreportingcurrencyexchangerate decimal(18,6),
 generalledgerreportingcurrencycode string,
 generalledgerreportingcurrencyamount decimal(18,6),
 generalledgersubledgerjournallinedescription string,
 generalledgerjournalname string,
 generalledgerjournalbatchname string,
 generalledgerjournallinenumber integer,
 generalledgersubledgerjournalcreateddate timestamp,
 generalledgersubledgerjournalcreator string,
 generalledgersubledgerjournalupdateddate timestamp,
 generalledgersubledgerjournallastupdatedby string,
 sourcesystemname string,
 originatinggeneralledgerdescription1 string,
 originatinggeneralledgerdescription2 string,
 originatinggeneralledgerdescription3 string,
 originatinggeneralledgerprojectcode string,
 activitygeneralledgerapplicationarea string,
 activitygeneralledgersourcecode string,
 generalledgerdatatype string,
 activityaccountingid string,
 legalcompanycode string,
 glscaledgercode string,
 generalledgeraccountnumber string,
 generalledgeraccountsuffix string,
 centernumber string,
 businessunit string,
 invoicenumber string,
 checknumber string,
 invoicepaymentmethodcode string,
 invoicepaygroup string,
 invoiceissuedate date,
 invoicepaymentdate date,
 supplieridentifier string,
 supplierstatedentifier string,
 suppliername string,
 escheatmentpropertytype string,
 sourcesystemsuppliername string,
 remittancemessage string,
 originatinggeneralledgerexpenseindicator string,
 sourcesystemsupplieridentifier string,
 sourcesystemvendorgroup string,
 sourcesystempaygroup string,
 legalcompanyname string,
 glscaledgerdescription string,
 generalledgeraccountdescription string,
 generalledgerlineofbusinessdescription string,
 centerdescription string,
 sourcesystembatchdate timestamp,
 sourcesystembatchidentifier string,
 invoiceidentifier string
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
 '${s3bucketname}/${projectname}/curated/erpdw/oracleeap/oracleeap_subledgerjournallinedetail';
MSCK REPAIR TABLE ${databasename}.oracleeap_subledgerjournallinedetail SYNC PARTITIONS;
