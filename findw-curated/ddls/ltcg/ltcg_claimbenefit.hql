DROP TABLE IF EXISTS ${databasename}.ltcg_claimbenefit;
CREATE EXTERNAL TABLE ${databasename}.ltcg_claimbenefit
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkclaimdocumentid string
,sourceclaimidentifier string
,sourcesteamname string
,contractnumber string
,contractadministrationlocationcode string
,claimbenefitlineindicator int
,claimbenefitamount decimal(18,6)
,claimbenefitinitalinvoicecreateddate date
,claimbenefitinitalpaymentdate date
,claimbenefitpartnershipexchangeindicator string
,claimbenefitpaymentreasoncode string
,claimbenefitpaymentreasondescription string
,claimbenefitpaymentsupplementaryreasoncode string
,claimbenefitpaymentsupplementaryreasondescription string
,claimbenefitprocesseddate date
,claimbenefitrequestedamount decimal(18,6)
,claimbenefitserviceenddate date
,claimbenefitservicestartdate date
,claimbenefitservicounts decimal(18,6)
,claimbenefitstatuscode string
,claimbenefitstatusreasoncode string
,claimbenefittypecode string
,claimbenefitlocationreference string
,claimbenefitstatusdescription string
,claimbenefitstatusreasondescription string
,claimbenefitrequestforbenefitidentifier string
,claimbenefitpaymentrequestlineidentifier string
)
PARTITIONED BY (
cycle_date date,
batch_id int)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_claimbenefit';
MSCK REPAIR TABLE ${databasename}.ltcg_claimbenefit SYNC PARTITIONS;
