DROP TABLE IF EXISTS ${databasename}.ltcg_claim;
CREATE EXTERNAL TABLE ${databasename}.ltcg_claim
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,sourcesalldentifier string
,sourcesteamname string
,contractnumber string
,contractadministrationlocationcode string
,claimmatindigidiosiscode string
,claimclaimantcognitiveimpairmentindicator string
,claimclaimantfulldependencycount int
,claimdisabilityenddate date
,claimprimarydiagnosiscode string
,claimprimarydiagnosiscodereassessment string
,claimreasoncode string
,claimrequestforbenefitstatuscode string
,claimstatuscode string
,claimstatusdescription string
,claimreasondescription string
,claimrequestforbenefitstatusdescription string
,claimrequestforbenefitidentifier string
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_claim';
MSCK REPAIR TABLE ${databasename}.ltcg_claim SYNC PARTITIONS;
