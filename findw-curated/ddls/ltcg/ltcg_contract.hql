DROP TABLE IF EXISTS ${databasename}.ltcg_contract;
CREATE EXTERNAL TABLE ${databasename}.ltcg_contract
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,sourcesystemid string
,contractnumber string
,contractadministrationlocationcode string
,contractparticipationindicator string
,contractps01indicator string
,contracteffectivefromdate date
,contractexpirydate date
,contractfirst71grouping string
,contractfirst71portfoliostring
,contractfirst71profitability string
,contractissuedate date
,contractissuemarketorganizationcode string
,contractproducttypename string
,contractqualifyingbusiness string
,contractreportinglineofbusiness string
,contractstatuscode string
,contractstatusreasoncode string
,originatingcontractnumber string
,contractplancode string
,sourcelegalentitylocode string
,originatingsourcesystemname string
,contractlegalcompanycode string
,contractbillingmode string
,contractcoverageid string
,contractpartnershipexchangestatuscode string
,contractpackage int
,contractannualizedpremium decimal(18,6)
,contractissuedatecode string
,contractmodalpremium decimal(18,6)
,contractpremiummodecode int
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_contract';
MSCK REPAIR TABLE ${databasename}.ltcg_contract SYNC PARTITIONS;
