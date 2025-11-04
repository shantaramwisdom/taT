DROP TABLE IF EXISTS ${databasename}.vantagep5_contractoption;
CREATE EXTERNAL TABLE ${databasename}.vantagep5_contractoption
(
recorded_timestamp timestamp
,source_system_name string
,documentid string
,fkcontractdocumentid string
,sourcesystemname string
,contractnumber string
,contractadministrationlocationcode string
,contractoptionsourcesystemordinalposition INTEGER
,contractoptioneffectivedate TIMESTAMP
,contractoptionbenefitselectiondate TIMESTAMP
,contractoptionbenefitstopdate TIMESTAMP
,contractoptionstatuscode string
,contractoptionmaximumanniversaryvalue DECIMAL(18,6)
,contractoptionbenefitstopindicator string
,contractoptionissueage INTEGER
,contractoptionmodeljointmonths INTEGER
,contractoptionsinglejointindicator string
,contractoptiondeathindicator string
,contractoptionincomenhancementindicator string
,contractoptionmodelingwbsegment INTEGER
,contractoptiontypegroup string
,contractoptionplancode string
,contractoptiongenerationid INTEGER
,contractoptionissuestatuscode string
,contractoptionreturnofpremiumvalue DECIMAL(18,6)
,contractoptionstepvalue DECIMAL(18,6)
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
'${s3bucketname}/${projectname}/curated/vantagep5/vantagep5_contractoption';
MSCK REPAIR TABLE ${databasename}.vantagep5_contractoption SYNC PARTITIONS;
