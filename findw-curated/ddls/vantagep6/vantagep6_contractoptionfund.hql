DROP TABLE IF EXISTS ${databasename}.vantagep6_contractoptionfund;
CREATE EXTERNAL TABLE ${databasename}.vantagep6_contractoptionfund
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 ifxcontractoptiondocumentid string,
 ifxfunddocumentid string,
 sourcesystemname string,
 contractnumber string,
 contractadministrationlocationcode string,
 contractoptionsourcesystemrelationposition integer,
 contractoptionfundnumber string,
 contractoptionfundclassindicator string,
 contractoptionfundbundledrevenueshareamount decimal(18,8),
 contractoptionfundbundledinvestmentmanagementfeesamount decimal(18,8),
 contractoptionfundbundledrevenueshareamount decimal(18,8),
 contractoptionfundbundledinvestmentmanagementfeesamount decimal(18,8),
 contractoptionfundbundledfacilitationfeesamount decimal(18,8),
 contractoptionfundbundledgedainlossamount decimal(18,8),
 contractoptionfundsaferevenueshareamount decimal(18,8),
 contractoptionfundsafeinvestmentmanagementfeesamount decimal(18,8),
 contractoptionfundvalue decimal(18,6),
 contractoptionfunddefaultfundindicator string,
 contractoptionfundseparateaccountvalueindex1 decimal(18,8),
 contractoptionfundseparateaccountvalueindex2 decimal(18,8),
 contractoptionfundseparateaccountvalueindex3 decimal(18,8),
 contractoptionfundseparateaccountvalueindex4 decimal(18,8),
 contractoptionfundseparateaccountvalueindex5 decimal(18,8),
 contractoptionfundseparateaccountvalueindex6 decimal(18,8),
 contractoptionfundseparateaccountvalueindex7 decimal(18,8),
 contractoptionfundseparateaccountvalueindex8 decimal(18,8),
 contractoptionfundseparateaccountvalueindex9 decimal(18,8),
 contractoptionfundseparateaccountvalueindex10 decimal(18,8),
 contractoptionfundseparatefundvalue decimal(18,8),
 contractoptionfunddynamicseparateaccountvalue decimal(18,8),
 contractoptionfundsafehedgingseparateaccountvalue decimal(18,8)
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
 '${s3bucketname}/${projectname}/curated/vantagep6/vantagep6_contractoptionfund';
MSCK REPAIR TABLE ${databasename}.vantagep6_contractoptionfund SYNC PARTITIONS;
