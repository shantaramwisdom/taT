DROP TABLE IF EXISTS ${databasename}.vantagep6_contractoption;
CREATE EXTERNAL TABLE ${databasename}.vantagep6_contractoption
(
 recorded_timestamp timestamp,
 source_system_name string,
 documentid string,
 ifxcontractdocumentid string,
 sourcesystemname string,
 contractnumber string,
 contractadministrationlocationcode string,
 contractoptionsourcesystemrelationposition integer,
 contractoptioneffectivedate timestamp,
 contractoptionbenefitstartdate timestamp,
 contractoptionbenefitstopdate timestamp,
 contractoptionstatuscode string,
 contractoptionmaximumanniversaryvalue decimal(18,6),
 contractoptionbenefittypeindicator string,
 contractoptionissueage integer,
 contractoptiondurationinmonth integer,
 contractoptionissuersstatuscode string,
 contractoptionissinglejointindicator string,
 contractoptiondeathindicator string,
 contractoptionincomeenhancementindicator string,
 contractoptionmodelingmessageid integer,
 contractoptionmodelingtypegroupindicator string,
 contractoptionissuestatecode string,
 contractoptionpercentagegrowthvalue decimal(18,6),
 contractoptionlifetimeasremainingwithdrawalamount decimal(18,6),
 contractoptionprincipalbaseremainingwithdrawalamount decimal(18,6),
 contractoptionreturnofpremiumvalue decimal(18,6),
 contractoptioncurrentnavvalue decimal(18,6),
 contractoptionguaranteedfuturevalue decimal(18,6),
 contractoptionhighmonthiversaryvalue decimal(18,6),
 contractoptionwithdrawalpercentage decimal(18,6),
 contractoptionlifetimefreepercentage decimal(18,6),
 contractoptioncurrentnavvalue decimal(18,6),
 contractoptiontaxableearning decimal(18,6),
 contractoptionfeespercentage decimal(18,6),
 contractoptiongrowthbasevalue decimal(18,6),
 contractoptionlongtermendid integer,
 contractoptionplancode string,
 contractoptiontypesource string,
 contractoptionbenefitaccessrepayment decimal(18,6),
 contractoptiondeathbenefitindicator string,
 contractoptionbenefitfactor decimal(18,6)
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
 '${s3bucketname}/${projectname}/curated/vantagep6/vantagep6_contractoption';
MSCK REPAIR TABLE ${databasename}.vantagep6_contractoption SYNC PARTITIONS;
