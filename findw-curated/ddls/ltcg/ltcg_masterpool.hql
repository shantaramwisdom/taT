DROP TABLE IF EXISTS ${databasename}.ltcg_masterpool;
CREATE EXTERNAL TABLE ${databasename}.ltcg_masterpool
(
recorded_timestamp timestamp,
source_system_name string,
view_row_create_day date,
view_row_obsolete_day date,
view_row_effective_day date,
view_row_expiration_day date,
policy_id int,
coverage_id int,
policy_pool_id int,
coverage_unit_id int,
ltcg_pool_type string,
ltcgpoolcount int,
max_greaterof_indic string,
nh_lifetimemax decimal(18,2),
nh_litunitmeasure string,
nh_usedmaxdollars decimal(18,2),
nh_usedmaxdays int,
nh_indemnity string,
al_lifetimemax decimal(18,2),
al_litunitmeasure string,
al_usedmaxdollars decimal(18,2),
al_usedmaxdays int,
al_indemnity string,
hc_lifetimemax1 decimal(18,2),
hc_litunitmeasure1 string,
hc_lifetimemax2 decimal(18,2),
hc_litunitmeasure2 string,
hc_usedmaxdollars decimal(18,2),
hc_usedmaxdays int,
hc_indemnity string,
nh_elim int,
nh_elimunitmeasure string,
al_elim int,
al_elimunitmeasure string,
hc_elim int,
hc_elimunitmeasure string,
elim_dollarsused decimal(18,2),
nhcurrdb decimal(18,2),
alcurrdb decimal(18,2),
hccurrdb decimal(18,2),
procurrdb decimal(18,2),
vpu double,
pooltype string,
otherpool string,
issuestate string,
policy_no string,
parent_unit_id int,
shortdescription string,
nh_lifemaxdays decimal(18,2),
monthlyfac string,
monthlyhc string
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
'${s3bucketname}/${projectname}/curated/ltcg/ltcg_masterpool';
MSCK REPAIR TABLE ${databasename}.ltcg_masterpool SYNC PARTITIONS;
