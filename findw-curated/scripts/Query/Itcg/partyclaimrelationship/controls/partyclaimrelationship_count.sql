select
'{batchid}' batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'integer' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.claim_eligibility_effective_history_daily_{cycledate}' measure_table ,
count(*) measure_value
from
(
select
ceehc.policy_id,
ceehc.rfb_id
from
{source_database}.claim_eligibility_effective_history_daily_{cycledate} ceehc
left join {source_database}.policy_effective_history_daily_{cycledate} pehc on
ceehc.policy_id = pehc.policy_id
left anti join ltcg_expired_contracts expired on pehc.policy_no = expired.policy_no
where
cast(ceehc.view_row_create_day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(ceehc.view_row_obsolete_day as date)
and cast(ceehc.view_row_effective_day as date) <= '{cycle_date}'
and '{cycle_date}' < cast(ceehc.view_row_expiration_day as date)
group by
ceehc.policy_id,
ceehc.rfb_id
)
union all
select
'{batchid}' batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'integer' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.claim_eligibility_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from
(
select
ceehc.policy_id,
ceehc.rfb_id
from
{source_database}.claim_eligibility_effective_history_daily_{cycledate} ceehc
where
cast(ceehc.view_row_create_day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(ceehc.view_row_obsolete_day as date)
and cast(ceehc.view_row_effective_day as date) <= '{cycle_date}'
and '{cycle_date}' < cast(ceehc.view_row_expiration_day as date)
group by
ceehc.policy_id,
ceehc.rfb_id
) claim_eligibility
join
(
select
pehc.policy_id,
insured_life_id,
pehc.policy_no
from
{source_database}.policy_effective_history_daily_{cycledate} pehc
where
cast(pehc.view_row_create_day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(pehc.view_row_obsolete_day as date)
and cast(pehc.view_row_effective_day as date) <= '{cycle_date}'
and '{cycle_date}' < cast(pehc.view_row_expiration_day as date)
and (pehc.policy_status_cd not in ('104','105')
and (pehc.policy_status_cd != '106'
or pehc.original_effective_dt < '2021-01-01')) ) policy on
claim_eligibility.policy_id = policy.policy_id
join
(
select
pehc.life_id
from
{source_database}.life_effective_history_daily_{cycledate} pehc
where
cast(pehc.view_row_create_day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(pehc.view_row_obsolete_day as date)
and cast(pehc.view_row_effective_day as date) <= '{cycle_date}'
and '{cycle_date}' < cast(pehc.view_row_expiration_day as date)
) life on
policy.insured_life_id = life.life_id
left anti join ltcg_expired_contracts expired on policy.policy_no = expired.policy_no
