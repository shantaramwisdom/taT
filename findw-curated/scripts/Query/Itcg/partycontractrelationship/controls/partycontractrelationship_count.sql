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
'{source_database}.life_effective_history_daily_{cycledate}/policy_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from
{source_database}.LIFE_effective_history_daily_{cycledate} LIFE
join {source_database}.Policy_effective_history_daily_{cycledate} POLICY on
LIFE.Life_ID = POLICY.Insured_Life_ID
left anti join ltcg_expired_contracts expired on POLICY.policy_no = expired.policy_no
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
'{source_database}.life_effective_history_daily_{cycledate}/policy_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from
{source_database}.LIFE_effective_history_daily_{cycledate} LIFE
join {source_database}.Policy_effective_history_daily_{cycledate} POLICY on
LIFE.Life_ID = POLICY.Insured_Life_ID
left anti join ltcg_expired_contracts expired on POLICY.policy_no = expired.policy_no
where
cast(LIFE.View_Row_Create_Day as date) <= '{cycle_date}'
or ('{cycle_date}' <= cast(LIFE.View_Row_Obsolete_Day as date)
and cast(LIFE.View_Row_Effective_Day as date) <= '{cycle_date}'
or '{cycle_date}' < cast(LIFE.View_Row_Expiration_Day as date))
and ('{cycle_date}' <= cast(POLICY.View_Row_Create_Day as date)
or cast(POLICY.View_Row_Obsolete_Day as date) >= '{cycle_date}'
or cast(POLICY.View_Row_Effective_Day as date) <= '{cycle_date}'
or '{cycle_date}' <= cast(POLICY.View_Row_Expiration_Day as date))
and (coalesce(POLICY.Policy_Status_Cd, '') not in ('104','105')
and (coalesce(POLICY.Policy_Status_Cd, '') != '106'
or cast(POLICY.Original_Effective_Dt as date) < '2021-01-01'))
