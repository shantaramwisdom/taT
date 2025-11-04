select
'{batchid}' batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'BALANCING_COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.life_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from
(
select
distinct life.life_id
from
{source_database}.life_effective_history_daily_{cycledate} life
left join {source_database}.Policy_effective_history_daily_{cycledate} policy on
life.life_id = policy.insured_life_id left anti
join ltcg_expired_contracts on
ltcg_expired_contracts.policy_no = policy.policy_no
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
'INTEGER' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.life_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from {source_database}.life_effective_history_daily_{cycledate}
where
cast(view_row_create_day as date) <= '{cycle_date}'
or ('{cycle_date}' > cast(view_row_obsolete_day as date)
or cast(view_row_effective_day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(view_row_expiration_day as date))
)
