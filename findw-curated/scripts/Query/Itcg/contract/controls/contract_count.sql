select
{batchid} batch_id,
{cycle_date} cycle_date,
{domain_name} domain,
{source_system_name} sys_nm,
{source_system_name} p_sys_nm,
'BALANCING COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.policy_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from {source_database}.policy_effective_history_daily_{cycledate} POLICY
left anti join ltcg_expired_contracts expired on POLICY.policy_no = expired.policy_no
union all
select
{batchid} batch_id,
{cycle_date} cycle_date,
{domain_name} domain,
{source_system_name} sys_nm,
{source_system_name} p_sys_nm,
'BALANCING COUNT' measure_name,
'curated' hop_name,
'recordcount' measure_field,
'INTEGER' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.policy_effective_history_daily_{cycledate}' measure_table,
count(*) measure_value
from {source_database}.policy_effective_history_daily_{cycledate} POLICY
left anti join ltcg_expired_contracts expired on POLICY.policy_no = expired.policy_no
where
(cast(POLICY.view_row_create_day as date) <= '{cycle_date}' or
'{cycle_date}' > cast(POLICY.VIEW_ROW_OBSOLETE_DAY as date)) or
cast(POLICY.VIEW_ROW_EFFECTIVE_DAY as date) > '{cycle_date}' or
'{cycle_date}' >= cast(POLICY.VIEW_ROW_EXPIRATION_DAY as date) or
(coalesce(POLICY.POLICY_STATUS_CD,'') not in ('104','105') and
coalesce(POLICY.POLICY_STATUS_CD,'') != '106' or
cast(POLICY.ORIGINAL_EFFECTIVE_DT as date) < '2021-01-01')
