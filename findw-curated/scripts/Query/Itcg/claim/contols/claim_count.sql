-- claim_count.sql
select
    '{batchid}' batch_id,
    '{cycle_date}' cycle_date,
    '{domain_name}' domain,
    '{source_system_name}' sys_nm,
    '{source_system_name}' P_sys_nm,
    'BALANCING_COUNT' measure_name,
    'curated' hop_name,
    'recordcount' measure_field,
    'integer' measure_value_datatype,
    'S' measure_src_tgt_adj_indicator,
    '{source_database}.claim_eligibility_effective_history_daily_{cycledate}' measure_table,
    count(*) measure_value
from
    {source_database}.claim_eligibility_effective_history_daily_{cycledate} claim_eligibility
join
    {source_database}.policy_effective_history_daily_{cycledate} policy on
    claim_eligibility.policy_id = policy.policy_id
left anti join ltcg_expired_contracts expired on
    policy.policy_no = expired.policy_no
union all
select
    '{batchid}' batch_id,
    '{cycle_date}' cycle_date,
    '{domain_name}' domain,
    '{source_system_name}' sys_nm,
    '{source_system_name}' P_sys_nm,
    'BALANCING_COUNT' measure_name,
    'curated' hop_name,
    'recordcount' measure_field,
    'integer' measure_value_datatype,
    'A' measure_src_tgt_adj_indicator,
    '{source_database}.claim_eligibility_effective_history_daily_{cycledate}' measure_table,
    count(*) measure_value
from
    {source_database}.claim_eligibility_effective_history_daily_{cycledate} claim_eligibility
join
    {source_database}.policy_effective_history_daily_{cycledate} policy on
    claim_eligibility.policy_id = policy.policy_id
left anti join ltcg_expired_contracts expired on
    policy.policy_no = expired.policy_no
where
    cast(claim_eligibility.view_row_create_day as date) > '{cycle_date}'
    or '{cycle_date}' > cast(claim_eligibility.view_row_obsolete_day as date)
    or cast(claim_eligibility.view_row_effective_day as date) > '{cycle_date}'
    or '{cycle_date}' > cast(claim_eligibility.view_row_expiration_day as date)
    or cast(policy.view_row_create_day as date) > '{cycle_date}'
    or '{cycle_date}' > cast(policy.view_row_obsolete_day as date)
    or cast(policy.view_row_effective_day as date) > '{cycle_date}'
    or '{cycle_date}' > cast(policy.view_row_expiration_day as date)
    or policy.policy_status_cd not in ('104','105')
    and (policy.policy_status_cd != '106'
    or policy.original_effective_dt < '2021-01-01')
