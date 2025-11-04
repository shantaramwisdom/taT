with ltcg_expired_contracts as (
select
distinct POLICY.policy_id
from
{source_database}.Policy_effective_history_daily_{cycledate} POLICY left anti
join ltcg_expired_contracts expired on
policy.policy_no = expired.policy_no
)
select
{batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'CONTROL_TOTAL' measure_name,
'curated' hop_name,
'sd_amount' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.service_detail_effective_history_daily_{cycledate}' measure_table,
sum(cast(sd_amount as decimal(18,6))) measure_value
from
{source_database}.service_detail_effective_history_daily_{cycledate} sehd
inner join ltcg_expired_contracts POLICY on sehd.policy_id = POLICY.policy_id
where
cast(view_row_create_day as date) = '{cycle_date}'
union all
select
{batchid} batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'CONTROL_TOTAL' measure_name,
'curated' hop_name,
'sd_amount' measure_field,
'INTEGER' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.service_detail_effective_history_daily_{cycledate}' measure_table,
sum(cast(sd_amount as decimal(18,6))) measure_value
from
{source_database}.service_detail_effective_history_daily_{cycledate} sehd
inner join ltcg_expired_contracts POLICY on sehd.policy_id = POLICY.policy_id
where
cast(view_row_create_day as date) = '{cycle_date}'
and SD_REPORTED_FLG is null
union all
select
sd_amount
from
{source_database}.service_detail_effective_history_daily_{cycledate} sehd
inner join ltcg_expired_contracts POLICY on sehd.policy_id = POLICY.policy_id
where
cast(view_row_create_day as date) = '{cycle_date}'
and SD_REPORTED_FLG is not null
and SD_REPORTED_FLG != '1'
union all
select
sd_amount
from
{source_database}.service_detail_effective_history_daily_{cycledate} sehd
inner join ltcg_expired_contracts POLICY on sehd.policy_id = POLICY.policy_id
left join batch_tracking bt on
bt.source_system_name = 'LTCG'
where
cast(view_row_create_day as date) = '{cycle_date}'
and SD_REPORTED_FLG = '1'
and (SD_OK_TO_PAY_FLG = '2'
and SD_EARLIEST_REPORT_DT <= bt.min_cycle_date)
union all
select
sd_amount
from
{source_database}.service_detail_effective_history_daily_{cycledate} sehd
inner join ltcg_expired_contracts POLICY on sehd.policy_id = POLICY.policy_id
left join batch_tracking bt on
bt.source_system_name = 'LTCG'
where
cast(view_row_create_day as date) = '{cycle_date}'
and SD_REPORTED_FLG = '1'
and (SD_OK_TO_PAY_FLG = '1'
and Benefit_Payment_ID is not null
and Benefit_Adjustment_ID is null)
and SD_EARLIEST_REPORT_DT <= bt.min_cycle_date
or ( SD_OK_TO_PAY_FLG = '1'
and Benefit_Payment_ID is null
and Benefit_Adjustment_ID is not null)
