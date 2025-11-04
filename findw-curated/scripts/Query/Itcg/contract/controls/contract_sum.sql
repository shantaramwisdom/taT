select
{batchid} batch_id,
{cycle_date} cycle_date,
{domain_name} domain,
{source_system_name} sys_nm,
{source_system_name} p_sys_nm,
'CONTROL TOTAL' measure_name,
'curated' hop_name,
'cu_base_age' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.policy_effective_history_daily_{cycledate}' measure_table,
sum(cast(CU_BASE_AGE as int)) as measure_value
from {source_database}.policy_effective_history_daily_{cycledate} PL
left anti join ltcg_expired_contracts expired on PL.policy_no = expired.policy_no
left join (
select
MIN_COV.Coverage_Unit_ID,
COV.*
from (
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID,
MIN(Coverage_Unit_ID) as Coverage_Unit_ID
from {source_database}.coverage_effective_history_daily_{cycledate} COVERAGE
where
cast(COVERAGE.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(COVERAGE.VIEW_ROW_OBSOLETE_DAY as date)
group by
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID
) MIN_COV
inner join (
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID,
CU_BASE_AGE,
Coverage_Unit_ID as cov_unit_id
from {source_database}.coverage_effective_history_daily_{cycledate} COVERAGE
where
cast(COVERAGE.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(COVERAGE.VIEW_ROW_OBSOLETE_DAY as date)
) COV on
MIN_COV.Policy_ID = COV.Policy_ID
and MIN_COV.Coverage_ID = COV.Coverage_ID
and MIN_COV.Coverage_Unit_ID = COV.cov_unit_id) CVCTA on
PL.Policy_ID = CVCTA.Policy_ID
and PL.Coverage_ID = CVCTA.Coverage_ID
union all
select
{batchid} batch_id,
{cycle_date} cycle_date,
{domain_name} domain,
{source_system_name} sys_nm,
{source_system_name} p_sys_nm,
'CONTROL TOTAL' measure_name,
'curated' hop_name,
'cu_base_age' measure_field,
'INTEGER' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.policy_effective_history_daily_{cycledate}' measure_table,
sum(cast(CU_BASE_AGE as int)) as measure_value
from {source_database}.policy_effective_history_daily_{cycledate} PL
left anti join ltcg_expired_contracts expired on PL.policy_no = expired.policy_no
left join (
select
MIN_COV.Coverage_Unit_ID,
COV.*
from (
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID,
MIN(Coverage_Unit_ID) as Coverage_Unit_ID
from {source_database}.coverage_effective_history_daily_{cycledate} COVERAGE
group by
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID
) MIN_COV
inner join (
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID,
CU_BASE_AGE,
Coverage_Unit_ID as cov_unit_id
from {source_database}.coverage_effective_history_daily_{cycledate} COVERAGE ) COV on
MIN_COV.Policy_ID = COV.Policy_ID
and MIN_COV.Coverage_ID = COV.Coverage_ID
and MIN_COV.Coverage_Unit_ID = COV.cov_unit_id) CVCTA on
PL.Policy_ID = CVCTA.Policy_ID
and PL.Coverage_ID = CVCTA.Coverage_ID
where
(cast(PL.view_row_create_day as date) <= '{cycle_date}'
or '{cycle_date}' > cast(PL.VIEW_ROW_OBSOLETE_DAY as date))
or cast(PL.VIEW_ROW_EFFECTIVE_DAY as date) > '{cycle_date}'
or '{cycle_date}' >= cast(PL.VIEW_ROW_EXPIRATION_DAY as date)
or (PL.POLICY_STATUS_CD in ('104', '105')
and PL.POLICY_STATUS_CD != '106'
or cast(PL.ORIGINAL_EFFECTIVE_DT as date) < '2021-01-01'))