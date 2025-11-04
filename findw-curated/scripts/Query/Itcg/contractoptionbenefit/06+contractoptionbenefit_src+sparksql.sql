select
'{batchid}' as batchid,
'{cycle_date}' as load_date,
'{cycle_date}' as cycle_date,
Coverage_TA.POLICY_ID,
Coverage_TA.COVERAGE_ID,
Coverage_TA.COVERAGE_UNIT_ID,
POLICY.POLICY_NO as ContractNumber,
Coverage_TA.COVERAGE_UNIT_ID as ContractOptionBenefitCoverageUnitID,
case
when Coverage_TA.COVERAGE_UNIT_ID = Coverage_TA1.min_Coverage_Unit_ID then '1'
else Coverage_TA.COVERAGE_UNIT_ID
end as ContractoptionsSourceSystemOrdinalPosition
from
(
select
POLICY_NO,
POLICY_ID,
COVERAGE_ID
from
{source_database}.policy_effective_history_daily_{cycledate} POLICY
where
cast(POLICY.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' <= cast(POLICY.VIEW_ROW_OBSOLETE_DAY as date)
and cast(POLICY.VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' < cast(POLICY.VIEW_ROW_EXPIRATION_DAY as date))
and (POLICY.POLICY_STATUS_CD in ('104', '105')
or (POLICY.POLICY_STATUS_CD = '106'
and cast(POLICY.ORIGINAL_EFFECTIVE_DT as date) >= '2021-01-01'))) POLICY
join (
select
POLICY_ID,
COVERAGE_ID,
COVERAGE_UNIT_ID
from
{source_database}.coverage_ta_effective_history_daily_{cycledate} COVERAGE_TA
where
cast(COVERAGE_TA.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and ('{cycle_date}' <= cast(COVERAGE_TA.VIEW_ROW_OBSOLETE_DAY as date))
and COVERAGE_TA.EXT_COV_TYPE_CD in ('173', '174')) COVERAGE_TA on
POLICY.POLICY_ID = Coverage_TA.POLICY_ID
and POLICY.COVERAGE_ID = Coverage_TA.COVERAGE_ID
left join (
select
policy_id,
COVERAGE_ID,
min(COVERAGE_UNIT_ID) as min_Coverage_Unit_ID
from
{source_database}.coverage_ta_effective_history_daily_{cycledate}
where
cast(view_row_create_day as date) <= '{cycle_date}'
and ('{cycle_date}' <= cast(view_row_obsolete_day as date))
and EXT_COV_TYPE_CD = '173'
group by
policy_id,
COVERAGE_ID) COVERAGE_TA1 on
Coverage_TA.POLICY_ID = Coverage_TA1.POLICY_ID
and Coverage_TA.COVERAGE_ID = Coverage_TA1.COVERAGE_ID
