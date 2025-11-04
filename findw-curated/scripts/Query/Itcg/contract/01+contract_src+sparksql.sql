select
PL.ORIGINAL_EFFECTIVE_DT as contracteffectivedate,
coalesce(PL.initial_coverage_issue_dt, PL.ORIGINAL_EFFECTIVE_DT) as contractissuedate,
PL.policy_id,
PL.APPN_RCV_DT,
PL.TAX_QUALIFICATION_CD ,
PL.TAX_QUALIF_GRANDFATHERED_FLG,
PL.coverage_status_rsn_cd,
PL.policy_status_cd,
PL.policy_no as ContractNumber,
PL.PARTNERSHIP_STATUS_CD,
PL.filing_state,
Business_Group.GRP_REFERENCE,
Business_Group.Parent_GRP_ID,
Business_Group.GML_LEVEL,
Business_Group.GRP_ID,
Business_Group.BG1_GML_LEVEL,
Business_Group.BG1_GRP_REFERENCE,
CVCTA.coverage_exchange_type_cd,
CVCTA.CU_BASE_AGE,
CVCTA.COVERAGE_ID,
CVCTA.REPORTING_LOB,
CVCTA.ADMIN_SYSTEM_PLAN_CD,
AC.FREQUENCY_VALUE,
AC.BILLING_TYPE_CD,
AC.Account_STATUS_CD,
CP.ANNUAL_PREMIUM_AMT_SUM,
'{cycle_date}' as cycle_date,
'{batchid}' as batchid
from (
select
POLICY.original_effective_dt,
POLICY.initial_coverage_issue_dt,
POLICY.policy_id,
POLICY.APPN_RCV_DT,
POLICY.TAX_QUALIFICATION_CD ,
POLICY.TAX_QUALIF_GRANDFATHERED_FLG,
POLICY.coverage_status_rsn_cd,
POLICY.PARTNERSHIP_STATUS_CD,
POLICY.policy_status_cd,
POLICY.policy_no,
POLICY.Coverage_ID,
POLICY.GRP_ID,
POLICY.filing_state,
POLICY.Cycle_Date
from
{source_database}.policy_effective_history_daily_{cycledate} POLICY
where
(cast(POLICY.view_row_create_day as date) <= ('{cycle_date}'
and '{cycle_date}' = cast(POLICY.VIEW_ROW_OBSOLETE_DAY as date))
and cast(POLICY.VIEW_ROW_EFFECTIVE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' < cast(POLICY.VIEW_ROW_EXPIRATION_DAY as date))
and (POLICY.POLICY_STATUS_CD in ('104','105')
or POLICY.POLICY_STATUS_CD = '106'
and cast(POLICY.ORIGINAL_EFFECTIVE_DT as date) >= '2021-01-01')) ) PL
left join (
select
CV.coverage_exchange_type_cd,
CV.CU_BASE_AGE,
CV.COVERAGE_ID,
CTA.REPORTING_LOB,
CTA.ADMIN_SYSTEM_PLAN_CD,
CV.POLICY_ID
from
(
select
MIN_COV.Coverage_Unit_ID,
COV.*
from
(
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID,
MIN(Coverage_Unit_ID) as Coverage_Unit_ID
from
{source_database}.coverage_effective_history_daily_{cycledate} COVERAGE
where
cast(COVERAGE.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(COVERAGE.VIEW_ROW_OBSOLETE_DAY as date))
group by
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID ) MIN_COV
inner join (
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID,
CU_BASE_AGE,
COVERAGE_EXCHANGE_TYPE_CD,
Coverage_Unit_ID as cov_unit_id
from
{source_database}.coverage_effective_history_daily_{cycledate} COVERAGE
where
cast(COVERAGE.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(COVERAGE.VIEW_ROW_OBSOLETE_DAY as date)) ) COV on
MIN_COV.POLICY_ID = COV.POLICY_ID
and MIN_COV.Coverage_ID = COV.Coverage_id
and MIN_COV.Coverage_Unit_ID = COV.cov_unit_id) CV
left join (
select
CTA.*,
MIN_CTA.coverage_unit_id
from
(
select
MIN(coverage_unit_id) as coverage_unit_id,
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
POLICY_ID,
COVERAGE_ID
from
{source_database}.Coverage_TA_effective_history_daily_{cycledate} Coverage_TA
where
(cast(Coverage_TA.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(Coverage_TA.VIEW_ROW_OBSOLETE_DAY as date)))
and Coverage_TA.EXT_COV_TYPE_CD = '173'
group by
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
Policy_ID,
Coverage_ID) MIN_CTA
inner join (
select
REPORTING_LOB,
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
POLICY_ID,
COVERAGE_ID,
ADMIN_SYSTEM_PLAN_CD,
coverage_unit_id as CVUNITID
from
{source_database}.Coverage_TA_effective_history_daily_{cycledate} Coverage_TA
where
(cast(Coverage_TA.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(Coverage_TA.VIEW_ROW_OBSOLETE_DAY as date))) ) CTA on
MIN_CTA.POLICY_ID = CTA.POLICY_ID
and MIN_CTA.COVERAGE_ID = CTA.COVERAGE_ID
and MIN_CTA.coverage_unit_id = CTA.CVUNITID) CTA on
CV.POLICY_ID = CTA.POLICY_ID
and CV.COVERAGE_ID = CTA.COVERAGE_ID) CVCTA on
PL.Policy_ID = CVCTA.Policy_ID
and PL.Coverage_ID = CVCTA.Coverage_ID
Left join (
select
BG2.*,
BG1.GML_LEVEL,
BG1_GRP_REFERENCE
from
(
select
*
from
{source_database}.business_group_effective_history_daily_{cycledate} BUSINESS_GROUP
where
cast(BUSINESS_GROUP.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(BUSINESS_GROUP.VIEW_ROW_OBSOLETE_DAY as date))
and BUSINESS_GROUP.GML_LEVEL = '2') JBG2
left join (
select
GRP_ID as BG1_GRP_ID,
GML_LEVEL as BG1_GML_LEVEL,
case
when GRP_REFERENCE is null then 'S'
else GRP_REFERENCE
end as BG1_GRP_REFERENCE
from
{source_database}.business_group_effective_history_daily_{cycledate} BUSINESS_GROUP
where
cast(BUSINESS_GROUP.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(BUSINESS_GROUP.VIEW_ROW_OBSOLETE_DAY as date))
and BUSINESS_GROUP.GML_LEVEL = '1') BG1 on
BG2.Parent_GRP_ID = BG1.BG1_GRP_ID ) Business_Group on
PL.GRP_ID = Business_Group.GRP_ID
Left join (
select
*
from
{source_database}.Account_effective_history_daily_{cycledate} Account
where
cast(Account.VIEW_ROW_CREATE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(Account.VIEW_ROW_OBSOLETE_DAY as date))
and cast(Account.VIEW_ROW_EFFECTIVE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' < cast(Account.VIEW_ROW_EXPIRATION_DAY as date)) ) AC on
PL.POLICY_ID = AC.POLICY_ID
Left join (
select
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
VIEW_ROW_EFFECTIVE_DAY,
VIEW_ROW_EXPIRATION_DAY,
Policy_ID,
Coverage_ID,
cast(sum(ANNUAL_PREMIUM_AMT) as decimal(18,2)) as ANNUAL_PREMIUM_AMT_SUM
from
{source_database}.coverage_premium_effective_history_daily_{cycledate} CP
where
cast(View_ROW_Create_Day as date) <= ('{cycle_date}'
and '{cycle_date}' <= cast(View_ROW_Obsolete_Day as date))
and cast(VIEW_ROW_EFFECTIVE_DAY as date) <= ('{cycle_date}'
and '{cycle_date}' < cast(VIEW_ROW_EXPIRATION_DAY as date))
group by
VIEW_ROW_CREATE_DAY,
VIEW_ROW_OBSOLETE_DAY,
VIEW_ROW_EFFECTIVE_DAY,
VIEW_ROW_EXPIRATION_DAY,
Policy_ID,
Coverage_ID )cp on
PL.POLICY_ID = CP.POLICY_ID
and PL.COVERAGE_ID = CP.COVERAGE_ID
