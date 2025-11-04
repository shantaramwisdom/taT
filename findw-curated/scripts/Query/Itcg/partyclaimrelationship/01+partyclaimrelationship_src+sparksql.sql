select
'{batchid}' as batchid,
POLICY.POLICY_NO as ContractNumber,
concat(cast(POLICY.POLICY_NO as string), '_', claim_eligibility.RFB_ID) as SourceclaimIdentifier,
concat('LTCG.LIFE.ID:', life.life_id) as SourcePartyID,
'CLNT' as SRC_RLTNSP_TYP,
'{cycle_date}' as cycle_date
from
(
select
ceehc.POLICY_ID,
ceehc.RFB_ID
from
{source_database}.claim_eligibility_effective_history_daily_{cycledate} ceehc
where
cast(ceehc.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(ceehc.VIEW_ROW_OBSOLETE_DAY as date)
and cast(ceehc.VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(ceehc.VIEW_ROW_EXPIRATION_DAY as date)
group by
ceehc.POLICY_ID,
ceehc.RFB_ID
) claim_eligibility
inner join
(
select
*
from
{source_database}.Policy_effective_history_daily_{cycledate} pehc
where
cast(pehc.View_Row_Create_Day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(pehc.View_Row_Obsolete_Day as date)
and cast(pehc.View_Row_Effective_Day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(pehc.View_Row_Expiration_Day as date)
and (pehc.POLICY_STATUS_CD in ('104','105')
or (pehc.POLICY_STATUS_CD = '106'
and pehc.ORIGINAL_EFFECTIVE_DT >= '2021-01-01'))) POLICY on
CLAIM_ELIGIBILITY.POLICY_ID = POLICY.POLICY_ID
inner join
(
select
*
from
{source_database}.Life_effective_history_daily_{cycledate} lehc
where
cast(lehc.View_Row_Create_Day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(lehc.View_Row_Obsolete_Day as date)
and cast(lehc.View_Row_Effective_Day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(lehc.View_Row_Expiration_Day as date)
) LIFE on
POLICY.INSURED_LIFE_ID = LIFE.LIFE_ID
