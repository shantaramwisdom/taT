select
POLICY.POLICY_NO as ContractNumber,
case
when char_length(POLICY.POLICY_NO) in (11, 12)
or (char_length(POLICY.POLICY_NO) not in (11, 12)
and SUBSTRING(POLICY.POLICY_NO, 3, 2) <> 'B9') then 'PI'
when char_length(POLICY.POLICY_NO) not in (11, 12)
and SUBSTRING(POLICY.POLICY_NO, 3, 2) = 'B9' then 'CI'
else null
end as SRC_RLTNSP_TYP,
CONCAT(CONCAT('LTCG.LIFE.ID:', LIFE.LIFE_ID)) as SourcePartyID,
'{cycle_date}' as cycle_date,
'{batchid}' as batch_id
from
(
select
*
from
{source_database}.LIFE_effective_history_daily_{cycledate}
where
cast(View_Row_Create_Day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(View_Row_Obsolete_Day as date)
and cast(View_Row_Effective_Day as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(View_Row_Expiration_Day as date)
) LIFE
inner join (
select
*
from
{source_database}.Policy_effective_history_daily_{cycledate}
where
cast(VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(VIEW_ROW_OBSOLETE_DAY as date)
and cast(VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
and '{cycle_date}' <= cast(VIEW_ROW_EXPIRATION_DAY as date)
and (POLICY_STATUS_CD in ('104', '105')
or (POLICY_STATUS_CD = '106'
and cast(ORIGINAL_EFFECTIVE_DT as date) >= '2021-01-01'))) POLICY on
LIFE.Life_ID = POLICY.Insured_Life_ID
