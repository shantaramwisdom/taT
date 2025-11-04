select
    'LTCG' AS sourcesystemname,
    claim_eligibility.CLAIM_STATUS_RSN_CD as ClaimReasonCode,
    COALESCE(claim_eligibility.CLAIM_STATUS_RSN_DESC,'$') as ClaimReasonDescription,
    Policy.POLICY_NO as ContractNumber,
    concat(concat(Policy.POLICY_NO,'_'), claim_eligibility.RFB_ID) as SourceClaimIdentifier,
    claim_eligibility.RFB_ID as claimrequestforbenefitidentifier,
    claim_eligibility.DE_ICD_CODE as ClaimAdmittingDiagnosisCode,
    case
        when claim_eligibility.EB_COGNITIVE_IMP_CD = '1' then 'Y'
        when claim_eligibility.EB_COGNITIVE_IMP_CD = '2' then 'N'
        else '$'
    end as ClaimClaimantCognitiveImpairmentIndicator,
    (case
        when claim_eligibility.EB_ADL_AMBULATION_CD in ('1','2') then 1
        else 0
    end) + (case
        when claim_eligibility.EB_ADL_BATHING_CD in ('1','2') then 1
        else 0
    end) + (case
        when claim_eligibility.EB_ADL_DRESSING_CD in ('1','2') then 1
        else 0
    end) + (case
        when claim_eligibility.EB_ADL_FEEDING_CD in ('1','2') then 1
        else 0
    end) + (case
        when claim_eligibility.EB_ADL_INCONTINENCE_CD in ('1','2') then 1
        else 0
    end) + (case
        when claim_eligibility.EB_ADL_TOILETING_CD in ('1','2') then 1
        else 0
    end) + (case
        when claim_eligibility.EB_ADL_TRANSFERRING_CD in ('1','2') then 1
        else 0
    end) as ClaimCurrentADLDependencyCount,
    cast(claim_eligibility.EB_END_DT as date) as ClaimDisabilityEndDate,
    Claim_Eligibility.EB_ICD_CODE as ClaimPrimaryDiagnosisCodeAtReassessment,
    Claim_Eligibility.PD_ICD_CODE as ClaimPrimaryDiagnosisCode,
    '{cycle_date}' as load_date,
    claim_eligibility.RFB_STATUS_CD,
    claim_eligibility.CLAIM_STATUS_CD,
    claim_eligibility.EB_STATUS_CD,
    cast('{cycle_date}' as date) as cycle_date,
    cast('{batchid}' as int) as batchid,
    SHA2(concat(COALESCE(SourceClaimIdentifier,''),':',COALESCE(SourceSystemName,'')),256) as documentid
from
(
    select
    *
    from
    {source_database}.Claim_Eligibility_effective_history_daily_{cycledate} ceehc
    where
    cast(ceehc.VIEW_ROW_CREATE_DAY as date) <= '{cycle_date}'
        and '{cycle_date}' <= cast(ceehc.VIEW_ROW_OBSOLETE_DAY as date)
        and cast(ceehc.VIEW_ROW_EFFECTIVE_DAY as date) <= '{cycle_date}'
        and '{cycle_date}' < cast(ceehc.VIEW_ROW_EXPIRATION_DAY as date)
) claim_eligibility
inner join (
    select
    *
    from
    {source_database}.Policy_effective_history_daily_{cycledate} pehc
    where
    cast(pehc.View_Row_Create_Day as date) <= '{cycle_date}'
        and '{cycle_date}' <= cast(pehc.View_Row_Obsolete_Day as date)
        and cast(pehc.View_Row_Effective_Day as date) <= '{cycle_date}'
        and '{cycle_date}' < cast(pehc.View_Row_Expiration_Day as date)
        and (pehc.POLICY_STATUS_CD in ('104','105')
        or (pehc.POLICY_STATUS_CD = '106'
        and pehc.ORIGINAL_EFFECTIVE_DT >= '2021-01-01'))
) POLICY on
    CLAIM_ELIGIBILITY.POLICY_ID = POLICY.POLICY_ID