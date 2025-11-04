-- 06+finaldf+sparksql+overwrite.sql
select
    from_utc_timestamp(current_timestamp,'US/Central') AS recorded_timestamp,
    '{source_system_name}' AS source_system_name,
    m1.documentid,
    SHA2(concat(COALESCE(m1.ContractNumber,''),':',coalesce(m7.CNTRT_ADMN_LOC_CD,''),':',COALESCE('LTCG','')),256) as fkcontractdocumentid,
    m1.sourceclaimidentifier,
    'LTCG' as sourcesystemname,
    m1.contractnumber,
    coalesce(m7.CNTRT_ADMN_LOC_CD,'@') as ContractAdministrationLocationCode,
    m1.claimadmittingdiagnosiscode,
    m1.claimclaimantcognitiveimpairmentindicator,
    m1.claimcurrentadldependencycount,
    m1.claimdisabilityenddate,
    m1.claimprimarydiagnosiscode,
    m1.claimprimarydiagnosiscodeatreassessment,
    m1.claimreasoncode,
    coalesce(m4.CNTRT_CNTRT_OPT_CD,'@') as ClaimRequestForBenefitStatusCode,
    case
        when coalesce(m1.CLAIM_STATUS_CD,'') = '' then '$'
        when CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) in ('71','72')
            and CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) = m5.SRC_INPUT then m6.CNTRT_CNTRT_OPT_CD
        when CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) not in ('71','72')
            and m1.CLAIM_STATUS_CD = m8.SRC_INPUT then m9.CNTRT_CNTRT_OPT_CD
        else '@'
    end as ClaimStatusCode,
    case
        when coalesce(m1.CLAIM_STATUS_CD,'') = '' then '$'
        when CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) in ('71','72')
            and CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) = m5.SRC_INPUT then m6.CNTRT_CNTRT_OPT_DESC
        when CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) not in ('71','72')
            and m1.CLAIM_STATUS_CD = m8.SRC_INPUT then m9.CNTRT_CNTRT_OPT_DESC
        else '$'
    end as ClaimStatusDescription,
    m1.claimreasondescription,
    coalesce(m4.CNTRT_CNTRT_OPT_DESC,'@') as ClaimRequestForBenefitStatusDescription,
    m1.claimrequestforbenefitidentifier,
    m1.cycle_date,
    m1.batchid
from
    claim_src m1
--left anti join ltcg_expired_contracts expired on m1.contractnumber = expired.policy_no
left join SOURCESYSTEMNAME m2 on
    m2.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join LKP_SRC_CNTRT_STS m3 on
    coalesce(m2.CNTRT_SRC_SYS_NM,'@') = m3.SRC_SYS_NM
    and m1.RFB_STATUS_CD = m3.SRC_INPUT
    and m3.SRC_STS_RS_CD = 'R'
    and m3.SRC_INPUT_TYPE = 'Request'
left join DIM_CNTRT_STS m4 on
    coalesce(m3.CNTRT_OPT_STS_RSN_CD,'@') = m4.CNTRT_OPT_STS_RSN_CD
left join LKP_SRC_CNTRT_STS m5 on
    coalesce(m2.CNTRT_SRC_SYS_NM,'@') = m5.SRC_SYS_NM
    and m5.SRC_INPUT_TYPE = 'Claim'
    and CONCAT(m1.CLAIM_STATUS_CD,coalesce(m1.EB_STATUS_CD,'')) = m5.SRC_INPUT
    and m5.SRC_STS_RSN_CD = 'A'
left join DIM_CNTRT_STS m6 on
    coalesce(m5.CNTRT_OPT_STS_RSN_CD,'@') = m6.CNTRT_OPT_STS_RSN_CD
left join LKP_CNTRT_ADMN_LOC_CD m7 on
    m7.CNTRT_SRC_SYS_NM = coalesce(m2.CNTRT_SRC_SYS_NM,'@')
    and m7.ADMN_PRCS_LEG_ENTY_CD_SRC = '~'
