-- 07+finaldf+sparksql+overwrite.sql
select
    from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') AS recorded_timestamp,
    '{source_system_name}' AS source_system_name,
    SHA2(concat(COALESCE(m1.sourceclaimidentifier,''),':',COALESCE(sourcesystemname.CNTRT_SRC_SYS_NM,''),':',COALESCE(m1.claimbenefitlineindicator,'')),256) as documentid,
    SHA2(concat(COALESCE(m1.contractnumber,''),':',COALESCE('LTCG','')),256) as fkcontractdocumentid,
    SHA2(concat(COALESCE(m1.sourceclaimidentifier,''),':',COALESCE(sourcesystemname.CNTRT_SRC_SYS_NM,''),':',COALESCE('LTCG','')),256) as fkclaimdocumentid,
    m1.sourceclaimidentifier,
    coalesce(sourcesystemname.CNTRT_SRC_SYS_NM,'@') as SourceSystemName,
    m1.contractnumber,
    'LTCG' as contractadministrationlocationcode,
    cast(m1.claimbenefitlineindicator as int) as claimbenefitlineindicator,
    cast(m1.claimbenefitamount as decimal(18,6)) as claimbenefitamount,
    cast(m1.claimbenefitclaiminvoicereceiveddate as date) as claimbenefitclaiminvoicereceiveddate,
    cast(m1.claimbenefitclaimpaymentdate as date) as claimbenefitclaimpaymentdate,
    m1.claimbenefitpartnershipexchangeindicator,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitpaymentreason.CNTRT_OPT_STS_RSN_CD,'@') as claimbenefitpaymentreasoncode,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitpaymentreason.CNTRT_OPT_STS_RSN_DESC,'@') as claimbenefitpaymentreasondescription,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason.CNTRT_OPT_STS_RSN_CD,'@') as claimbenefitpaymentsupplementaryreasoncode,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason.CNTRT_OPT_STS_RSN_DESC,'@') as claimbenefitpaymentsupplementaryreasondescription,
    cast(m1.claimbenefitprocesseddate as date) as claimbenefitprocesseddate,
    cast(m1.claimbenefitrequestedamount as decimal(18,6)) as claimbenefitrequestedamount,
    cast(m1.claimbenefitserviceenddate as date) as claimbenefitserviceenddate,
    cast(m1.claimbenefitservicestartdate as date) as claimbenefitservicestartdate,
    cast(m1.claimbenefitserviceunits as decimal(18,6)) as claimbenefitserviceunits,
    coalesce(claimbenefitstatuscode.CNTRT_OPT_CD,'@') as claimbenefitstatuscode,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.CNTRT_OPT_STS_RSN_CD,'@') as claimbenefitstatusreasoncode,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.CNTRT_OPT_STS_RSN_DESC,'@') as claimbenefitstatusreasondescription,
    m1.claimbenefitstypecode,
    coalesce(LKP_LOC_CD.CLM_BNFT_LOC_OF_CARE_CD,'@') as claimbenefitlocationareacode,
    coalesce(claimbenefitstatuscode.CNTRT_OPT_DESC,'@') as claimbenefitstatusdescription,
    coalesce(LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.CNTRT_OPT_STS_RSN_DESC,'@') as claimbenefitstatusreasondescription,
    m1.claimbenefitrequestforbenefitidentifier,
    m1.paymentrequestlineidentifier as claimbenefitpaymentrequestlineidentifier,
    cast('{cycle_date}' as date) as cycle_date,
    cast('{batchid}' as int) as batchid
from
    claimbenefit_src m1
left join sourcesystemname on
    sourcesystemname.CNTRT_SRC_SYS_NM_SRC = 'LTCG'
left join LKP_SRC_CNTRT_STS as LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode on
    LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.SRC_SYS_NM = sourcesystemname.CNTRT_SRC_SYS_NM
    and LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.SRC_INPUT_TYPE = 'Claim-Benefits'
    and LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.SRC_INPUT = m1.EB_STATUS_CD
    and LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.SRC_STS_RSN_CD = m1.EB_STATUS_RSN_CD
left join claimbenefitstatuscode on
    claimbenefitstatuscode.CNTRT_OPT_STS_RSN_CD = LKP_SRC_CNTRT_STS_claimbenefitstatusreasoncode.CNTRT_OPT_STS_RSN_CD
left join LKP_SRC_CNTRT_STS as LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason on
    LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason.SRC_SYS_NM = sourcesystemname.CNTRT_SRC_SYS_NM
    and LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason.SRC_INPUT_TYPE = 'Supplementary'
    and LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason.SRC_INPUT = m1.SUPPLEMENTARY_STMT_CD
    and LKP_SRC_CNTRT_STS_claimbenefitpaymentsupplementaryreason.SRC_STS_RSN_CD = '-'
left join LKP_SRC_CNTRT_STS as LKP_SRC_CNTRT_STS_claimbenefitpaymentreason on
    LKP_SRC_CNTRT_STS_claimbenefitpaymentreason.SRC_SYS_NM = sourcesystemname.CNTRT_SRC_SYS_NM
    and LKP_SRC_CNTRT_STS_claimbenefitpaymentreason.SRC_INPUT_TYPE = 'Payment'
    and LKP_SRC_CNTRT_STS_claimbenefitpaymentreason.SRC_INPUT = m1.SD_OK_REASON_DECISION_TYPE_CD
    and LKP_SRC_CNTRT_STS_claimbenefitpaymentreason.SRC_STS_RSN_CD = m1.SD_OK_REASON_CD
left join locationareacode as LKP_LOC_CD on
    m1.ClaimBenefitsTypeCode = LKP_LOC_CD.CLM_BNFT_TYP_CD