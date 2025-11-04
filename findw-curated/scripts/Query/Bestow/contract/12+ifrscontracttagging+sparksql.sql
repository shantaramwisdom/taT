SELECT from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
       '{source_system_name}' AS source_system_name,
       SHA2(concat(a.sourcesystemname, ':', COALESCE(a.contractnumber, ''), ':', COALESCE(a.contractadministrationlocationcode, '@')), 256) AS documentid,
       a.sourcesystemname AS sourcesystemname,
       a.contractnumber,
       a.contractadministrationlocationcode,
       a.contractissuedate,
       a.sourcelegalentitycode,
       a.contractplancode,
       a.contracteffectiveatdate,
       COALESCE(LKP_LGL_ENTY_CD.LGL_ENTY_CD, '@') AS contractlegalcompanycode,
       a.contractreportinglineofbusiness,
       a.contractstatusreasoncode,
       COALESCE(DIM_CNTRT_STS.CNTRT_OPT_CD, '@') AS contractstatuscode,
       a.contractqualifiedindicator,
       a.contractsopsisindicator,
       a.contractgroupingindicator,
       a.contractcoverageid,
       a.contractparticipationindicator,
       a.contractsourcemarketingorganizationcode,
       a.contractproducttypename,
       a.originatingcontractnumber,
       a.originatingsourcesystemname,
       a.ifrs17portfolio AS contractifrs17portfolio,
       a.ifrs17grouping AS contractifrs17grouping,
       a.ifrs17cohort AS contractifrs17cohort,
       a.ifrs17profitability AS contractifrs17profitability,
       CAST('{cycle_date}' AS DATE) AS cycle_date,
       CAST('{batchid}' AS INT) AS batch_id,
-- IFRS17 CLOUD INSERT Flag
CASE WHEN a.TAG_VW_SYS_NM IS NULL AND a.TAG_VW_CNTRCT_NBR IS NULL AND (
          a.ifrs17portfolio IS NOT NULL OR a.ifrs17grouping IS NOT NULL OR
          a.ifrs17cohort IS NOT NULL OR a.ifrs17profitability IS NOT NULL)
     THEN 'Y'
     ELSE 'N' END AS rdm_insert_flag
FROM
(SELECT src.*,
        COALESCE(LKP_CNTRT_ADMN_LOC_CD.CNTRT_ADMN_LOC_CD, '@') AS contractadministrationlocationcode,
        COALESCE(LKP_STAT_CMPY_TRNSLATN.SRC_LGL_ENTY, '@') AS sourcelegalentitycode,
        COALESCE(LKP_SRC_CNTRT_STS.CNTRT_OPT_STS_RSN_CD, '@') AS contractstatusreasoncode,
        LKP_SRC_CNTRT_STS.SRC_INPUT,
        NULL AS contractqualifiedindicator,
        NULL AS contractsopsisindicator,
        NULL AS contractgroupingindicator,
        NULL AS contractcoverageid,
        NULL AS contractparticipationindicator,
        LKP_CNTRT_PRDCT_TYP_NM.CNTRT_PRDCT_TYP_NM AS contractproducttypename,
        src.originatingcontractnumber,
        src.issuing_company,
        src.originatingsourcesystemname,
CASE 
     WHEN LKP_IFRS17_CNTRT_TAG_VW.IFRS17_GRPNG IS NOT NULL THEN LKP_IFRS17_CNTRT_TAG_VW.IFRS17_GRPNG
     WHEN (to_timestamp('{cycle_date}', 'yyyy-MM-dd') BETWEEN LKP_IFRS17_NB_PRT_CHRT.IFRS17_COHORT_STRT_DT AND LKP_IFRS17_NB_PRT_CHRT.IFRS17_COHORT_END_DT)
     THEN LKP_IFRS17_NB_PRT_CHRT.IFRS17GROUPING
END AS ifrs17grouping,
CASE 
     WHEN LKP_IFRS17_CNTRT_TAG_VW.IFRS17_COHORT IS NOT NULL THEN LKP_IFRS17_CNTRT_TAG_VW.IFRS17_COHORT
     WHEN (to_timestamp('{cycle_date}', 'yyyy-MM-dd') BETWEEN LKP_IFRS17_NB_PRT_CHRT.IFRS17_COHORT_STRT_DT AND LKP_IFRS17_NB_PRT_CHRT.IFRS17_COHORT_END_DT)
     THEN LKP_IFRS17_NB_PRT_CHRT.IFRS17COHORT
END AS ifrs17cohort,
CASE 
     WHEN LKP_IFRS17_CNTRT_TAG_VW.IFRS17_PRTFOLIO IS NOT NULL THEN LKP_IFRS17_CNTRT_TAG_VW.IFRS17_PRTFOLIO
     WHEN (to_timestamp('{cycle_date}', 'yyyy-MM-dd') BETWEEN LKP_IFRS17_NB_PRT_CHRT.IFRS17_COHORT_STRT_DT AND LKP_IFRS17_NB_PRT_CHRT.IFRS17_COHORT_END_DT)
     THEN LKP_IFRS17_NB_PRT_CHRT.IFRS17PORTFOLIO
END AS ifrs17portfolio,
COALESCE(LKP_IFRS17_CNTRT_TAG_VW.IFRS17_PRFTBLY, LKP_HT_MP_IFRS17_PRFTBLY.IFRS17_PRFTBLY) AS ifrs17profitability,
LKP_IFRS17_CNTRT_TAG_VW.ORIGNTNG_SRC_SYS_NM AS TAG_VW_SYS_NM,
LKP_IFRS17_CNTRT_TAG_VW.ORIGNTNG_CNTRCT_NBR AS TAG_VW_CNTRCT_NBR,
COALESCE(DIM_LOB.rlob,'BI') as contractreportinglineofbusiness
From contract_src src
LEFT JOIN LKP_IFRS17_CNTRT_TAG_VW on src.originatingsourcesystemname = LKP_IFRS17_CNTRT_TAG_VW.ORIGNTNG_SRC_SYS_NM
and src.originatingcontractnumber = LKP_IFRS17_CNTRT_TAG_VW.ORIGNTNG_CNTRCT_NBR
LEFT JOIN LKP_HT_MP_IFRS17_PRFTBLY on src.contractissueage between LKP_HT_MP_IFRS17_PRFTBLY.Age_Low and LKP_HT_MP_IFRS17_PRFTBLY.Age_High
and (to_timestamp('{cycle_date}', 'yyyy-MM-dd') between LKP_HT_MP_IFRS17_PRFTBLY.Heat_Map_Start_Date and LKP_HT_MP_IFRS17_PRFTBLY.Heat_Map_End_Date)
and src.policyfaceamount between LKP_HT_MP_IFRS17_PRFTBLY.Face_Amount_Low and LKP_HT_MP_IFRS17_PRFTBLY.Face_Amount_High
and LKP_HT_MP_IFRS17_PRFTBLY.PRODUCT_NAME = src.contractplancode
and LKP_HT_MP_IFRS17_PRFTBLY.GENDER = src.insuredsex
and LKP_HT_MP_IFRS17_PRFTBLY.RISK_CLASS = src.riskclass
and LKP_HT_MP_IFRS17_PRFTBLY.PREMIUM_MODE = src.billing_mode
LEFT JOIN LKP_CNTRT_PRDCT_TYP_NM on src.sourcesystemname = LKP_CNTRT_PRDCT_TYP_NM.CNTRT_SRC_SYS_NM
LEFT JOIN LKP_IFRS17_NB_PRT_CHRT on src.originatingsourcesystemname = LKP_IFRS17_NB_PRT_CHRT.ORIG_SRC_SYS_NM
and src.contractplancode = LKP_IFRS17_NB_PRT_CHRT.CNTRT_PLN_CD
and LKP_CNTRT_PRDCT_TYP_NM.CNTRT_PRDCT_TYP_NM = LKP_IFRS17_NB_PRT_CHRT.CNTRT_PRDCT_TYP_NM
LEFT JOIN LKP_STAT_CMPY_TRNSLATN on src.sourcesystemname = LKP_STAT_CMPY_TRNSLATN.CNTRT_SRC_SYS_NM
and src.issuing_company = LKP_STAT_CMPY_TRNSLATN.ORIGNTNG_LGL_ENTY
LEFT JOIN LKP_CNTRT_ADMN_LOC_CD on src.SourceSystemName = LKP_CNTRT_ADMN_LOC_CD.CNTRT_SRC_SYS_NM_SRC
LEFT JOIN LKP_SRC_CNTRT_STS on src.sourcesystemname = LKP_SRC_CNTRT_STS.SRC_SYS_NM
and trim(src.policy_status) = trim(LKP_SRC_CNTRT_STS.SRC_INPUT)
LEFT JOIN DIM_LOB on src.contractplancode = DIM_LOB.product_cd
and CASE 
         WHEN src.contractsourcemarketingorganizationcode = 'WR15' THEN src.contractsourcemarketingorganizationcode
         ELSE 'Default'
    END = DIM_LOB.marketing_org
and src.contractissuedate between to_date(DIM_LOB.start_date,'M/d/y') and to_date(DIM_LOB.end_date,'M/d/y'))a
LEFT JOIN DIM_CNTRT_STS ON trim(src.policy_status) = DIM_CNTRT_STS.CNTRT_OPT_STS_RSN_CD
LEFT JOIN LKP_LGL_ENTY_CD ON src.sourcesystemname = LKP_LGL_ENTY_CD.CNTRT_SRC_SYS_NM
AND a.sourcelegalentitycode = LKP_CNTRT_ADMN_LOC_CD.SRC_LGL_ENTY_CD
