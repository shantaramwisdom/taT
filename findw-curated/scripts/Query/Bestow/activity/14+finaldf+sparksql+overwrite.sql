SELECT 
       from_utc_timestamp(CURRENT_TIMESTAMP,'US/Central') AS recorded_timestamp,
       '{source_system_name}' AS source_system_name,
       SHA2(concat(COALESCE(src.sourcesystemname,''),':',COALESCE(src.source_activity_id,''),':',COALESCE(src.activitytype,''),':',COALESCE(src.activityamounttype,'')),256) AS documentid,
       fkcontractdocumentid,
       src.sourcesystemname,
       generateSameUUID_uuids(concat(COALESCE(src.sourcesystemname,''),':',COALESCE(src.activitytype,''),':',COALESCE(src.source_activity_id,''),':',COALESCE(cast('{cycle_date}' as string),''))) as activityaccountingid,
       sha2(concat(src.source_activity_id,256)) AS sourceactivityid,
       src.activitysourcesystemactivityid,
       src.activityamounttype,
       src.activitytype,
       src.contractnumber,
       src.contractadministrationlocationcode,
       src.activityaccountingbalancedentryindicator,
       CAST(src.activityamount AS DECIMAL(18,6)) AS activityamount,
       CAST(src.activityreporteddate AS date) AS activityreporteddate,
       CAST(src.activityeffectivedate AS date) AS activityeffectivedate,
       src.activitygeneralledgerapplicationareacode,
       src.activitygeneralledgersourcecode,
       src.activitylegalcompanycode,
       src.activityreversalcode,
       src.activitysourcetransactioncode,
       src.activitysourceactivityparentid,
       src.fundsourcefundidentifier,
       src.fundclassindicator,
       src.activitysourceaccountingdeviator,
       src.activitysourcedepositidentifier,
       src.activityfirstyearrenewalindicator,
       src.activitytypegroup,
       src.activitymoneymethod,
       src.activitysourceaccountingmemocode,
       CAST('{cycle_date}' AS date) AS cycle_date,
       CAST({batchid} AS INT) AS batch_id
FROM (SELECT src.*,
             LKP_LGL_ENTY_CD.LGL_ENTY_CD AS activitylegalcompanycode,
             CASE
                  WHEN src.trn_activity_type IN ('P','D','L','F') THEN COALESCE(LKP_ACTVTY_TYP_TYPGRP.ACTVTY_AMT_TYP,'@')
                  ELSE NULL
             END AS activityamounttype,
             CASE
                  WHEN src.trn_activity_type IN ('P','D','L','F') THEN COALESCE(LKP_ACTVTY_TYP_TYPGRP.ACTVTY_TYP,'@')
                  ELSE NULL
             END AS activitytype,
             CASE
                  WHEN src.trn_activity_type IN ('P','D','L','F') THEN COALESCE(LKP_ACTVTY_TYP_TYPGRP.ACTVTY_TYP_GRP,'@')
                  ELSE NULL
             END AS activitytypegroup
      FROM (SELECT src.*,
                   CASE
                        WHEN contractnumber IS NOT NULL THEN SHA2(concat('{source_system_name}',':',coalesce(contractnumber,''),':',COALESCE(CNTRT_ADMN_LOC_CD,'@')),256)
                        ELSE NULL
                   END AS fkcontractdocumentid,
                   LKP_ACTVTY_GL.activitygeneralledgersourcecode,
                   LKP_ACTVTY_GL.activitygeneralledgerapplicationareacode,
                   coalesce(SRC_LGL_ENTY.SRC_LGL_ENTY,'TLC') AS activitysourcelegalentitycode,
                   COALESCE(LKP_SRC_SYS_ACTVTY_MEMO_CD.ACT_SRC_ACCT_MEMO_CD,'@') AS activitysourceaccountingmemocode,
                   COALESCE(LKP_ACT_MNY_MTHD.ACT_MNY_MTHD_DESC,'@') AS activitymoneymethod,
                   COALESCE(LKP_CNTRT_ADMN_LOC_CD.CNTRT_ADMN_LOC_CD,'@') AS contractadministrationlocationcode,
                   COALESCE(LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACT_SRCSYS_ACG_BAL_ENT_IND,'@') AS activityaccountingbalancedentryindicator
            FROM transaction_src src
            LEFT JOIN LKP_ACTVTY_GL ON src.sourcesystemname = LKP_ACTVTY_GL.ACTVTY_SRC_SYS_NM
            LEFT JOIN SRC_LGL_ENTY ON src.issuing_company = SRC_LGL_ENTY.ORIGNTG_LGL_ENTY
            LEFT JOIN LKP_SRC_SYS_ACTVTY_MEMO_CD ON src.reason_code = LKP_SRC_SYS_ACTVTY_MEMO_CD.SRC_SYS_ACCT_MEMO_CD
            LEFT JOIN LKP_ACT_MNY_MTHD ON COALESCE(src.payment_method,'-') = LKP_ACT_MNY_MTHD.SRC_ACT_MNY_MTHD
            LEFT JOIN LKP_CNTRT_ADMN_LOC_CD ON src.sourcesystemname = LKP_CNTRT_ADMN_LOC_CD.CNTRT_SRC_SYS_NM_SRC
            LEFT JOIN LKP_ACT_SRCSYS_ACG_BAL_ENT_IND ON 
            LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACTVTY_SRC_TRXN_CD = src.activitysourcetransactioncode
            AND LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACTVTY_SRC_ACCTNG_MEMO_CD = COALESCE(LKP_SRC_SYS_ACTVTY_MEMO_CD.ACT_SRC_ACCT_MEMO_CD,'@')
      ) src
      LEFT JOIN LKP_LGL_ENTY_CD ON src.activitysourcelegalentitycode = LKP_LGL_ENTY_CD.SRC_LGL_ENTY_CD
      LEFT JOIN LKP_ACTVTY_TYP_TYPGRP ON src.sourcesystemname = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_SRC_SYS_NAME
      AND src.activitysourcetransactioncode = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_SRC_TRN_CD
      AND src.activitysourceaccountingmemocode = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_SRC_ACCTNG_MEMO_CD
      AND src.activityaccountingbalancedentryindicator = LKP_ACTVTY_TYP_TYPGRP.ACTSRCSYS_ACG_BAL_ENT_IND
      AND src.activityfirstyearrenewalindicator = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_FRST_YR_RNWL_IND
      AND (CASE
              WHEN src.trn_activity_type = 'P' THEN src.activityfirstyearrenewalindicator = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_FRST_YR_RNWL_IND
              ELSE 1 = 1
           END)
) src
UNION ALL (SELECT * FROM everest_fee_finaldf)