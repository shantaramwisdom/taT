SELECT src.*,
       LKP_LGL_ENTY_CD.LGL_ENTY_CD AS activitylegalcompanycode,
       COALESCE(LKP_ACTVTY_TYP_TYPGRP.ACTVTY_AMT_TYP,'@') AS activityamounttype,
       COALESCE(LKP_ACTVTY_TYP_TYPGRP.ACTVTY_TYP,'@') AS activitytype,
       COALESCE(LKP_ACTVTY_TYP_TYPGRP.ACTVTY_TYP_GRP,'@') AS activitytypegroup,
       concat(activityamounttype,'_',src.contractnumber) AS activitysourcesystemactivityid,
       concat(activityamounttype,'_',src.contractnumber,'_',src.activityreporteddate) AS sourceactivityid
FROM
(SELECT src.*,
        CASE
            WHEN contractnumber IS NOT NULL THEN SHA2(concat('{source_system_name}',':',coalesce(contractnumber,''),':',COALESCE(CNTRT_ADMN_LOC_CD,'@')),256)
            ELSE NULL
        END AS fkcontractdocumentid,
        LKP_ACTVTY_GL.activitygeneralledgersourcecode,
        LKP_ACTVTY_GL.activitygeneralledgerapplicationareacode,
        SRC_LGL_ENTY.SRC_LGL_ENTY AS activitysourcelegalentitycode,
        COALESCE(LKP_SRC_SYS_ACTVTY_MEMO_CD.ACT_SRC_ACCT_MEMO_CD,'@') AS activitysourceaccountingmemocode,
        COALESCE(EVE_LKP_ACT_MNY_MTHD.ACT_MNY_MTHD_DESC,'@') AS activitymoneymethod,
        COALESCE(LKP_CNTRT_ADMN_LOC_CD.CNTRT_ADMN_LOC_CD,'@') AS contractadministrationlocationcode,
        COALESCE(LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACT_SRCSYS_ACG_BAL_ENT_IND,'@') AS activityaccountingbalancedentryindicator
 FROM everest_fee_calculation src
 LEFT JOIN LKP_ACTVTY_GL ON src.sourcesystemname = LKP_ACTVTY_GL.ACTVTY_SRC_SYS_NM
 LEFT JOIN SRC_LGL_ENTY ON src.issuing_company = SRC_LGL_ENTY.ORIGNTG_LGL_ENTY
 LEFT JOIN LKP_SRC_SYS_ACTVTY_MEMO_CD ON src.activitysourcetransactioncode = LKP_SRC_SYS_ACTVTY_MEMO_CD.SRC_SYS_ACCT_MEMO_CD
 LEFT JOIN EVE_LKP_ACT_MNY_MTHD ON src.activitysourcetransactioncode = EVE_LKP_ACT_MNY_MTHD.ACT_SRC_TRXN_CD
 LEFT JOIN LKP_CNTRT_ADMN_LOC_CD ON src.sourcesystemname = LKP_CNTRT_ADMN_LOC_CD.CNTRT_SRC_SYS_NM_SRC
 LEFT JOIN LKP_ACT_SRCSYS_ACG_BAL_ENT_IND ON 
           LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACTVTY_SRC_SYS_NM = 'Bestow'
           AND LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACTVTY_SRC_TRXN_CD = src.activitysourcetransactioncode
           AND LKP_ACT_SRCSYS_ACG_BAL_ENT_IND.ACTVTY_SRC_ACCTNG_MEMO_CD = COALESCE(LKP_SRC_SYS_ACTVTY_MEMO_CD.ACT_SRC_ACCT_MEMO_CD,'@')
) src
LEFT JOIN LKP_LGL_ENTY_CD ON src.activitysourcelegalentitycode = LKP_LGL_ENTY_CD.SRC_LGL_ENTY_CD
LEFT JOIN LKP_ACTVTY_TYP_TYPGRP ON src.sourcesystemname = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_SRC_SYS_NAME
AND src.activitysourcetransactioncode = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_SRC_TRN_CD
AND src.activitysourceaccountingmemocode = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_SRC_ACCTNG_MEMO_CD
AND src.activityaccountingbalancedentryindicator = LKP_ACTVTY_TYP_TYPGRP.ACTSRCSYS_ACG_BAL_ENT_IND
AND src.activityfirstyearrenewalindicator = LKP_ACTVTY_TYP_TYPGRP.ACTVTY_FRST_YR_RNWL_IND) SRC