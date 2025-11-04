SELECT From_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
'{source_system_name}' as source_system_name,
Sha2(Concat(Coalesce(src.cntrt_src_sys_nm, ''), ':',
Coalesce(Coalesce(src.cntrt_admn_loc_cd, '@'), ':',
Coalesce(Coalesce(src.contractoptionsourcesystemordinalposition, ''), 256)) ,256) AS documentid,
Sha2(Concat(Coalesce(src.cntrt_src_sys_nm, ''), ':',
Coalesce(Coalesce(src.cntrt_admn_loc_cd, '@'), ':',
Coalesce(Coalesce(src.cntrt_src_sys_nm, ''), 256)) ,256) AS fkcontractdocumentid,
src.Contractnumber AS contractnumber,
Coalesce(src.cntrt_admn_loc_cd, '@') AS ContractAdministrationLocationCode,
Cast(src.contractoptionsourcesystemordinalposition AS INT) AS contractoptionsourcesystemordinalposition,
Cast(src.contractoptioncoverageunitid AS INT) AS contractoptioncoverageunitid,
src.contractoptionplancode AS contractoptionplancode,
Coalesce(dim_cntrt_sts.cntrt_cntrt_opt_sts_cd, '@') AS ContractoptionStatusCode,
Coalesce(SRC_CNTRT_STS.cntrt_opt_sts_rsn_cd, '@') AS ContractoptionStatusReasonCode,
Cast(src.cycle_date AS DATE) AS cycle_date,
Cast(src.batchid AS INT) AS batch_id
FROM contractoption_src src
LEFT JOIN lkp_cntrt_src_sys_nm CNTRT_SRC_SYS_NM ON CNTRT_SRC_SYS_NM.cntrt_src_sys_nm_src = 'LTCG'
LEFT JOIN lkp_cntrt_admn_loc_cd CNTRT_ADMN_LOC_CD ON CNTRT_SRC_SYS_NM.cntrt_src_sys_nm = CNTRT_ADMN_LOC_CD.cntrt_src_sys_nm_src
AND CNTRT_ADMN_LOC_CD.admin_prcss_leg_entty_cd_src = '4'
LEFT JOIN lkp_src_cntrt_sts SRC_CNTRT_STS ON CNTRT_SRC_SYS_NM.cntrt_src_sys_nm = SRC_CNTRT_STS.cntrt_src_sys_nm
AND SRC_CNTRT_STS.src_input_type = 'Contractoption'
AND SRC.coverage_status_cd = SRC_CNTRT_STS.src_sts_cd
AND SRC.coverage_status_reason_cd = SRC_CNTRT_STS.src_sts_rsn_cd
LEFT JOIN dim_cntrt_sts DIM_CNTRT_STS ON SRC_CNTRT_STS.cntrt_opt_sts_rsn_cd = dim_cntrt_sts.cntrt_opt_sts_rsn_cd
