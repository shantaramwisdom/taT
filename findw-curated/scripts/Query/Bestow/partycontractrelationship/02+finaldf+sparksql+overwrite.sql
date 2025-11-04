SELECT from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
       '{source_system_name}' AS source_system_name,
       SHA2(concat(fkcontractdocumentid,'','Primary Insured'), 256) AS documentid,
       fkcontractdocumentid,
       fkpartydocumentid,
       '{source_system_name}' AS sourcesystemname,
       contractnumber,
       contractadministrationlocationcode,
       sourcepartyid,
       'Primary Insured' AS relationshiptype,
       CAST('{cycle_date}' AS date) AS cycle_date,
       CAST('{batchid}' AS INT) AS batch_id
FROM (SELECT pmf.policy_number AS contractnumber,
             coalesce(LKP_CNTRT_ADMN_LOC_CD.CNTRT_ADMN_LOC_CD, '@') AS contractadministrationlocationcode,
             SHA2(concat('Bestow','',coalesce(pmf.policy_number,''),coalesce(pmf.insured_first_name,''),coalesce(pmf.insured_last_name,'')),256) AS fkcontractdocumentid,
             SHA2(concat(coalesce(pmf.policy_number,''),coalesce(pmf.insured_last_name,''),coalesce(pmf.insured_first_name,'')),256) AS fkpartydocumentid,
             SHA2(concat(coalesce(pmf.policy_number,''),coalesce(pmf.insured_last_name,''),coalesce(pmf.insured_first_name,'')),256) AS sourcepartyid
      FROM {source_database}.pmf_daily_{cycledate} pmf
      LEFT JOIN LKP_CNTRT_ADMN_LOC_CD ON CNTRT_SRC_SYS_NM_SRC = 'Bestow'
      WHERE cycle_date = {cycledate}
        AND (insured_first_name IS NOT NULL OR insured_last_name IS NOT NULL)
        AND policy_status not in ('APPROVED_SCHEDULED','APPROVED_STALE'))
