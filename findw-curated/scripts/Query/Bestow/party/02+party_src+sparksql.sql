SELECT 'Bestow' AS sourcesystemname,
       insured_sex,
       insured_dob AS dateofbirth,
       insured_first_name AS partyfirstname,
       insured_middle_name AS partymiddlename,
       insured_last_name AS partylastname,
       insured_street_address AS partyprimaryaddressline1,
       insured_city AS partyprimarycityname,
       insured_postal_code AS partyprimarypostalcode,
       insured_state,
       SHA2(concat('Bestow', ':', COALESCE(policy_number, ''), ':', COALESCE(CNTRT_ADMN_LOC_CD, '@'), ':', COALESCE(insured_first_name, ''), ':', COALESCE(insured_last_name, '')), 256) AS documentid,
       SHA2(concat(COALESCE(policy_number, ''), ':', COALESCE(CNTRT_ADMN_LOC_CD, '@'), ':', COALESCE(insured_first_name, ''), ':', COALESCE(insured_last_name, '')), 256) AS sourcepartyid
FROM {source_database}.pmf_daily_{cycledate} pc
LEFT JOIN LKP_CNTRT_ADMN_LOC_CD ON CNTRT_SRC_SYS_NM_SRC = 'Bestow'
WHERE cycle_date = {cycledate}
  AND (insured_first_name IS NOT NULL
       OR insured_last_name IS NOT NULL)
  AND policy_status not in ('APPROVED_SCHEDULED','APPROVED_STALE')
