WITH source_data AS (
  SELECT 
    'Financial' AS data_type,
    TRIM(cirec802_po1_le) || TRIM(cirec802_po1_lg) AS secondary_ledger_code,
    'TAI ASIA' AS source_system_nm,
    TRIM(cirec802_po1_file_date) AS transaction_date,
    TRIM(cirec802_po1_ref_number) AS contractnumber,
    CAST('{cycle_date}' AS DATE) AS cycle_date
  FROM {source_database}.accounting_detail1_current
  WHERE cycle_date = '{cycledate}'
),
source AS (
  SELECT COUNT(*) AS src_cnt
  FROM source_data
),
source_adj AS (
  SELECT COUNT(*) AS src_adj_cnt
  FROM source_data a
  LEFT JOIN lkp_actvtv_gl_sbldgr_nm b 
    ON '~' = b.ACTVTY_GL_APP_AREA_CD
   AND '~' = b.ACTVTY_GL_SRC_CD
   AND 'TAI ASIA' = b.src_sys_nm_desc
  LEFT JOIN lkp_actvtv_ldgr_nm c 
    ON SUBSTR(secondary_ledger_code, 3, 2) = c.actvtv_gl_ldgr_cd
   AND secondary_ledger_code = c.sec_ldgr_cd
  WHERE c.include_exclude = 'Exclude'
),
target_data AS (
  SELECT 
    python_date_format_checker(transaction_date, '%m/%d/%Y') AS transaction_date_chk,
    secondary_ledger_code,
    transaction_date,
    cycle_date,
    contractnumber,
    data_type,
    SUBSTR(secondary_ledger_code, 3, 2) AS secondary_ledger_code_drvd,
    b.oracle_fah_ldgr_nm AS ledger_name_drvd,
    b.src_sys_nm_desc AS source_system_nm_drvd,
    b.evnt_typ_cd AS event_type_code_drvd,
    b.oracle_fah_sublgdr_nm AS subledger_short_name_drvd
  FROM source_data a
  LEFT JOIN lkp_actvtv_gl_sblgdr_nm b 
    ON '~' = b.ACTVTY_GL_APP_AREA_CD
   AND '~' = b.ACTVTY_GL_SRC_CD
   AND 'TAI ASIA' = b.src_sys_nm_desc
  LEFT JOIN lkp_actvtv_ldgr_nm c 
    ON SUBSTR(secondary_ledger_code, 3, 2) = c.actvtv_gl_ldgr_cd
   AND secondary_ledger_code = c.sec_ldgr_cd
  WHERE NVL(c.include_exclude, 'NULLVAL') IN ('Include', 'NULLVAL')
),
target_force_ignore AS (
  SELECT DISTINCT cycle_date
  FROM target_data a
  WHERE transaction_date_chk = 'faLse'
),
target_ignore AS (
  SELECT COUNT(*) AS target_ign_count
  FROM (
    SELECT 
      transaction_date,
      data_type,
      contractnumber,
      secondary_ledger_code_drvd,
      ledger_name_drvd,
      source_system_nm_drvd,
      event_type_code_drvd,
      subledger_short_name_drvd
    FROM (
      SELECT 
        a.*,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_date,
                       data_type,
                       contractnumber,
                       secondary_ledger_code_drvd,
                       ledger_name_drvd,
                       source_system_nm_drvd,
                       event_type_code_drvd,
                       subledger_short_name_drvd
          ORDER BY NULL
        ) AS fltr
      FROM target_data a
      LEFT ANTI JOIN target_force_ignore b USING (cycle_date)
      WHERE transaction_date_chk = 'true'
        AND ledger_name_drvd IS NOT NULL
        AND source_system_nm_drvd IS NOT NULL
        AND event_type_code_drvd IS NOT NULL
        AND subledger_short_name_drvd IS NOT NULL
        AND secondary_ledger_code NOT LIKE '%x%'
        AND LEN(secondary_ledger_code) >= 4
    ) 
    WHERE fltr > 1
  )
),
new_errors AS (
  SELECT SUM(err_cnt) AS err_cnt
  FROM (
    SELECT COUNT(*) AS err_cnt
    FROM target_data a
    WHERE transaction_date_chk = 'faLse'
       OR ledger_name_drvd IS NULL
       OR source_system_nm_drvd IS NULL
       OR event_type_code_drvd IS NULL
       OR subledger_short_name_drvd IS NULL
       OR secondary_ledger_code LIKE '%x%'
       OR LEN(secondary_ledger_code) < 4
    UNION ALL
    SELECT COUNT(*) AS err_cnt
    FROM target_data a
    LEFT SEMI JOIN target_force_ignore b USING (cycle_date)
    WHERE transaction_date_chk = 'true'
      AND ledger_name_drvd IS NOT NULL
      AND source_system_nm_drvd IS NOT NULL
      AND event_type_code_drvd IS NOT NULL
      AND subledger_short_name_drvd IS NOT NULL
      AND secondary_ledger_code NOT LIKE '%x%'
      AND LEN(secondary_ledger_code) >= 4
  )
),
errors_cleared AS (
  SELECT COUNT(*) AS clr_cnt
  FROM {curated_database}.currentbatch b
  JOIN {curated_database}.{curated_table_name} a USING (cycle_date, batch_id)
  WHERE b.source_system = '{source_system_name}'
    AND b.domain_name = '{domain_name}'
    AND a.original_cycle_date IS NOT NULL
    AND a.original_batch_id IS NOT NULL
    AND (
      a.original_cycle_date != b.cycle_date
      OR a.original_batch_id != b.batch_id
    )
    AND b.batch_frequency = '{batch_frequency}'
    AND b.cycle_date = '{cycle_date}'
    AND b.batch_id = {batchid}
)
SELECT 
  {batchid} AS batch_id,
  '{cycle_date}' AS cycle_date,
  '{domain_name}' AS domain,
  '{source_system_name}' AS sys_nm,
  '{source_system_name}' AS p_sys_nm,
  'BALANCING_COUNT' AS measure_name,
  'curated' AS hop_name,
  'recordcount' AS measure_field,
  'INTEGER' AS measure_value_datatype,
  'S' AS measure_src_tgt_adj_indicator,
  '{source_database}.accounting_detail_current' AS measure_table,
  src_cnt AS measure_value
FROM source

UNION ALL

SELECT 
  {batchid} AS batch_id,
  '{cycle_date}' AS cycle_date,
  '{domain_name}' AS domain,
  '{source_system_name}' AS sys_nm,
  '{source_system_name}' AS p_sys_nm,
  'BALANCING_COUNT' AS measure_name,
  'curated' AS hop_name,
  'recordcount' AS measure_field,
  'INTEGER' AS measure_value_datatype,
  'SI' AS measure_src_tgt_adj_indicator,
  '{source_database}.accounting_detail_current' AS measure_table,
  err_cnt AS measure_value
FROM new_errors

UNION ALL

SELECT 
  {batchid} AS batch_id,
  '{cycle_date}' AS cycle_date,
  '{domain_name}' AS domain,
  '{source_system_name}' AS sys_nm,
  '{source_system_name}' AS p_sys_nm,
  'BALANCING_COUNT' AS measure_name,
  'curated' AS hop_name,
  'recordcount' AS measure_field,
  'INTEGER' AS measure_value_datatype,
  'A' AS measure_src_tgt_adj_indicator,
  '{source_database}.accounting_detail_current' AS measure_table,
  src_adj_cnt AS measure_value
FROM source_adj

UNION ALL

SELECT 
  {batchid} AS batch_id,
  '{cycle_date}' AS cycle_date,
  '{domain_name}' AS domain,
  '{source_system_name}' AS sys_nm,
  '{source_system_name}' AS p_sys_nm,
  'BALANCING_COUNT' AS measure_name,
  'curated' AS hop_name,
  'recordcount' AS measure_field,
  'INTEGER' AS measure_value_datatype,
  'TA' AS measure_src_tgt_adj_indicator,
  '{source_database}.accounting_detail_current' AS measure_table,
  clr_cnt AS measure_value
FROM errors_cleared

UNION ALL

SELECT 
  {batchid} AS batch_id,
  '{cycle_date}' AS cycle_date,
  '{domain_name}' AS domain,
  '{source_system_name}' AS sys_nm,
  '{source_system_name}' AS p_sys_nm,
  'BALANCING_COUNT' AS measure_name,
  'curated' AS hop_name,
  'recordcount' AS measure_field,
  'INTEGER' AS measure_value_datatype,
  'TI' AS measure_src_tgt_adj_indicator,
  '{source_database}.accounting_detail_current' AS measure_table,
  target_ign_count AS measure_value
FROM target_ignore;
