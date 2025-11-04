WITH source_data AS (
    SELECT 
        TRIM(CIRCE802_POL_FILE_DATE) AS transaction_date,
        TRIM(circe802_pol_le) || TRIM(circe802_pol_lg) AS secondary_ledger_code,
        'Financial' AS data_type,
        'TAI ASIA' AS source_system_nm,
        TRIM(CIRCE802_POL_REF_NUMBER) AS contractnumber,
        ABS(NVL(CAST(TRIM(CIRCE802_POL_TRANS_AMOUNT) AS DECIMAL(18, 2)), 0.0)) AS default_amount,
        CAST('{cycle_date}' AS DATE) AS cycle_date
    FROM {source_database}.accounting_detail_current
    WHERE cycle_date = {cycleDate}
),

source AS (
    SELECT 
        SUM(default_amount) AS src_default_amount
    FROM source_data
),

source_adj AS (
    SELECT 
        SUM(default_amount) AS src_adj_default_amount
    FROM source_data a
    LEFT JOIN lkp_acctvty_gl_sublggr_nm b 
        ON '~' = b.ACCTVY_GL_APP_AREA_CD
        AND '~' = b.ACCTVY_GL_SRC_CD
        AND 'TAI ASIA' = b.src_sys_nm_desc
    LEFT JOIN lkp_acctvty_gl_ldrgr_nm c 
        ON SUBSTR(secondary_ledger_code, 3, 2) = c.acctvty_gl_ldrgr_cd
        AND secondary_ledger_code = c.sec_ldrgr_cd
    WHERE c.include_exclude = 'Exclude'
),

target_data AS (
    SELECT 
        python_date_format_checker(transaction_date, '%m/%d/%Y') AS transaction_date_chk,
        secondary_ledger_code,
        transaction_date,
        data_type,
        contractnumber,
        cycle_date,
        SUBSTR(secondary_ledger_code, 3, 2) AS secondary_ledger_code_drvd,
        c.oracle_fah_ldrgr_nm AS ledger_name_drvd,
        source_system_nm AS source_system_nm_drvd,
        b.event_type_cd AS event_type_code_drvd,
        b.oracle_fah_sublggr_nm AS subledger_short_name_drvd,
        default_amount,
        monotonically_increasing_id() AS random_counter
    FROM source_data a
    LEFT JOIN lkp_acctvty_gl_sublggr_nm b 
        ON '~' = b.ACCTVY_GL_APP_AREA_CD
        AND '~' = b.ACCTVY_GL_SRC_CD
        AND 'TAI ASIA' = b.src_sys_nm_desc
    LEFT JOIN lkp_acctvty_gl_ldrgr_nm c 
        ON SUBSTR(secondary_ledger_code, 3, 2) = c.acctvty_gl_ldrgr_cd
        AND secondary_ledger_code = c.sec_ldrgr_cd
    WHERE NVL(c.include_exclude, 'NULLVAL') IN ('Include', 'NULLVAL')
),

target_force_ignore AS (
    SELECT DISTINCT cycle_date
    FROM target_data a
    WHERE transaction_date_chk = 'false'
),

new_errors AS (
    SELECT 
        SUM(err_default_amount) AS err_default_amount
    FROM (
        SELECT 
            SUM(default_amount) AS err_default_amount
        FROM target_data a
        WHERE transaction_date_chk = 'false'
            OR ledger_name_drvd IS NULL
            OR source_system_nm_drvd IS NULL
            OR event_type_code_drvd IS NULL
            OR subledger_short_name_drvd IS NULL
            OR secondary_ledger_code LIKE '%x%'
            OR LEN(secondary_ledger_code) < 2
        
        UNION ALL

        SELECT 
            SUM(default_amount) AS err_default_amount
        FROM target_data a
        LEFT SEMI JOIN target_force_ignore b USING (cycle_date)
        WHERE transaction_date_chk = 'true'
            AND ledger_name_drvd IS NOT NULL
            AND source_system_nm_drvd IS NOT NULL
            AND event_type_code_drvd IS NOT NULL
            AND subledger_short_name_drvd IS NOT NULL
            AND SUBSTR(gl_source_code, 1, 3) NOT LIKE '%x%'
            AND secondary_ledger_code NOT LIKE '%x%'
            AND LEN(gl_application_area_code) > 0
            AND LEN(gl_source_code) = 3
            AND LEN(secondary_ledger_code) >= 4
    )
),

errors_cleared AS (
    SELECT 
        SUM(default_amount) AS clr_default_amount
    FROM {curated_database}.currentbatch b
    JOIN {curated_database}.{curated_table_name} a 
        USING (cycle_date, batch_id)
    WHERE b.source_system = '{source_system_name}'
        AND b.domain_nm = '{domain_name}'
        AND b.original_cycle_date IS NOT NULL
        AND b.original_batch_id IS NOT NULL
        AND (
            a.original_cycle_date != b.cycle_date
            OR a.original_batch_id != b.batch_id
        )
        AND b.batch_frequency = '{batch_frequency}'
        AND b.cycle_date = '{cycle_date}'
        AND b.batch_id = {batchid}
)

-- Final Aggregated Output
SELECT 
    {batchid} AS batch_id,
    '{cycle_date}' AS cycle_date,
    '{domain_name}' AS domain,
    '{source_system_name}' AS sys_nm,
    '{source_system_name}' AS p_sys_nm,
    'CONTROL_TOTAL' AS measure_name,
    'curated' AS hop_name,
    'ex_amount_ten || ex_amount_decimal' AS measure_field,
    'INTEGER' AS measure_value_datatype,
    'S' AS measure_src_tgt_adj_indicator,
    '{source_database}.accounting_detail_current' AS measure_table,
    NVL(src_default_amount, 0) AS measure_value
FROM source

UNION ALL

SELECT 
    {batchid} AS batch_id,
    '{cycle_date}' AS cycle_date,
    '{domain_name}' AS domain,
    '{source_system_name}' AS sys_nm,
    '{source_system_name}' AS p_sys_nm,
    'CONTROL_TOTAL' AS measure_name,
    'curated' AS hop_name,
    'ex_amount_ten || ex_amount_decimal' AS measure_field,
    'INTEGER' AS measure_value_datatype,
    'SI' AS measure_src_tgt_adj_indicator,
    '{source_database}.accounting_detail_current' AS measure_table,
    NVL(err_default_amount, 0) AS measure_value
FROM new_errors

UNION ALL

SELECT 
    {batchid} AS batch_id,
    '{cycle_date}' AS cycle_date,
    '{domain_name}' AS domain,
    '{source_system_name}' AS sys_nm,
    '{source_system_name}' AS p_sys_nm,
    'CONTROL_TOTAL' AS measure_name,
    'curated' AS hop_name,
    'ex_amount_ten || ex_amount_decimal' AS measure_field,
    'INTEGER' AS measure_value_datatype,
    'A' AS measure_src_tgt_adj_indicator,
    '{source_database}.accounting_detail_current' AS measure_table,
    NVL(src_adj_default_amount, 0) AS measure_value
FROM source_adj

UNION ALL

SELECT 
    {batchid} AS batch_id,
    '{cycle_date}' AS cycle_date,
    '{domain_name}' AS domain,
    '{source_system_name}' AS sys_nm,
    '{source_system_name}' AS p_sys_nm,
    'CONTROL_TOTAL' AS measure_name,
    'curated' AS hop_name,
    'ex_amount_ten || ex_amount_decimal' AS measure_field,
    'INTEGER' AS measure_value_datatype,
    'TA' AS measure_src_tgt_adj_indicator,
    '{source_database}.accounting_detail_current' AS measure_table,
    NVL(clr_default_amount, 0) AS measure_value
FROM errors_cleared;
