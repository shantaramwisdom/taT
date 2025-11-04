WITH input AS (
    SELECT
        *,
        from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS original_recorded_timestamp,
        CAST('{cycle_date}' AS DATE) AS original_cycle_date,
        CAST({batchid} AS INT) AS original_batch_id,
        CAST(NULL AS STRING) AS old_error_message,
        CAST(NULL AS INT) AS error_record_aging_days
    FROM (
        SELECT
            trim(fsi_gl_pt_effective_date)      AS transaction_date,
            trim(fsi_gl_bh_rev_effective_date)  AS gl_reversal_date,
            trim(fsi_gl_bh_application_area)    AS gl_application_area_code,
            trim(fsi_gl_pt_source_code)         AS gl_source_code,
            trim(fsi_gl_pt_company)             AS secondary_ledger_code,
            trim(fsi_gl_pt_source_code)         AS gl_full_source_code,
            trim(fsi_gl_pt_center)              AS orig_gl_center,
            CASE
                WHEN trim(fsi_gl_pt_account) LIKE '9%'
                     AND NOT trim(fsi_gl_pt_account) LIKE '99%' THEN 'Statistical'
                ELSE 'Financial'
            END AS data_type
        FROM {source_database}.fctransstoupload_current
        WHERE cycle_date = CAST('{cycle_date}' AS DATE)

        UNION

        SELECT
            trim(fsi_gl_pt_effective_date)      AS transaction_date,
            trim(fsi_gl_bh_rev_effective_date)  AS gl_reversal_date,
            trim(fsi_gl_bh_application_area)    AS gl_application_area_code,
            trim(fsi_gl_pt_source_code)         AS gl_source_code,
            trim(fsi_gl_pt_company)             AS secondary_ledger_code,
            trim(fsi_gl_pt_source_code)         AS gl_full_source_code,
            trim(fsi_gl_pt_center)              AS orig_gl_center,
            CASE
                WHEN trim(fsi_gl_pt_account) LIKE '9%'
                     AND NOT trim(fsi_gl_pt_account) LIKE '99%' THEN 'Statistical'
                ELSE 'Financial'
            END AS data_type
        FROM {source_database}.fctransmainframe_current
        WHERE cycle_date = CAST('{cycle_date}' AS DATE)

        UNION

        SELECT
            trim(fsi_gl_pt_effective_date)      AS transaction_date,
            trim(fsi_gl_bh_rev_effective_date)  AS gl_reversal_date,
            trim(fsi_gl_bh_application_area)    AS gl_application_area_code,
            trim(fsi_gl_pt_source_code)         AS gl_source_code,
            trim(fsi_gl_pt_company)             AS secondary_ledger_code,
            trim(fsi_gl_pt_source_code)         AS gl_full_source_code,
            trim(fsi_gl_pt_center)              AS orig_gl_center,
            CASE
                WHEN trim(fsi_gl_pt_account) LIKE '9%'
                     AND NOT trim(fsi_gl_pt_account) LIKE '99%' THEN 'Statistical'
                ELSE 'Financial'
            END AS data_type
        FROM {source_database}.fctransdistributed_current
        WHERE cycle_date = CAST('{cycle_date}' AS DATE)
    )
    UNION ALL
    SELECT
        transaction_date,
        gl_reversal_date,
        gl_application_area_code,
        gl_source_code,
        secondary_ledger_code,
        gl_full_source_code,
        orig_gl_center,
        data_type,
        original_recorded_timestamp,
        original_cycle_date,
        original_batch_id,
        error_message      AS old_error_message,
        error_record_aging_days
    FROM error_header
),

master_temp AS (
    SELECT
        transaction_date,
        gl_reversal_date,
        gl_application_area_code,
        gl_source_code,
        secondary_ledger_code,
        gl_full_source_code,
        orig_gl_center,
        data_type,

        CASE
            WHEN gl_source_code IS NULL
                 OR length(trim(substr(gl_source_code, 1, 3))) < 3
                 OR substr(gl_source_code, 1, 3) LIKE '%x%' THEN NULL
            ELSE substr(gl_source_code, 1, 3)
        END AS gl_source_code_drvd,

        CASE
            WHEN secondary_ledger_code IS NULL
                 OR length(trim(substr(secondary_ledger_code, 3, 2))) < 2 THEN NULL
            ELSE substr(secondary_ledger_code, 3, 2)
        END AS secondary_ledger_code_drvd,

        c.oracle_fah_ldgr_nm AS ledger_name_drvd,
        b.src_sys_nm_desc    AS source_system_nm_drvd,
        b.evnt_typ_cd        AS event_type_code_drvd,
        b.oracle_fah_sblgdr_nm AS subledger_short_name_drvd,

        python_date_format_checker(transaction_date, 'YYYY')           AS transaction_date_chk,
        python_date_format_checker(gl_reversal_date, 'YYYY', TRUE)     AS gl_reversal_date_chk,

        CASE WHEN transaction_date_chk = 'true' THEN to_date(transaction_date, 'YYYYDDD') END AS transaction_date_drvd,
        CASE WHEN gl_reversal_date_chk = 'true' THEN to_date(gl_reversal_date, 'YYYYDDD') END AS gl_reversal_date_drvd,

        CASE
            WHEN substr(gl_full_source_code, 1, 3) IN ('903','851','444')
                THEN nvl(d.reinsurance_assumed_ceded_flag, '@')
        END AS reinsuranceassumedcededflag_drvd,

        /* decode(@->1 else 0) -> CASE */
        CASE WHEN reinsuranceassumedcededflag_drvd = '@' THEN 1 ELSE 0 END AS reinsuranceassumedcededflag_error_flag,

        CAST(original_recorded_timestamp AS TIMESTAMP) AS original_recorded_timestamp,
        CAST(original_cycle_date AS DATE) AS original_cycle_date,
        CAST(original_batch_id AS INT)    AS original_batch_id,

        b.include_exclude AS sblgdr_include_exclude,
        c.include_exclude AS ldgr_include_exclude,

        old_error_message,
        error_record_aging_days
    FROM input a
    LEFT JOIN lkp_actvty_gl_sblgdr_nm b
      ON a.gl_application_area_code = b.ACTVTY_GL_APP_AREA_CD
     AND substr(gl_source_code, 1, 3) = b.ACTVTY_GL_SRC_CD
    LEFT JOIN lkp_actvty_gl_ldgr_nm c
      ON substr(secondary_ledger_code, 3, 2) = c.actvty_gl_ldgr_cd
     AND secondary_ledger_code = c.sec_ldgr_cd
    LEFT JOIN lkp_reinsuranceattributes d
      ON substr(a.secondary_ledger_code, 1, 2) = d.legal_entity
     AND a.orig_gl_center = d.geac_centers
    WHERE nvl(b.include_exclude, 'NULLVAL') IN ('Include','NULLVAL')
      AND nvl(c.include_exclude, 'NULLVAL') IN ('Include','NULLVAL')
),

master AS (
    SELECT
        a.*,
        MAX(reinsuranceassumedcededflag_error_flag) OVER (
            PARTITION BY ledger_name_drvd,
                         source_system_nm_drvd,
                         event_type_code_drvd,
                         subledger_short_name_drvd,
                         gl_source_code_drvd,
                         gl_application_area_code,
                         transaction_date_drvd,
                         data_type,
                         gl_reversal_date_drvd,
                         secondary_ledger_code_drvd,
                         original_cycle_date,
                         original_batch_id
        ) AS max_reinsuranceassumedcededflag_error_flag
    FROM master_temp a
),

keys AS (
    SELECT
        *,
        substr(event_type_code_drvd, 1, 6)
        || date_format(from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central'), 'yyyyMMddHHmmssSSS')
        || '_' || row_number() OVER (PARTITION BY subledger_short_name_drvd ORDER BY NULL) AS transaction_number_drvd
    FROM (
        SELECT DISTINCT
            ledger_name_drvd,
            source_system_nm_drvd,
            event_type_code_drvd,
            subledger_short_name_drvd,
            gl_source_code_drvd,
            gl_application_area_code,
            transaction_date_drvd,
            data_type,
            gl_reversal_date_drvd,
            secondary_ledger_code_drvd,
            original_cycle_date,
            original_batch_id
        FROM master
        WHERE ledger_name_drvd IS NOT NULL
          AND event_type_code_drvd IS NOT NULL
          AND source_system_nm_drvd IS NOT NULL
          AND subledger_short_name_drvd IS NOT NULL
          AND transaction_date_chk = 'true'
          AND gl_reversal_date_chk = 'true'
          AND substr(gl_source_code, 1, 3) NOT LIKE '% %'
          AND substr(secondary_ledger_code, 3, 2) NOT LIKE '% %'
          AND length(gl_application_area_code) > 0
          AND length(gl_source_code) >= 3
          AND length(secondary_ledger_code) >= 4
          AND max_reinsuranceassumedcededflag_error_flag = 0
          AND (
                (CAST(nvl(gl_reversal_date, 0) AS INT) > 0
                 AND CAST(nvl(transaction_date, 0) AS INT) <= CAST(nvl(gl_reversal_date, 0) AS INT))
               OR CAST(nvl(gl_reversal_date, 0) AS INT) = 0
          )
    )
),

final AS (
    SELECT
        a.*,
        b.transaction_number_drvd,

        /* Date error details */
        trim(
            CASE
                WHEN transaction_date_chk = 'false'
                  THEN nvl(transaction_date, '') || ' transaction_date is in invalid format, expected YYYYJJJ;'
                ELSE '' END
            ||
            CASE
                WHEN gl_reversal_date_chk = 'false'
                  THEN nvl(gl_reversal_date, '') || ' gl_reversal_date is in invalid format, expected YYYYJJJ;'
                ELSE '' END
            ||
            CASE
                WHEN CAST(nvl(gl_reversal_date, 0) AS INT) > 0
                 AND CAST(nvl(transaction_date, 0) AS INT) > CAST(nvl(gl_reversal_date, 0) AS INT)
                  THEN coalesce(CAST(a.transaction_date_drvd AS STRING), a.transaction_date, 'NULL')
                       || ' > ' ||
                       coalesce(CAST(a.gl_reversal_date_drvd AS STRING), gl_reversal_date, 'NULL')
                       || ' transaction_date is greater than gl_reversal_date;'
                ELSE '' END
        ) AS date_error_message_,

        /* Hard error build */
        trim(
            nvl(date_error_message_, '')
            ||
            CASE
                WHEN a.gl_application_area_code IS NULL OR length(a.gl_application_area_code) = 0
                  THEN nvl(a.gl_application_area_code, 'BLANK/NULL') || ' gl_application_area_code is Invalid from Source;'
                ELSE '' END
            ||
            CASE
                WHEN a.gl_source_code IS NULL
                  OR substr(a.gl_source_code, 1, 3) LIKE '% %'
                  OR length(a.gl_source_code) < 3
                  THEN nvl(a.gl_source_code, 'BLANK/NULL') || ' gl_source_code is Invalid from Source;'
                ELSE '' END
            ||
            CASE
                WHEN a.secondary_ledger_code IS NULL
                  OR substr(a.secondary_ledger_code, 3, 2) LIKE '% %'
                  OR length(a.secondary_ledger_code) < 4
                  THEN nvl(a.secondary_ledger_code, 'BLANK/NULL') || ' secondary_ledger_code is Invalid from Source;'
                ELSE '' END
        ) AS hard_error_message_,

        /* Soft error build (only if no hard errors) */
        trim(
            CASE
                WHEN length(hard_error_message_) = 0 THEN
                    (CASE
                        WHEN a.source_system_nm_drvd IS NULL OR a.event_type_code_drvd IS NULL THEN
                            'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                            || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                        ELSE '' END)
                    ||
                    (CASE
                        WHEN a.ledger_name_drvd IS NULL THEN
                            'ledger_name is missing in RDM table lkp_actvty_gl_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd ('
                            || nvl(substr(secondary_ledger_code, 3, 2), '') || ') and secondary_ledger_code (' || nvl(secondary_ledger_code, '') || ');'
                        ELSE '' END)
                    ||
                    (CASE
                        WHEN a.subledger_short_name_drvd IS NULL THEN
                            'subledger_short_name is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                            || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                        ELSE '' END)
                    ||
                    (CASE
                        WHEN b.transaction_number_drvd IS NULL THEN 'transaction_number is invalid;'
                        ELSE '' END)
                    ||
                    (CASE
                        WHEN a.reinsuranceassumedcededflag_drvd = '@' THEN
                            'reinsuranceassumedcededflag is missing in RDM table lkp_reinsurance_attributes for key combination secondary_ledger_code ('
                            || nvl(substr(a.secondary_ledger_code, 1, 2), '') || ') and orig_gl_center (' || nvl(orig_gl_center, '') || ');'
                        ELSE '' END)
                ELSE '' END
        ) AS soft_error_message_,

        CASE
            WHEN length(hard_error_message_) > 0 THEN 'N'
            WHEN length(soft_error_message_) > 0 THEN 'Y'
        END AS reprocess_flag,

        trim(concat(nvl(hard_error_message_, ''), nvl(soft_error_message_, ''))) AS error_message_,

        sblgdr_include_exclude,
        ldgr_include_exclude
    FROM master a
    LEFT JOIN keys b
      ON a.source_system_nm_drvd = b.source_system_nm_drvd
     AND a.event_type_code_drvd = b.event_type_code_drvd
     AND a.ledger_name_drvd     = b.ledger_name_drvd
     AND a.subledger_short_name_drvd = b.subledger_short_name_drvd
     AND a.transaction_date_drvd = b.transaction_date_drvd
     AND a.gl_application_area_code = b.gl_application_area_code
     AND a.gl_source_code_drvd   = b.gl_source_code_drvd
     AND a.secondary_ledger_code_drvd = b.secondary_ledger_code_drvd
     AND a.data_type             = b.data_type
     AND coalesce(a.gl_reversal_date_drvd, '') = coalesce(b.gl_reversal_date_drvd, '')
     AND a.original_cycle_date   = b.original_cycle_date
     AND a.original_batch_id     = b.original_batch_id
),

force_error_valid AS (
    SELECT DISTINCT
        original_cycle_date,
        original_batch_id,
        gl_application_area_code,
        gl_source_code_drvd
    FROM final
    WHERE (gl_application_area_code, gl_source_code_drvd) IN (
        SELECT gl_application_area_code, gl_source_code_drvd
        FROM final
        WHERE length(date_error_message_) > 0
          AND original_cycle_date = CAST('{cycle_date}' AS DATE)
          AND original_batch_id   = {batchid}
        GROUP BY gl_application_area_code, gl_source_code_drvd
    )
      AND length(error_message_) = 0
      AND original_cycle_date = CAST('{cycle_date}' AS DATE)
      AND original_batch_id   = {batchid}
)

SELECT
    transaction_date,
    gl_reversal_date,
    gl_application_area_code,
    gl_source_code,
    secondary_ledger_code,
    data_type,
    gl_source_code_drvd,
    gl_full_source_code,
    orig_gl_center,
    reinsuranceassumedcededflag_drvd,
    secondary_ledger_code_drvd,
    ledger_name_drvd,
    source_system_nm_drvd,
    event_type_code_drvd,
    subledger_short_name_drvd,
    transaction_date_chk,
    gl_reversal_date_chk,
    a.transaction_date_drvd,
    gl_reversal_date_drvd,
    transaction_number_drvd,
    original_recorded_timestamp,
    original_cycle_date,
    original_batch_id,
    old_error_message,
    error_record_aging_days,
    sblgdr_include_exclude,
    ldgr_include_exclude,

    /* Rebuild hard/soft for the SELECT scope; use a.* fields and a.transaction_number_drvd */
    trim(
        CASE
            WHEN length(hard_error_message_) > 0 THEN
                (CASE
                    WHEN a.source_system_nm_drvd IS NULL OR a.event_type_code_drvd IS NULL THEN
                        'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.ledger_name_drvd IS NULL THEN
                        'ledger_name is missing in RDM table lkp_actvty_gl_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd ('
                        || nvl(substr(secondary_ledger_code, 3, 2), '') || ') and secondary_ledger_code (' || nvl(secondary_ledger_code, '') || ');'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.subledger_short_name_drvd IS NULL THEN
                        'subledger_short_name is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.transaction_number_drvd IS NULL THEN 'transaction_number is invalid;'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.reinsuranceassumedcededflag_drvd = '0' THEN
                        'reinsuranceassumedcededflag is missing in RDM table lkp_reinsurance_attributes for key combination secondary_ledger_code ('
                        || nvl(substr(a.secondary_ledger_code, 1, 2), '') || ') and orig_gl_center (' || nvl(orig_gl_center, '') || ');'
                    ELSE '' END)
            ELSE '' END
    ) AS hard_error_message_,

    trim(
        CASE
            WHEN length(hard_error_message_) = 0 THEN
                (CASE
                    WHEN a.source_system_nm_drvd IS NULL OR a.event_type_code_drvd IS NULL THEN
                        'source_system_nm/event_type_code is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.ledger_name_drvd IS NULL THEN
                        'ledger_name is missing in RDM table lkp_actvty_gl_ldgr_nm for key combination actvty_gl_ldgr_cd/secondary_ledger_code_drvd ('
                        || nvl(substr(secondary_ledger_code, 3, 2), '') || ') and secondary_ledger_code (' || nvl(secondary_ledger_code, '') || ');'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.subledger_short_name_drvd IS NULL THEN
                        'subledger_short_name is missing in RDM table lkp_actvty_gl_sblgdr_nm for key combination gl_application_area_code ('
                        || nvl(substr(gl_source_code, 1, 3), '') || ') and gl_source_code (' || nvl(substr(gl_source_code, 1, 3), '') || ');'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.transaction_number_drvd IS NULL THEN 'transaction_number is invalid;'
                    ELSE '' END)
                ||
                (CASE
                    WHEN a.reinsuranceassumedcededflag_drvd = '0' THEN
                        'reinsuranceassumedcededflag is missing in RDM table lkp_reinsurance_attributes for key combination secondary_ledger_code ('
                        || nvl(substr(a.secondary_ledger_code, 1, 2), '') || ') and orig_gl_center (' || nvl(orig_gl_center, '') || ');'
                    ELSE '' END)
            ELSE '' END
    ) AS soft_error_message_,

    CASE
        WHEN length(hard_error_message_) > 0 THEN 'N'
        WHEN length(soft_error_message_) > 0 THEN 'Y'
    END AS reprocess_flag,

    trim(concat(nvl(hard_error_message_, ''), nvl(soft_error_message_, ''))) AS error_message_,

    a.transaction_number_drvd,

    /* Forced error overlay if other headers in same key space had date errors */
    trim(
        CASE
            WHEN b.original_cycle_date IS NOT NULL
             AND length(hard_error_message_) = 0 THEN
                'Forced Error. Date Errors Present in Other Headers for combination of gl_application_area_code ('
                || nvl(a.gl_application_area_code, '') || ') and gl_source_code (' || nvl(a.gl_source_code_drvd, '') || ');'
            ELSE '' END
        || nvl(hard_error_message_, '')
    ) AS hard_error_message_forced,

    soft_error_message_ AS soft_error_message,

    CASE
        WHEN length(hard_error_message_) > 0 THEN 'N'
        WHEN length(soft_error_message_) > 0 THEN 'Y'
    END AS reprocess_flag_forced,

    trim(concat(nvl(hard_error_message_, ''), nvl(soft_error_message_, ''))) AS error_message,

    CASE
        WHEN a.original_cycle_date IS NOT NULL
         AND a.original_batch_id IS NOT NULL
         AND (
             CAST('{cycle_date}' AS DATE) <> CAST(a.original_cycle_date AS DATE)
             OR CAST({batchid} AS INT) <> CAST(a.original_batch_id AS INT)
         )
         AND length(error_message) = 0 THEN 'Good - Reprocessed Error'
        WHEN a.original_cycle_date IS NOT NULL
         AND a.original_batch_id IS NOT NULL
         AND length(error_message) > 0
         AND length(hard_error_message_forced) > 0 THEN 'Hard Error'
        WHEN a.original_cycle_date IS NOT NULL
         AND a.original_batch_id IS NOT NULL
         AND length(error_message) > 0
         AND length(soft_error_message) > 0 THEN 'Soft Error'
        WHEN a.original_cycle_date IS NOT NULL
         AND a.original_batch_id IS NOT NULL
         AND length(error_message) = 0 THEN 'Good - Current Load'
        WHEN a.original_cycle_date IS NOT NULL
         AND a.original_batch_id IS NOT NULL
         AND length(error_message) > 0 THEN 'Unknown Error'
        ELSE 'Unknown'
    END AS error_cleared,

    sblgdr_include_exclude,
    ldgr_include_exclude
FROM final a
LEFT JOIN force_error_valid b USING (
    original_cycle_date,
    original_batch_id,
    gl_application_area_code,
    gl_source_code_drvd
);