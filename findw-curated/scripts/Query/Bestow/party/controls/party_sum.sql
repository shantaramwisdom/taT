SELECT {batchid}          batch_id,
       '{cycle_date}'     cycle_date,
       '{domain_name}'    domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'CONTROL_TOTAL'    measure_name,
       'curated'          hop_name,
       'dateofbirth'      measure_field,
       'DATE'             measure_value_datatype,
       'S'                measure_src_tgt_adj_indicator,
       '{source_database}.pmf_daily_{cycledate}' measure_table,
       coalesce(sum(int(date_format(cast(insured_dob AS date),'yyyymmdd'))), 0) measure_value
FROM {source_database}.pmf_daily_{cycledate} pc
UNION ALL
SELECT {batchid}          batch_id,
       '{cycle_date}'     cycle_date,
       '{domain_name}'    domain,
       '{source_system_name}' sys_nm,
       '{source_system_name}' p_sys_nm,
       'CONTROL_TOTAL'    measure_name,
       'curated'          hop_name,
       'dateofbirth'      measure_field,
       'DATE'             measure_value_datatype,
       'A'                measure_src_tgt_adj_indicator,
       '{source_database}.pmf_daily_{cycledate}' measure_table,
       coalesce(sum(int(date_format(cast(insured_dob AS date),'yyyymmdd'))), 0) measure_value
FROM {source_database}.pmf_daily_{cycledate} pc
WHERE cycle_date != {cycledate}
   OR (insured_first_name IS NULL and insured_last_name IS NULL)
   OR policy_status in ('APPROVED_SCHEDULED','APPROVED_STALE')
