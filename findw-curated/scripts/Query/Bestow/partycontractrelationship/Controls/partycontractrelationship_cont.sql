SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} domain,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       '{source_database}.pmf_daily_{cycledate}' measure_table,
       count(*) measure_value
FROM {source_database}.pmf_daily_{cycledate} pc
UNION ALL
SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} domain,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'BALANCING_COUNT' measure_name,
       'curated' hop_name,
       'recordcount' measure_field,
       'INTEGER' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       '{source_database}.pmf_daily_{cycledate}' measure_table,
       count(*) measure_value
FROM {source_database}.pmf_daily_{cycledate} pc
WHERE cycle_date != {cycledate}
  OR (insured_first_name IS NULL AND insured_last_name IS NULL)
  OR policy_status in ('APPROVED_SCHEDULED','APPROVED_STALE')