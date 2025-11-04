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
       {source_database}.pmf_daily_{cycledate} measure_table,
       Count(*) measure_value
FROM {source_database}.pmf_daily_{cycledate}
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
       {source_database}.pmf_daily_{cycledate} measure_table,
       Count(*) measure_value
FROM {source_database}.pmf_daily_{cycledate}
WHERE cycle_date != {cycledate}
OR policy_status in ('APPROVED_SCHEDULED','APPROVED_STALE')
