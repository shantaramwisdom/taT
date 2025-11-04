SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} domain,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'policy_bind_date' measure_field,
       'DATE' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       {source_database}.pmf_daily_{cycledate} measure_table,
       coalesce(sum(int(date_format(case when policy_status='APPROVED_PENDING' then CAST(next_billing_date AS DATE) else CAST(policy_bind_date AS DATE) END,'yyyyMMdd'))),0) measure_value
FROM {source_database}.pmf_daily_{cycledate}
UNION ALL
SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} domain,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'policy_bind_date' measure_field,
       'DATE' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       {source_database}.pmf_daily_{cycledate} measure_table,
       coalesce(sum(int(date_format(case when policy_status='APPROVED_PENDING' then CAST(next_billing_date AS DATE) else CAST(policy_bind_date AS DATE) END,'yyyyMMdd'))),0) measure_value
FROM {source_database}.pmf_daily_{cycledate}
WHERE cycle_date != {cycledate}
OR policy_status in ('APPROVED_SCHEDULED','APPROVED_STALE')
