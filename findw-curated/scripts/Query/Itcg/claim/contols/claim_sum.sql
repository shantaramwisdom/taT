SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} DOMAIN,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'claimcurrentadldependencycount' measure_field,
       'INTEGER' measure_value_datatype,
       'S' measure_src_tgt_adj_indicator,
       {source_database}.claim_eligibility_effective_history_daily_{cycledate} measure_table,
       sum(cast((CASE
                     WHEN claim_eligibility.EB_ADL_AMBULATION_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_BATHING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_DRESSING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_FEEDING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_INCONTINENCE_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_TOILETING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_TRANSFERRING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) as int)) AS measure_value
FROM {source_database}.Claim_Eligibility_effective_history_daily_{cycledate} claim_eligibility
JOIN {source_database}.Policy_effective_history_daily_{cycledate} policy ON claim_eligibility.POLICY_ID = policy.POLICY_ID
left anti join ltcg_expired_contracts expired on policy.policy_no = expired.policy_no
UNION ALL
SELECT {batchid} batch_id,
       {cycle_date} cycle_date,
       {domain_name} DOMAIN,
       {source_system_name} sys_nm,
       {source_system_name} p_sys_nm,
       'CONTROL_TOTAL' measure_name,
       'curated' hop_name,
       'claimcurrentadldependencycount' measure_field,
       'INTEGER' measure_value_datatype,
       'A' measure_src_tgt_adj_indicator,
       {source_database}.claim_eligibility_effective_history_daily_{cycledate} measure_table,
       sum(cast((CASE
                     WHEN claim_eligibility.EB_ADL_AMBULATION_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_BATHING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_DRESSING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_FEEDING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_INCONTINENCE_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_TOILETING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) + (CASE
                     WHEN claim_eligibility.EB_ADL_TRANSFERRING_CD in ('1','2') THEN 1
                     ELSE 0
                 END) as int)) AS measure_value
FROM {source_database}.Claim_Eligibility_effective_history_daily_{cycledate} claim_eligibility
JOIN {source_database}.Policy_effective_history_daily_{cycledate} policy ON claim_eligibility.POLICY_ID = policy.POLICY_ID
left anti join ltcg_expired_contracts expired on policy.policy_no = expired.policy_no
WHERE cast(Claim_Eligibility.VIEW_ROW_CREATE_DAY as date) > '{cycle_date}'
      OR '{cycle_date}' >= cast(Claim_Eligibility.VIEW_ROW_OBSOLETE_DAY as date)
      OR cast(Claim_Eligibility.VIEW_ROW_EFFECTIVE_DAY as date) > '{cycle_date}'
      OR '{cycle_date}' >= cast(Claim_Eligibility.VIEW_ROW_EXPIRATION_DAY as date)
      OR cast(policy.VIEW_ROW_CREATE_DAY as date) > '{cycle_date}'
      OR '{cycle_date}' >= cast(policy.VIEW_ROW_OBSOLETE_DAY as date)
      OR cast(policy.VIEW_ROW_EFFECTIVE_DAY as date) > '{cycle_date}'
      OR '{cycle_date}' >= cast(policy.VIEW_ROW_EXPIRATION_DAY as date)
      OR (policy.POLICY_STATUS_CD not in ('104','105')
          AND (policy.POLICY_STATUS_CD = '106'
               OR policy.ORIGINAL_EFFECTIVE_DT < '2021-01-01'))
