SELECT
'{batchid}' batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'CONTROL_TOTAL' measure_name,
'curated' hop_name,
'coverage_unit_id' measure_field,
'INTEGER' measure_value_datatype,
'S' measure_src_tgt_adj_indicator,
'{source_database}.coverage_effective_history_daily_{cycledate}/coverage_TA_effective_history_daily_{cycledate}' measure_table,
sum(cast(Coverage_TA.Coverage_Unit_ID as int)) as measure_value
FROM {source_database}.POLICY_effective_history_daily_{cycledate} POLICY
left anti join ltcg_expired_contracts expired on POLICY.policy_no = expired.policy_no
JOIN {source_database}.Coverage_effective_history_daily_{cycledate} COVERAGE ON POLICY.POLICY_ID = COVERAGE.POLICY_ID
AND POLICY.COVERAGE_ID = COVERAGE.COVERAGE_ID
JOIN {source_database}.Coverage_TA_effective_history_daily_{cycledate} Coverage_TA ON COVERAGE.POLICY_ID = Coverage_TA.POLICY_ID
AND Coverage_TA.COVERAGE_ID = COVERAGE.COVERAGE_ID
AND COVERAGE.COVERAGE_UNIT_ID = Coverage_TA.COVERAGE_UNIT_ID
UNION ALL
SELECT
'{batchid}' batch_id,
'{cycle_date}' cycle_date,
'{domain_name}' domain,
'{source_system_name}' sys_nm,
'{source_system_name}' p_sys_nm,
'CONTROL_TOTAL' measure_name,
'curated' hop_name,
'coverage_unit_id' measure_field,
'INTEGER' measure_value_datatype,
'A' measure_src_tgt_adj_indicator,
'{source_database}.coverage_effective_history_daily_{cycledate}/coverage_TA_effective_history_daily_{cycledate}' measure_table,
sum(cast(Coverage_TA.Coverage_Unit_ID as int)) as measure_value
FROM {source_database}.POLICY_effective_history_daily_{cycledate} POLICY
left anti join ltcg_expired_contracts expired on POLICY.policy_no = expired.policy_no
JOIN {source_database}.Coverage_effective_history_daily_{cycledate} COVERAGE ON POLICY.POLICY_ID = COVERAGE.POLICY_ID
AND POLICY.COVERAGE_ID = COVERAGE.COVERAGE_ID
JOIN {source_database}.Coverage_TA_effective_history_daily_{cycledate} Coverage_TA ON COVERAGE.POLICY_ID = Coverage_TA.POLICY_ID
AND Coverage_TA.COVERAGE_ID = COVERAGE.COVERAGE_ID
AND COVERAGE.COVERAGE_UNIT_ID = Coverage_TA.COVERAGE_UNIT_ID
WHERE cast(POLICY.View_Row_Create_Day as DATE) > '{cycle_date}'
or ('{cycle_date}' > cast(POLICY.VIEW_ROW_OBSOLETE_DAY as date))
or cast(POLICY.VIEW_ROW_EFFECTIVE_DAY as date) > '{cycle_date}'
or ('{cycle_date}' >= cast(POLICY.VIEW_ROW_EXPIRATION_DAY as date)
and (coalesce(POLICY.POLICY_STATUS_CD, '') not in ('104', '105')
or coalesce(POLICY.POLICY_STATUS_CD, '') = '106'
and cast(POLICY.ORIGINAL_EFFECTIVE_DT as date) < '2021-01-01'))
or cast(Coverage.VIEW_ROW_CREATE_DAY as date) > '{cycle_date}'
or ('{cycle_date}' > cast(Coverage.VIEW_ROW_OBSOLETE_DAY as date))
or cast(Coverage_TA.VIEW_ROW_CREATE_DAY as date) > '{cycle_date}'
or ('{cycle_date}' > cast(Coverage_TA.VIEW_ROW_OBSOLETE_DAY as date))
or coalesce(Coverage_TA.EXT_COV_TYPE_CD, '') not in ('173', '174')
