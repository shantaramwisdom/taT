SELECT
'{batchid}' as batchid,
POLICY.POLICY_NO AS ContractNumber,
Coverage.COVERAGE_STATUS_CD AS COVERAGE_STATUS_CD,
Coverage.COVERAGE_STATUS_REASON_CD AS COVERAGE_STATUS_REASON_CD,
Coverage_TA.COVERAGE_UNIT_ID AS ContractoptionCoverageUnitID,
Coverage_TA.ADMIN_SYSTEM_PLAN_CD AS ContractoptionPlanCode,
'{cycle_date}' as cycle_date,
Coverage_TA.ContractoptionSourceSystemOrdinalPosition AS ContractoptionSourceSystem=OrdinalPosition
FROM
(
    SELECT
    *
    FROM
    {source_database}.POLICY_effective_history_daily_{cycledate} POLICY
    WHERE
    CAST(POLICY.VIEW_ROW_CREATE_DAY AS DATE) <= '{cycle_date}'
    AND '{cycle_date}' <= CAST(POLICY.VIEW_ROW_OBSOLETE_DAY AS date)
    AND CAST(POLICY.VIEW_ROW_EFFECTIVE_DAY AS date) <= '{cycle_date}'
    AND '{cycle_date}' < CAST(POLICY.VIEW_ROW_EXPIRATION_DAY AS date)
    AND (POLICY.POLICY_STATUS_CD IN ('104', '105')
    OR (POLICY.POLICY_STATUS_CD = '106'
    AND CAST(POLICY.ORIGINAL_EFFECTIVE_DT AS date) >= '2021-01-01'))) POLICY
INNER JOIN (
    SELECT
    *
    FROM
    {source_database}.Coverage_effective_history_daily_{cycledate} COVERAGE
    WHERE
    CAST(Coverage.VIEW_ROW_CREATE_DAY AS date) <= '{cycle_date}'
    AND '{cycle_date}' <= CAST(Coverage.VIEW_ROW_OBSOLETE_DAY AS date)
) COVERAGE ON
POLICY.POLICY_ID = COVERAGE.POLICY_ID
AND POLICY.COVERAGE_ID = COVERAGE.COVERAGE_ID
INNER JOIN (
    SELECT
    C.*,
    CASE
        WHEN C.Coverage_Unit_ID = c1.min_Coverage_Unit_ID THEN '1'
        ELSE C.Coverage_Unit_ID
    END AS ContractoptionSourceSystemOrdinalPosition
    FROM
    (
        SELECT
        *
        FROM
        {source_database}.Coverage_TA_effective_history_daily_{cycledate} Coverage_TA
        WHERE
        CAST(Coverage_TA.VIEW_ROW_CREATE_DAY AS date) <= '{cycle_date}'
        AND '{cycle_date}' <= CAST(Coverage_TA.VIEW_ROW_OBSOLETE_DAY AS date)
        AND Coverage_TA.EXT_COV_TYPE_CD IN ('173', '174')
    ) C
    LEFT JOIN (
        SELECT
        policy_id,
        COVERAGE_ID,
        min(Coverage_Unit_ID) AS min_Coverage_Unit_ID
        FROM
        {source_database}.Coverage_TA_effective_history_daily_{cycledate}
        WHERE
        CAST(VIEW_ROW_CREATE_DAY AS date) <= '{cycle_date}'
        AND '{cycle_date}' <= CAST(VIEW_ROW_OBSOLETE_DAY AS date)
        AND EXT_COV_TYPE_CD = '173'
        GROUP BY
        policy_id,
        COVERAGE_ID
    ) C1 ON
    C.policy_id = C1.policy_id
    AND C.COVERAGE_ID = C1.COVERAGE_ID
) Coverage_TA ON
COVERAGE.POLICY_ID = Coverage_TA.POLICY_ID
AND COVERAGE.COVERAGE_ID = Coverage_TA.COVERAGE_ID
AND COVERAGE.COVERAGE_UNIT_ID = Coverage_TA.COVERAGE_UNIT_ID
