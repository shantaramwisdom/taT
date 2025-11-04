SELECT 'Bestow' AS sourcesystemname,
concat(COALESCE(trn.transaction_guid, ''), '0', COALESCE(trn.reversal_indicator, ''), COALESCE(trn.debit_credit_indicator, '')) AS source_activity_id,
trn.transaction_amount AS activityamount,
from_utc_timestamp(trn.transaction_event_at, 'US/Central') AS activityreportdate,
COALESCE(trn.premium_effective_date, from_utc_timestamp(trn.transaction_event_at, 'US/Central')) AS activityeffectivedate,
trn.transaction_guid AS activitysourceactivityparentid,
NULL AS activitysourcetransactivityid,
trn.payout_id AS activitysourcedepositidentifier,
pmf.issuing_company,
trn.activity_type AS trn_activity_type,
trn.transaction_code AS activitysourcetransactioncode,
trn.reason AS reason_code,
trn.payment_method,
trn.policy_number AS contractnumber,
CASE
WHEN trn.activity_type = 'p'
AND trn.premium_year = '1' THEN 'F'
WHEN trn.activity_type = 'p'
AND trn.premium_year = '2' THEN 'R'
ELSE NULL
END AS activityfirstyearrenewalindicator,
CASE
WHEN trn.transaction_code = 'PAYMENT_FAILED'
AND lower(trn.reversal_indicator) = 'false' THEN 'R'
WHEN lower(trn.reversal_indicator) = 'true' THEN 'R'
ELSE 'O'
END AS activityreversalcode,
NULL AS fundsourcefundidentifier,
NULL AS fundclassindicator,
NULL AS activitysourceaccountingdeviator
FROM {source_database}.transactions_daily_{cycledate} trn
LEFT JOIN (
select
policy_number, policy_status, issuing_company
from {source_database}.pmf_daily_{cycledate}
where policy_status not in ('APPROVED_SCHEDULED','APPROVED_STALE')
) pmf ON trn.policy_number = pmf.policy_number
WHERE trn.debit_credit_indicator = 'D'
and trn.cycle_date = {cycledate}