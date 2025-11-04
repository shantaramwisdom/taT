SELECT 'Bestow' AS sourcesystemname,
CAST('{cycle_date}' AS date) AS activityreportdate,
CAST(pmf.policy_bind_date AS DATE) AS activityeffectivedate,
CAST(pmf.policy_bind_date AS DATE) AS contractissuedate,
pmf.issuing_company,
pmf.policy_number AS contractnumber,
'O' AS activityreversalcode,
NULL AS activitysourcedepositidentifier,
NULL AS fundsourcefundidentifier,
NULL AS fundclassindicator,
NULL AS activitysourceaccountingdeviator,
CASE
WHEN add_months(CAST('{cycle_date}' AS date), datediff(YEAR, CAST(pmf.policy_bind_date AS DATE), CAST('{cycle_date}' AS date)) * (-12)) < CAST(pmf.policy_bind_date AS DATE) THEN datediff(YEAR, CAST(pmf.policy_bind_date AS DATE), CAST('{cycle_date}' AS date)) - 1
ELSE datediff(YEAR, CAST(pmf.policy_bind_date AS DATE), CAST('{cycle_date}' AS date))
END AS current_age,
CASE
WHEN (add_months(date_add('{cycle_date}', -1), datediff(YEAR, CAST(pmf.policy_bind_date AS DATE), date_add('{cycle_date}', -1)) * (-12))) < CAST(pmf.policy_bind_date AS DATE) THEN datediff(YEAR, CAST(pmf.policy_bind_date AS DATE), date_add('{cycle_date}', -1)) - 1
ELSE datediff(YEAR, CAST(pmf.policy_bind_date AS DATE), date_add('{cycle_date}', -1))
END AS prior_cycle_date_age,
dateadd(YEAR, CASE
WHEN current_age != prior_cycle_date_age THEN current_age
ELSE prior_cycle_date_age + 1
END, pmf.policy_bind_date) AS anniv,
dateadd(YEAR, -1, anniv) AS prior_anniv,
exploded_rider.rider_benefit_service AS rider_service,
exploded_rider.rider_benefit_service_opt_in AS rider_opt_in
FROM
(SELECT *
FROM
(SELECT case when policy_status='APPROVED_PENDING' then CAST(next_billing_date AS DATE) else CAST(policy_bind_date AS DATE) END AS policy_bind_date,
rider,
issuing_company,
policy_number,
policy_status,
from_json(rider, 'array<struct<rider_benefit_service:string, rider_benefit_service_product_code:string,
rider_benefit_service_product_description:string,rider_benefit_service_opt_in:boolean >>') AS rider_array
FROM {source_database}.pmf_daily_{cycledate}
WHERE policy_status not in ( 'APPROVED_SCHEDULED', 'APPROVED_STALE')
AND get_json_object(rider, '$[*].rider_benefit_service') like '%Everest%'
AND cycle_date = {cycledate}) LATERAL VIEW EXPLODE(rider_array) AS exploded_rider
WHERE coalesce(exploded_rider.rider_benefit_service, '') = 'Everest'
AND coalesce(exploded_rider.rider_benefit_service_opt_in, false) = true) pmf
