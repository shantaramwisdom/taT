

09+LKP_IFRS17_NB_PRT_CHRT+rdm.sql
10+LKP_HT_MP_IFRS17_PRFTBLY+rdm.sql
11+DIM_LOB+sparksql.sql


SELECT 'Bestow' AS sourcesystemname,
       policy_number AS contractnumber,
       policy_number AS originatingcontractnumber,
       case when policy_status='APPROVED_PENDING' then CAST(next_billing_date AS DATE) else CAST(policy_bind_date AS DATE) END AS contractissuedate,
       issuing_company,
       product_code AS contractplancode,
       product_code AS producttype,
       case when policy_status='APPROVED_PENDING' then CAST(next_billing_date AS DATE) else CAST(policy_bind_date AS DATE) END AS contracteffectivedate,
       case when policy_status='APPROVED_PENDING' then cast((months_between(CAST(status_effective_at AS DATE) , to_date(insured_dob ,'yyyy-MM-dd')))/12) as int)
            else insured_issue_age end AS contractissueage,
       insured_sex AS insuredsex,
       risk_class AS riskclass,
       policy_face_amount AS policyfaceamount,
       policy_status,
       billing_mode,
       get_json_object(agents, '$[0].marketing_organization_cd') as contractsourceorganizationcode,
       'Bestow' AS originatingsourcesystemname,
       CAST('{cycle_date}' AS DATE) AS cycle_date
FROM {source_database}.pmf_daily_{cycledate}
WHERE cycle_date = {cycledate}
AND policy_status not in ('APPROVED_SCHEDULED','APPROVED_STALE')
