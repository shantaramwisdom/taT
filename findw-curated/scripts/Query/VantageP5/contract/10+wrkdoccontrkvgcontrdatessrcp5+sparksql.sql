SELECT a.uiddocumentgroupkey,
       a.contractissuedate,
       a.contractissuemonth,
       a.contractissueyear,
       a.contracteffectiveyear,
       a.contracteffectivemonth,
       a.contracteffectivedate,
       a.contractlastfinancetransactiondate,
       operationcode,
       a.dttlloaddate,
       a.intbatchid
FROM (SELECT generatesameuuid(concat(delta.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             cast(db.policy_issue_date as string) as contractissuedate,
             cast(db.contract_issue_month as string) as contractissuemonth,
             cast(db.contract_issue_year as string) as contractissueyear,
             db.contract_effective_year as contracteffectiveyear,
             db.contract_effective_month as contracteffectivemonth,
             cast(db.policy_eff_date as string) as contracteffectivedate,
             cast(db.last_fin_process_date as date) as contractlastfinancetransactiondate,
             db.operation_code as operationcode,
             '{cycle_date}' as dttlloaddate,
             {batchid} as intbatchid,
             row_number() OVER(PARTITION BY db.company_code,
                                          db.segment_id,
                                          db.key_qualifier
                               ORDER BY cast(delta.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM deltaunion delta
      JOIN (SELECT policy_issue_date,
                   contract_issue_month,
                   contract_issue_year,
                   contract_effective_year,
                   contract_effective_month,
                   policy_eff_date,
                   key_qualifier,
                   company_code,
                   segment_id,
                   segment_sequence_number,
                   last_fin_process_date,
                   batchid,
                   operation_code
            FROM gdqp5clprodsegmentdb) db
        ON delta.key_qualifier = db.key_qualifier
       AND delta.company_code = db.company_code) a
WHERE a.seq_num = 1;
