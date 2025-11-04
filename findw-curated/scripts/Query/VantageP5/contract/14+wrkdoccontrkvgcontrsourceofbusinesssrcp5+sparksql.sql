SELECT a.uiddocumentgroupkey,
       a.contractsourcemarketingorganizationcode,
       a.contractmodelingbusinessunitgroup,
       a.contractifrs17portfolio,
       a.contractifrs17grouping,
       a.contractifrs17cohort,
       a.contractifrs17profitability,
       operationcode,
       a.dttloaddate,
       a.intbatchid
FROM (SELECT generatesameuuid(concat(delta.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             cast(db.contract_source_marketing_organization_code as string) as contractsourcemarketingorganizationcode,
             cast(db.contract_modeling_business_unit_group as string) as contractmodelingbusinessunitgroup,
             cast(ifrs.contract_ifrs17_portfolio as string) as contractifrs17portfolio,
             cast(ifrs.contract_ifrs17_grouping as string) as contractifrs17grouping,
             cast(ifrs.contract_ifrs17_cohort as string) as contractifrs17cohort,
             cast(ifrs.contract_ifrs17_profitability as string) as contractifrs17profitability,
             db.operation_code as operationcode,
             '{cycle_date}' as dttloaddate,
             {batchid} as intbatchid,
             row_number() OVER(PARTITION BY db.company_code,
                                          db.segment_id,
                                          db.key_qualifier
                               ORDER BY cast(delta.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM deltaunion delta
      JOIN (SELECT key_qualifier,
                   company_code,
                   batchid,
                   segment_id,
                   segment_sequence_number,
                   operation_code,
                   contract_source_marketing_organization_code,
                   contract_modeling_business_unit_group
            FROM gdqp5clprodsegmentdb
            WHERE cast(batchid as int) <= {batchid}) db
        ON delta.key_qualifier = db.key_qualifier
       AND delta.company_code = db.company_code
      LEFT JOIN gdqp5ifrscncontracttagging ifrs
        ON db.key_qualifier = ifrs.key_qualifier
       AND db.company_code = ifrs.company_code) a
WHERE a.seq_num = 1;
