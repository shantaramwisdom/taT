SELECT ifrs.*
FROM (SELECT uuid ,
             company_code ,
             key_qualifier ,
             segment_id ,
             segment_sequence_number ,
             coalesce(contract_ifrs17_portfolio_override,
                      c_contract_ifrs17_portfolio) as contract_ifrs17_portfolio ,
             coalesce(contract_ifrs17_grouping_override,
                      c_contract_ifrs17_grouping) as contract_ifrs17_grouping ,
             coalesce(contract_ifrs17_cohort_override,
                      c_contract_ifrs17_cohort) as contract_ifrs17_cohort ,
             coalesce(contract_ifrs17_profitability_override,
                      c_contract_ifrs17_profitability) as contract_ifrs17_profitability ,
             row_type ,
             time_stamp ,
             load_date ,
             batchid ,
             row_number() OVER(PARTITION BY key_qualifier,
                                          company_code
                               ORDER BY cast(batchid as int) desc) as rnum
      FROM {source_database}.gdqp5ifrcontracttagging
      WHERE cast(batchid as int) <= (batchid)) ifrs
WHERE ifrs.rnum = 1;
