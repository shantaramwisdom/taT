SELECT jl.*
FROM (SELECT uuid ,
             company_code ,
             key_qualifier ,
             segment_id ,
             segment_sequence_number ,
             coalesce(t_gross_amount_override,
                      t_gross_amount) as t_gross_amount ,
             coalesce(contract_policy_yearto_date_cumulative_withdrawals_override,
                      c_contract_policy_yearto_date_cumulative_withdrawals) as contract_policy_yearto_date_cumulative_withdrawals ,
             batchid ,
             row_number() OVER(PARTITION BY key_qualifier,
                                          company_code,
                                          segment_id,
                                          segment_sequence_number
                               ORDER BY cast(batchid as int) desc) as seq_num
      FROM {source_database}.gdqp5clprodsegmentjl_cumulative_info
      WHERE cast(batchid as int) <= (batchid)) jl
LEFT JOIN (SELECT *
           FROM gdqp5deltaextract_mainfile_keys
           WHERE file_name like 'CLPRODX'
             and segment_id = 'JL') delta
       ON delta.key_qualifier = jl.key_qualifier
      and delta.company_code = jl.company_code
      and jl.segment_sequence_number = delta.segment_sequence_number
WHERE (delta.operation_code = 'NON-DELT');
