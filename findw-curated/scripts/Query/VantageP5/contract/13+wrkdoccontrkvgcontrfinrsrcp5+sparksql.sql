SELECT a.uiddocumentgroupkey,
       a.contracttotalseparateaccountvalue,
       decode(a.surrendervalueamount,null,null,null) as surrendervalueamount,
       a.contractpolicyyeartodatecumulativewithdrawals,
       decode(a.modelingrenewalpremium,null,null,null) as modelingrenewalpremium,
       decode(a.contractfreewithdrawalpercentage,null,null,null) as contractfreewithdrawalpercentage,
       decode(a.contractsurrenderchargepercentage,null,null,null) as contractsurrenderchargepercentage,
       decode(a.initialpremium,null,null,null) as initialpremium,
       decode(a.contractfundvaluetag,null,null,null) as contractfundvaluetag,
       operationcode,
       a.dttloaddate,
       a.intbatchid
FROM (SELECT generatesameuuid(concat(deltaunion.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             cast(inf.fund_value as string) as contracttotalseparateaccountvalue,
             cast(inf2.initial_premium as string) as initialpremium,
             cast(inf.surrender_value_amount as string) as surrendervalueamount,
             cast(inf.contract_total_separate_account_value as string) as contracttotalseparateaccountvalue,
             cast(inf2.modeling_renewal_premium as string) as modelingrenewalpremium,
             cast(inf2.contract_free_withdrawal_percentage as string) as contractfreewithdrawalpercentage,
             cast(inf2.contract_surrender_charge_percentage as string) as contractsurrenderchargepercentage,
             cast(inf2.contract_fund_value_tag as string) as contractfundvaluetag,
             cast(jl.contract_policy_yearto_date_cumulative_withdrawals as string) as contractpolicyyeartodatecumulativewithdrawals,
             db.operation_code as operationcode,
             '{cycle_date}' as dttloaddate,
             {batchid} as intbatchid,
             row_number() OVER(PARTITION BY db.company_code,
                                          db.segment_id,
                                          db.key_qualifier
                               ORDER BY cast(deltaunion.batchid as int) desc, cast(db.batchid as int) desc, cast(inf.batchid as int) desc, cast(inf2.batchid as int) desc, cast(jl.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM deltaunion
      JOIN (SELECT key_qualifier,
                   company_code,
                   operation_code,
                   segment_sequence_number,
                   segment_id,
                   batchid
            FROM gdqp5clprodsegmentdb
            WHERE cast(batchid as int) <= {batchid}) db
        ON deltaunion.key_qualifier = db.key_qualifier
       AND deltaunion.company_code = db.company_code
      LEFT JOIN (SELECT ex_policy_number as policy_number,
                        ex_statutory_company as co_code,
                        batchid,
                        coalesce(ex_total_cash_value_override,
                                 c_ex_total_cash_value,
                                 ex_total_cash_value) as fund_value,
                        coalesce(surrender_value_amount_override,
                                 c_surrender_value_amount,
                                 surrender_value_amount) as surrender_value_amount,
                        coalesce(contract_total_separate_account_value_override,
                                 c_contract_total_separate_account_value,
                                 contract_total_separate_account_value) as contract_total_separate_account_value
                 FROM {source_database}.gdqp5inforcevaluation
                 WHERE cast(batchid as int) = {batchidinf}) inf
        ON db.key_qualifier = inf.policy_number
      LEFT JOIN (SELECT policy_number,
                        company_code,
                        batchid,
                        coalesce(modeling_renewal_premium_override,
                                 c_modeling_renewal_premium,
                                 modeling_renewal_premium) as modeling_renewal_premium,
                        coalesce(contract_free_withdrawal_percentage_override,
                                 c_contract_free_withdrawal_percentage,
                                 contract_free_withdrawal_percentage) as contract_free_withdrawal_percentage,
                        coalesce(contract_surrender_charge_percentage_override,
                                 c_contract_surrender_charge_percentage,
                                 contract_surrender_charge_percentage) as contract_surrender_charge_percentage,
                        coalesce(initial_premium_override,
                                 c_initial_premium,
                                 initial_premium) as initial_premium,
                        coalesce(contract_fund_value_tag_override,
                                 c_contract_fund_value_tag,
                                 contract_fund_value_tag) as contract_fund_value_tag
                 FROM {source_database}.gdqp5inforceevaluation_derived
                 WHERE cast(batchid as int) = {batchidinf}) inf2
        ON db.key_qualifier = inf2.policy_number
      OUTER JOIN (SELECT key_qualifier,
                         company_code,
                         t_gross_amount,
                         contract_policy_yearto_date_cumulative_withdrawals,
                         batchid
                  FROM gdqp5clprodsegmentjl_cumulative_info
                  WHERE cast(batchid as int) <= {batchid}) jl
        ON jl.key_qualifier = db.key_qualifier
       AND jl.company_code = db.company_code) a
WHERE a.seq_num = 1;
