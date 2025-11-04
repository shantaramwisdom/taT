select 
    a.*
from 
(
    select 
        generatesameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code,';',cast(db.ordinal_position as string),':',
        cast(segmenteb_ebu_fund_information.fund_number as string))) as uiddocumentgroupkey,
        generatesameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code,';',cast(db.ordinal_position as string))) as contractoptiondocumentid,
        generatesameuuid(concat('{system_name}',';',db.key_qualifier,';',db.company_code)) as contractdocumentid,
        db.contractnumber,
        cast(db.ordinal_position as string) as contractoptionsourcesystemordinalposition,
        segmenteb_ebu_fund_information.contractoptionfundnumber,
        db.contractadministrationlocationcode,
        case 
            when db.operation_code = 'DELT' 
                or segmenteb_ebu_fund_information.operation_code = 'DELT' 
                then 'DELT'
            else 'NON-DELT'
        end as operationcode,
        ('{cycle_date}') as dttloaddate,
        (batchid) as intbatchid,
        segmenteb_ebu_fund_information.contractoptionfundclassindicator,
        inf.contractoptionfundbundledrevenueshareamount,
        inf.contractoptionfundbundledinvestmentmanagementfeeamount,
        inf.contractoptionfundbundledinvestmentmanagementfeeamount,
        inf.contractoptionfundbundledfundfacilitationfeeamount,
        inf.contractoptionfundbundledgainlossamount,
        inf.contractoptionfundsafeinvestmentmanagementfeeamount,
        inf.contractoptionfundgapvalue,
        inf.contractoptionfundvalue,
        inf.contractoptionfunddefaultfundindicator,
        inf.contractoptionfundseparateaccountvalueindex1,
        inf.contractoptionfundseparateaccountvalueindex2,
        inf.contractoptionfundseparateaccountvalueindex3,
        inf.contractoptionfundseparateaccountvalueindex4,
        inf.contractoptionfundseparateaccountvalueindex5,
        inf.contractoptionfundseparateaccountvalueindex6,
        inf.contractoptionfundseparateaccountvalueindex7,
        inf.contractoptionfundseparateaccountvalueindex8,
        inf.contractoptionfundseparateaccountvalueindex9,
        inf.contractoptionfundseparateaccountvalueindex10,
        inf.contractoptionfundsafevalue,
        inf.contractoptionfunddynamicseparateaccountvalue,
        inf.contractoptionfundselfhedgingseparateaccountvalue,
        inf.initalpremium,
        inf.surrendervalueamount,
        inf.modelingrenewalpremium,
        row_number() over(partition by db.company_code,
                                          db.segment_id,
                                          db.key_qualifier,
                                          db.ordinal_position,
                                          segmenteb_ebu_fund_information.fund_number
                          order by 
                              cast(delta.batchid as int) desc,
                              cast(db.batchid as int) desc,
                              cast(segmenteb_ebu_fund_information.batchid as int) desc,
                              db.segment_sequence_number desc) as seq_num
    from 
        deltajunion delta
    join gdqp5dprodsegmentdb db 
        on db.key_qualifier = delta.key_qualifier
        and db.company_code = delta.company_code
    join 
        (
            select 
                company_code,
                key_qualifier,
                fund_number,
                segment_sequence_number,
                occurs_index,
                contractoptionfundnumber,
                contractoptionfundclassindicator,
                operation_code,
                batchid
            from 
                gdqp5dprodsegmenteb_fund_information
            union all
            select 
                company_code,
                key_qualifier,
                fund_number,
                segment_sequence_number,
                occurs_index,
                contractoptionfundnumber,
                contractoptionfundclassindicator,
                operation_code,
                batchid
            from 
                gdqp5dprodsegmentebu_fund_information
        ) segmenteb_ebu_fund_information
        on segmenteb_ebu_fund_information.key_qualifier = delta.key_qualifier
        and segmenteb_ebu_fund_information.company_code = delta.company_code
    left join 
        (
            select 
                *
            from 
                (
                    select 
                        ex_policy_number,
                        fund_number,
                        coalesce(contract_option_fund_bundled_revenue_share_amount_override, c_contract_option_fund_bundled_revenue_share_amount, contract_option_fund_bundled_revenue_share_amount) as contractoptionfundbundledrevenueshareamount,
                        coalesce(contract_option_fund_bundled_investment_management_fee_amount_override, c_contract_option_fund_bundled_investment_management_fee_amount, contract_option_fund_bundled_investment_management_fee_amount) as contractoptionfundbundledinvestmentmanagementfeeamount,
                        coalesce(contract_option_fund_unbundled_revenue_share_amount_override, c_contract_option_fund_unbundled_revenue_share_amount, contract_option_fund_unbundled_revenue_share_amount) as contractoptionfundunbundledrevenueshareamount,
                        coalesce(contract_option_fund_unbundled_investment_management_fee_amount_override, c_contract_option_fund_unbundled_investment_management_fee_amount, contract_option_fund_unbundled_investment_management_fee_amount) as contractoptionfundunbundledinvestmentmanagementfeeamount,
                        coalesce(contract_option_fund_unbundled_fund_facilitation_fee_amount_override, c_contract_option_fund_unbundled_fund_facilitation_fee_amount, contract_option_fund_unbundled_fund_facilitation_fee_amount) as contractoptionfundunbundledfundfacilitationfeeamount,
                        coalesce(contract_option_fund_unbundled_gain_loss_amount_override, c_contract_option_fund_unbundled_gain_loss_amount, contract_option_fund_unbundled_gain_loss_amount) as contractoptionfundunbundledgainlossamount,
                        coalesce(contract_option_fund_safe_revenue_share_amount_override, c_contract_option_fund_safe_revenue_share_amount, contract_option_fund_safe_revenue_share_amount) as contractoptionfundsaferevenueshareamount,
                        coalesce(contract_option_fund_safe_investment_management_fee_amount_override, c_contract_option_fund_safe_investment_management_fee_amount, contract_option_fund_safe_investment_management_fee_amount) as contractoptionfundsafeinvestmentmanagementfeeamount,
                        coalesce(contract_option_fund_gap_value_override, c_contract_option_fund_gap_value, contract_option_fund_gap_value) as contractoptionfundgapvalue,
                        coalesce(contract_option_fund_value_override, c_contract_option_fund_value, contract_option_fund_value) as contractoptionfundvalue,
                        coalesce(contract_option_fund_default_fund_indicator_override, c_contract_option_fund_default_fund_indicator, contract_option_fund_default_fund_indicator) as contractoptionfunddefaultfundindicator,
                        coalesce(contract_option_fund_separate_account_value_index1_override, c_contract_option_fund_separate_account_value_index1, contract_option_fund_separate_account_value_index1) as contractoptionfundseparateaccountvalueindex1,
                        coalesce(contract_option_fund_separate_account_value_index2_override, c_contract_option_fund_separate_account_value_index2, contract_option_fund_separate_account_value_index2) as contractoptionfundseparateaccountvalueindex2,
                        coalesce(contract_option_fund_separate_account_value_index3_override, c_contract_option_fund_separate_account_value_index3, contract_option_fund_separate_account_value_index3) as contractoptionfundseparateaccountvalueindex3,
                        coalesce(contract_option_fund_separate_account_value_index4_override, c_contract_option_fund_separate_account_value_index4, contract_option_fund_separate_account_value_index4) as contractoptionfundseparateaccountvalueindex4,
                        coalesce(contract_option_fund_separate_account_value_index5_override, c_contract_option_fund_separate_account_value_index5, contract_option_fund_separate_account_value_index5) as contractoptionfundseparateaccountvalueindex5,
                        coalesce(contract_option_fund_separate_account_value_index6_override, c_contract_option_fund_separate_account_value_index6, contract_option_fund_separate_account_value_index6) as contractoptionfundseparateaccountvalueindex6,
                        coalesce(contract_option_fund_separate_account_value_index7_override, c_contract_option_fund_separate_account_value_index7, contract_option_fund_separate_account_value_index7) as contractoptionfundseparateaccountvalueindex7,
                        coalesce(contract_option_fund_separate_account_value_index8_override, c_contract_option_fund_separate_account_value_index8, contract_option_fund_separate_account_value_index8) as contractoptionfundseparateaccountvalueindex8,
                        coalesce(contract_option_fund_separate_account_value_index9_override, c_contract_option_fund_separate_account_value_index9, contract_option_fund_separate_account_value_index9) as contractoptionfundseparateaccountvalueindex9,
                        coalesce(contract_option_fund_separate_account_value_index10_override, c_contract_option_fund_separate_account_value_index10, contract_option_fund_separate_account_value_index10) as contractoptionfundseparateaccountvalueindex10,
                        coalesce(contract_option_fund_safe_fund_value_override, c_contract_option_fund_safe_fund_value, contract_option_fund_safe_fund_value) as contractoptionfundsafevalue,
                        coalesce(contract_option_fund_dynamic_separate_account_value_override, c_contract_option_fund_dynamic_separate_account_value, contract_option_fund_dynamic_separate_account_value) as contractoptionfunddynamicseparateaccountvalue,
                        coalesce(contract_option_fund_self_hedging_separate_account_value_override, c_contract_option_fund_self_hedging_separate_account_value, contract_option_fund_self_hedging_separate_account_value) as contractoptionfundselfhedgingseparateaccountvalue,
                        coalesce(surrender_value_amount_fund_override, c_surrender_value_amount_fund, surrender_value_amount_fund) as surrendervalueamount,
                        row_number() over(partition by ex_policy_number, fund_number order by cast(batchid as int) desc) as seq_num
                    from 
                        {source_database}.gdqp5inforceevaluation
                    where 
                        cast(batchid as int) <= cast('{batchidinf}' as int)
                ) a
            where 
                a.seq_num = 1
        ) inf
        on db.key_qualifier = inf.ex_policy_number
        and cast(inf.fund_number as int) = cast(segmenteb_ebu_fund_information.fund_number as int)
    left join 
(
    select 
        *
    from 
        (
            select 
                policy_number,
                fund_number,
                coalesce(initial_premium_override, c_initial_premium, initial_premium) as initialpremium,
                coalesce(modeling_renewal_premium_override, c_modeling_renewal_premium, modeling_renewal_premium) as modelingrenewalpremium,
                row_number() over(partition by policy_number, fund_number order by cast(batchid as int) desc) as seq_num
            from 
                {source_database}.gdqp5inforceevaluation_derived
            where 
                cast(batchid as int) <= cast('{batchidinf}' as int)
        ) a
    where 
        a.seq_num = 1
) infder on 
db.key_qualifier = infder.policy_number
and cast(infder.fund_number as int) = cast(segmenteb_ebu_fund_information.fund_number as int)
) a
where 
a.seq_num = 1
