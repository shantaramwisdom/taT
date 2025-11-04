SELECT uni.source_system_name
,uni.key_qualifier
,uni.source_system_activity_id
,uni.source_activity_id
,uni.activity_type
,uni.activity_amount_type
,uni.activity_effective_date
,uni.activity_reported_date
,uni.fund_number
,uni.activity_general_ledger_application_area_code
,uni.activity_general_ledger_source_code
,uni.activity_source_transaction_code
,uni.activity_money_method
,uni.activity_first_year_renewal_indicator
,uni.activity_type_group
,uni.activity_amount
,uni.source_activity_parent_id
,uni.activity_source_originating_user_id
,uni.activity_source_suspense_reference_number
,uni.activity_1035exchange_indicator
,uni.activity_source_claim_identifier
,uni.contract_administration_location_code
,uni.source_fund_identifier
,uni.activity_withholding_tax_jurisdiction
,uni.activity_source_accounting_memo_code
,uni.activity_reversal_code
,uni.legal_company_code
,uni.source_legal_entity_code
,uni.activity_deposit_source_batch_identifier
,uni.activity_source_accounting_deviator
,CASE
    WHEN db1.key_qualifier IS NOT NULL
    THEN generateSameUUID(CONCAT (
        'P5'
        ,'.'
        ,db1.key_qualifier
        ,'.'
        ,db1.company_code
    ))
    ELSE NULL
END AS fkcontractdocumentid
,uni.fkfunddocumentid
,uni.operationcode
FROM (
    SELECT gdqp5trxhist.source_system_name
    ,gdqp5trxhist.key_qualifier
    ,gdqp5trxhist.company_code
    ,gdqp5trxhist.source_system_activity_id
    ,gdqp5trxhist.source_activity_id
    ,gdqp5trxhist.activity_type
    ,gdqp5trxhist.activity_amount_type
    ,gdqp5trxhist.activity_effective_date
    ,gdqp5trxhist.activity_reported_date
    ,gdqp5trxhist.fund_number
    ,gdqp5trxhist.activity_general_ledger_application_area_code
    ,gdqp5trxhist.activity_general_ledger_source_code
    ,gdqp5trxhist.activity_source_transaction_code
    ,gdqp5trxhist.activity_money_method
    ,gdqp5trxhist.activity_first_year_renewal_indicator
    ,gdqp5trxhist.activity_type_group
    ,gdqp5trxhist.activity_amount
    ,gdqp5trxhist.source_activity_parent_id
    ,gdqp5trxhist.activity_source_originating_user_id
    ,gdqp5trxhist.activity_source_suspense_reference_number
    ,gdqp5trxhist.activity_1035exchange_indicator
    ,gdqp5trxhist.activity_source_claim_identifier
    ,gdqp5trxhist.contract_administration_location_code
    ,gdqp5trxhist.source_fund_identifier
    ,gdqp5trxhist.activity_withholding_tax_jurisdiction
    ,gdqp5trxhist.activity_source_accounting_memo_code
    ,gdqp5trxhist.activity_reversal_code
    ,gdqp5trxhist.legal_company_code
    ,gdqp5trxhist.source_legal_entity_code
    ,gdqp5trxhist.activity_deposit_source_batch_identifier
    ,gdqp5trxhist.activity_source_accounting_deviator
    ,gdqp5trxhist.fkfunddocumentid
    ,gdqp5trxhist.operationcode
    FROM (
        SELECT source_system_name
        ,key_qualifier
        ,company_code
        ,coalesce(source_system_activity_id_override, c_source_system_activity_id, source_system_activity_id) AS source_system_activity_id
        ,coalesce(source_activity_id_override, c_source_activity_id, source_activity_id) AS source_activity_id
        ,coalesce(activity_type_override, c_activity_type, activity_type) AS activity_type
        ,coalesce(activity_amount_type_override, c_activity_amount_type, activity_amount_type) AS activity_amount_type
        ,coalesce(activity_effective_date_override, c_activity_effective_date, activity_effective_date) AS activity_effective_date
        ,coalesce(activity_reported_date_override, c_activity_reported_date, activity_reported_date) AS activity_reported_date
        ,NULL AS fund_number
        ,coalesce(activity_general_ledger_application_area_code_override, c_activity_general_ledger_application_area_code, activity_general_ledger_application_area_code) AS activity_general_ledger_application_area_code
        ,coalesce(activity_general_ledger_source_code_override, c_activity_general_ledger_source_code, activity_general_ledger_source_code) AS activity_general_ledger_source_code
        ,coalesce(activity_source_transaction_code_override, c_activity_source_transaction_code, activity_source_transaction_code) AS activity_source_transaction_code
        ,coalesce(activity_money_method_override, c_activity_money_method, activity_money_method) AS activity_money_method
        ,coalesce(activity_first_year_renewal_indicator_override, c_activity_first_year_renewal_indicator, activity_first_year_renewal_indicator) AS activity_first_year_renewal_indicator
        ,coalesce(activity_type_group_override, c_activity_type_group, activity_type_group) AS activity_type_group
        ,coalesce(activity_amount_override, c_activity_amount, activity_amount) AS activity_amount
        ,coalesce(source_activity_parent_id_override, c_source_activity_parent_id, source_activity_parent_id) AS source_activity_parent_id
        ,coalesce(activity_source_originating_user_id_override, c_activity_source_originating_user_id, activity_source_originating_user_id) AS activity_source_originating_user_id
        ,NULL AS activity_source_suspense_reference_number
        ,coalesce(activity_1035exchange_indicator_override, c_activity_1035exchange_indicator, activity_1035exchange_indicator) AS activity_1035exchange_indicator
        ,coalesce(activity_source_claim_identifier_override, c_activity_source_claim_identifier, activity_source_claim_identifier) AS activity_source_claim_identifier
        ,coalesce(contract_administration_location_code_override, c_contract_administration_location_code, contract_administration_location_code) AS contract_administration_location_code
        ,coalesce(source_fund_identifier_override, c_source_fund_identifier, source_fund_identifier) AS source_fund_identifier
        ,coalesce(activity_withholding_tax_jurisdiction_override, c_activity_withholding_tax_jurisdiction, activity_withholding_tax_jurisdiction) AS activity_withholding_tax_jurisdiction
        ,coalesce(activity_source_accounting_memo_code_override, c_activity_source_accounting_memo_code, activity_source_accounting_memo_code) AS activity_source_accounting_memo_code
        ,coalesce(activity_reversal_code_override, c_activity_reversal_code, activity_reversal_code) AS activity_reversal_code
        ,coalesce(legal_company_code_override, c_legal_company_code, legal_company_code) AS legal_company_code
        ,coalesce(source_legal_entity_code_override, c_source_legal_entity_code, source_legal_entity_code) AS source_legal_entity_code
        ,coalesce(activity_deposit_source_batch_identifier_override, c_activity_deposit_source_batch_identifier, activity_deposit_source_batch_identifier) AS activity_deposit_source_batch_identifier
        ,coalesce(activity_source_accounting_deviator_override, c_activity_source_accounting_deviator, activity_source_accounting_deviator) AS activity_source_accounting_deviator
        ,NULL AS fkfunddocumentid
        ,NULL AS operationcode
        ,row_number() OVER (
            PARTITION BY source_system_name
            ,coalesce(source_activity_id_override, c_source_activity_id, source_activity_id)
            ,coalesce(activity_type_override, c_activity_type, activity_type)
            ,coalesce(activity_amount_type_override, c_activity_amount_type, activity_amount_type) ORDER BY cast(batchid AS INT) DESC
        ) AS seq_num
        FROM {source_database}.gdqp5trxhist
        WHERE cast(batchid AS INT) = {batchid}
    ) gdqp5trxhist
    WHERE gdqp5trxhist.seq_num = 1
UNION
SELECT gdqp5trxhist_fund_info.source_system_name
,gdqp5trxhist_fund_info.key_qualifier
,gdqp5trxhist_fund_info.company_code
,gdqp5trxhist_fund_info.source_system_activity_id
,gdqp5trxhist_fund_info.source_activity_id
,gdqp5trxhist_fund_info.activity_type
,gdqp5trxhist_fund_info.activity_amount_type
,gdqp5trxhist_fund_info.activity_effective_date
,gdqp5trxhist_fund_info.activity_reported_date
,gdqp5trxhist_fund_info.fund_number
,gdqp5trxhist_fund_info.activity_general_ledger_application_area_code
,gdqp5trxhist_fund_info.activity_general_ledger_source_code
,gdqp5trxhist_fund_info.activity_source_transaction_code
,gdqp5trxhist_fund_info.activity_money_method
,gdqp5trxhist_fund_info.activity_first_year_renewal_indicator
,gdqp5trxhist_fund_info.activity_type_group
,gdqp5trxhist_fund_info.activity_amount
,gdqp5trxhist_fund_info.source_activity_parent_id
,gdqp5trxhist_fund_info.activity_source_originating_user_id
,gdqp5trxhist_fund_info.activity_source_suspense_reference_number
,gdqp5trxhist_fund_info.activity_1035exchange_indicator
,gdqp5trxhist_fund_info.activity_source_claim_identifier
,gdqp5trxhist_fund_info.contract_administration_location_code
,gdqp5trxhist_fund_info.source_fund_identifier
,gdqp5trxhist_fund_info.activity_withholding_tax_jurisdiction
,gdqp5trxhist_fund_info.activity_source_accounting_memo_code
,gdqp5trxhist_fund_info.activity_reversal_code
,gdqp5trxhist_fund_info.legal_company_code
,gdqp5trxhist_fund_info.source_legal_entity_code
,gdqp5trxhist_fund_info.activity_deposit_source_batch_identifier
,gdqp5trxhist_fund_info.activity_source_accounting_deviator
,gdqp5trxhist_fund_info.fkfunddocumentid
,gdqp5trxhist_fund_info.operationcode
FROM (
    SELECT source_system_name
    ,key_qualifier
    ,company_code
    ,coalesce(source_system_activity_id_override, c_source_system_activity_id, source_system_activity_id) AS source_system_activity_id
    ,coalesce(source_activity_id_override, c_source_activity_id, source_activity_id) AS source_activity_id
    ,coalesce(activity_type_override, c_activity_type, activity_type) AS activity_type
    ,coalesce(activity_amount_type_override, c_activity_amount_type, activity_amount_type) AS activity_amount_type
    ,coalesce(activity_effective_date_override, c_activity_effective_date, activity_effective_date) AS activity_effective_date
    ,coalesce(activity_reported_date_override, c_activity_reported_date, activity_reported_date) AS activity_reported_date
    ,fund_number
    ,coalesce(activity_general_ledger_application_area_code_override, c_activity_general_ledger_application_area_code, activity_general_ledger_application_area_code) AS activity_general_ledger_application_area_code
    ,coalesce(activity_general_ledger_source_code_override, c_activity_general_ledger_source_code, activity_general_ledger_source_code) AS activity_general_ledger_source_code
    ,coalesce(activity_source_transaction_code_override, c_activity_source_transaction_code, activity_source_transaction_code) AS activity_source_transaction_code
    ,coalesce(activity_money_method_override, c_activity_money_method, activity_money_method) AS activity_money_method
    ,coalesce(activity_first_year_renewal_indicator_override, c_activity_first_year_renewal_indicator, activity_first_year_renewal_indicator) AS activity_first_year_renewal_indicator
    ,coalesce(activity_type_group_override, c_activity_type_group, activity_type_group) AS activity_type_group
    ,coalesce(activity_amount_override, c_activity_amount, activity_amount) AS activity_amount
    ,coalesce(source_activity_parent_id_override, c_source_activity_parent_id, source_activity_parent_id) AS source_activity_parent_id
    ,coalesce(activity_source_originating_user_id_override, c_activity_source_originating_user_id, activity_source_originating_user_id) AS activity_source_originating_user_id
    ,NULL AS activity_source_suspense_reference_number
    ,coalesce(activity_1035exchange_indicator_override, c_activity_1035exchange_indicator, activity_1035exchange_indicator) AS activity_1035exchange_indicator
    ,coalesce(activity_source_claim_identifier_override, c_activity_source_claim_identifier, activity_source_claim_identifier) AS activity_source_claim_identifier
    ,coalesce(contract_administration_location_code_override, c_contract_administration_location_code, contract_administration_location_code) AS contract_administration_location_code
    ,coalesce(source_fund_identifier_override, c_source_fund_identifier, source_fund_identifier) AS source_fund_identifier
    ,coalesce(activity_withholding_tax_jurisdiction_override, c_activity_withholding_tax_jurisdiction, activity_withholding_tax_jurisdiction) AS activity_withholding_tax_jurisdiction
    ,coalesce(activity_source_accounting_memo_code_override, c_activity_source_accounting_memo_code, activity_source_accounting_memo_code) AS activity_source_accounting_memo_code
    ,coalesce(activity_reversal_code_override, c_activity_reversal_code, activity_reversal_code) AS activity_reversal_code
    ,coalesce(legal_company_code_override, c_legal_company_code, legal_company_code) AS legal_company_code
    ,coalesce(source_legal_entity_code_override, c_source_legal_entity_code, source_legal_entity_code) AS source_legal_entity_code
    ,coalesce(activity_deposit_source_batch_identifier_override, c_activity_deposit_source_batch_identifier, activity_deposit_source_batch_identifier) AS activity_deposit_source_batch_identifier
    ,coalesce(activity_source_accounting_deviator_override, c_activity_source_accounting_deviator, activity_source_accounting_deviator) AS activity_source_accounting_deviator
    ,generateSameUUID(CONCAT (
        fund_number
        ,'.'
        ,company_code
    )) AS fkfunddocumentid
    ,NULL AS operationcode
    ,row_number() OVER (
        PARTITION BY source_system_name
        ,coalesce(source_activity_id_override, c_source_activity_id, source_activity_id)
        ,coalesce(activity_type_override, c_activity_type, activity_type)
        ,coalesce(activity_amount_type_override, c_activity_amount_type, activity_amount_type) ORDER BY cast(batchid AS INT) DESC
    ) AS seq_num
    FROM {source_database}.gdqp5trxhist_fund_info
    WHERE cast(batchid AS INT) = {batchid}
) gdqp5trxhist_fund_info
WHERE gdqp5trxhist_fund_info.seq_num = 1
)
UNION
(
SELECT gdqp5suspense.source_system_name
,gdqp5suspense.key_qualifier
,gdqp5suspense.company_code
,gdqp5suspense.source_system_activity_id
,gdqp5suspense.source_activity_id
,gdqp5suspense.activity_type
,gdqp5suspense.activity_amount_type
,gdqp5suspense.activity_effective_date
,gdqp5suspense.activity_reported_date
,gdqp5suspense.fund_number
,gdqp5suspense.activity_general_ledger_application_area_code
,gdqp5suspense.activity_general_ledger_source_code
,gdqp5suspense.activity_source_transaction_code
,gdqp5suspense.activity_money_method
,gdqp5suspense.activity_first_year_renewal_indicator
,gdqp5suspense.activity_type_group
,gdqp5suspense.activity_amount
,gdqp5suspense.source_activity_parent_id
,gdqp5suspense.activity_source_originating_user_id
,gdqp5suspense.activity_source_suspense_reference_number
,gdqp5suspense.activity_1035exchange_indicator
,gdqp5suspense.activity_source_claim_identifier
,gdqp5suspense.contract_administration_location_code
,gdqp5suspense.source_fund_identifier
,gdqp5suspense.activity_withholding_tax_jurisdiction
,gdqp5suspense.activity_source_accounting_memo_code
,gdqp5suspense.activity_reversal_code
,gdqp5suspense.legal_company_code
,gdqp5suspense.source_legal_entity_code
,gdqp5suspense.activity_deposit_source_batch_identifier
,gdqp5suspense.activity_source_accounting_deviator
,gdqp5suspense.fkfunddocumentid
,gdqp5suspense.operationcode
FROM (
    SELECT source_system_name
    ,key_qualifier
    ,company_code
    ,coalesce(activity_type_override, c_activity_type, activity_type) AS activity_type
    ,coalesce(activity_reported_date_override, c_activity_reported_date, activity_reported_date) AS activity_reported_date
    ,coalesce(activity_effective_date_override, c_activity_effective_date, activity_effective_date) AS activity_effective_date
    ,coalesce(activity_source_accounting_memo_code_override, c_activity_source_accounting_memo_code, activity_source_accounting_memo_code) AS activity_source_accounting_memo_code
    ,coalesce(activity_money_method_override, c_activity_money_method, activity_money_method) AS activity_money_method
    ,coalesce(activity_reversal_code_override, c_activity_reversal_code, activity_reversal_code) AS activity_reversal_code
    ,coalesce(activity_source_transaction_code_override, c_activity_source_transaction_code, activity_source_transaction_code) AS activity_source_transaction_code
    ,coalesce(source_legal_entity_code_override, c_source_legal_entity_code, source_legal_entity_code) AS source_legal_entity_code
    ,coalesce(legal_company_code_override, c_legal_company_code, legal_company_code) AS legal_company_code
    ,coalesce(activity_amount_override, c_activity_amount, activity_amount) AS activity_amount
    ,coalesce(activity_amount_type_override, c_activity_amount_type, activity_amount_type) AS activity_amount_type
    ,coalesce(source_system_activity_id_override, c_source_system_activity_id, source_system_activity_id) AS source_system_activity_id
    ,coalesce(source_activity_parent_id_override, c_source_activity_parent_id, source_activity_parent_id) AS source_activity_parent_id
    ,NULL AS fund_number
    ,coalesce(source_activity_id_override, c_source_activity_id, source_activity_id) AS source_activity_id
    ,coalesce(activity_type_group_override, c_activity_type_group, activity_type_group) AS activity_type_group
    ,coalesce(activity_first_year_renewal_indicator_override, c_activity_first_year_renewal_indicator, activity_first_year_renewal_indicator) AS activity_first_year_renewal_indicator
    ,coalesce(activity_general_ledger_source_code_override, c_activity_general_ledger_source_code, activity_general_ledger_source_code) AS activity_general_ledger_source_code
    ,coalesce(activity_general_ledger_application_area_code_override, c_activity_general_ledger_application_area_code, activity_general_ledger_application_area_code) AS activity_general_ledger_application_area_code
    ,coalesce(activity_source_accounting_deviator_override, c_activity_source_accounting_deviator, activity_source_accounting_deviator) AS activity_source_accounting_deviator
    ,coalesce(activity_source_originating_user_id_override, c_activity_source_originating_user_id, activity_source_originating_user_id) AS activity_source_originating_user_id
    ,coalesce(activity_source_suspense_reference_number_override, c_activity_source_suspense_reference_number, activity_source_suspense_reference_number) AS activity_source_suspense_reference_number
    ,coalesce(activity_deposit_source_batch_identifier_override, c_activity_deposit_source_batch_identifier, activity_deposit_source_batch_identifier) AS activity_deposit_source_batch_identifier
    ,coalesce(activity_source_claim_identifier_override, c_activity_source_claim_identifier, activity_source_claim_identifier) AS activity_source_claim_identifier
    ,coalesce(activity_1035exchange_indicator_override, c_activity_1035exchange_indicator, activity_1035exchange_indicator) AS activity_1035exchange_indicator
    ,coalesce(contract_administration_location_code_override, c_contract_administration_location_code, contract_administration_location_code) AS contract_administration_location_code
    ,coalesce(source_fund_identifier_override, c_source_fund_identifier, source_fund_identifier) AS source_fund_identifier
    ,coalesce(activity_withholding_tax_jurisdiction_override, c_activity_withholding_tax_jurisdiction, activity_withholding_tax_jurisdiction) AS activity_withholding_tax_jurisdiction
    ,NULL AS fkfunddocumentid
    ,NULL AS operationcode
    ,row_number() OVER (
        PARTITION BY source_system_name
        ,coalesce(source_activity_id_override, c_source_activity_id, source_activity_id)
        ,coalesce(activity_type_override, c_activity_type, activity_type)
        ,coalesce(activity_amount_type_override, c_activity_amount_type, activity_amount_type) ORDER BY cast(batchid AS INT) DESC
    ) AS seq_num
    FROM {source_database}.gdqp5suspense
    WHERE cast(batchid AS INT) = {batchid}
) gdqp5suspense
WHERE gdqp5suspense.seq_num = 1
) uni
LEFT JOIN (
    SELECT key_qualifier
    ,company_code
    ,row_number() OVER (
        PARTITION BY key_qualifier
        ,company_code ORDER BY gdqp5clprodsegmentdb.batchid DESC
    ) AS row_num
    FROM {source_database}.gdqp5clprodsegmentdb
    WHERE gdqp5clprodsegmentdb.batchid <= {batchid}
) db1 ON uni.key_qualifier = db1.key_qualifier
AND uni.company_code = db1.company_code
AND db1.row_num = 1

