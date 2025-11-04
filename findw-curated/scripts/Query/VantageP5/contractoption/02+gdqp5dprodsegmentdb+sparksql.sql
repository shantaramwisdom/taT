SELECT db.*,
       CASE
           WHEN delta.operation_code = 'DELT' THEN 'DELT'
           ELSE 'NON-DELT'
       END AS operation_code
FROM
    (SELECT uuid,
            segment_id,
            segment_sequence_number,
            product_code,
            ordinal_position,
            coalesce(company_code_override, c_company_code, company_code) AS company_code,
            coalesce(key_qualifier_override, c_key_qualifier, key_qualifier) AS key_qualifier,
            coalesce(plan_code_override, c_plan_code, plan_code) AS plan_code,
            coalesce(policy_issue_state_override, c_policy_issue_state, policy_issue_state) AS policy_issue_state,
            coalesce(policy_issue_age_override, c_policy_issue_age, policy_issue_age) AS policy_issue_age,
            coalesce(policy_eff_date_override, c_policy_eff_date, policy_eff_date) AS policy_eff_date,
            coalesce(contract_option_duration_in_months_override, c_contract_option_duration_in_months, contract_option_duration_in_months) AS contract_option_duration_in_months,
            coalesce(contract_option_modeling_type_group_indicator_override, c_contract_option_modeling_type_group_indicator, contract_option_modeling_type_group_indicator) AS contract_option_modeling_type_group_indicator,
            coalesce(contract_option_single_joint_indicator_override, c_contract_option_single_joint_indicator, contract_option_single_joint_indicator) AS contract_option_single_joint_indicator,
            coalesce(contract_option_death_indicator_override, c_contract_option_death_indicator, contract_option_death_indicator) AS contract_option_death_indicator,
            coalesce(contract_option_income_enhancement_indicator_override, c_contract_option_income_enhancement_indicator, contract_option_income_enhancement_indicator) AS contract_option_income_enhancement_indicator,
            coalesce(contract_option_modeling_pwvb_segment_override, c_contract_option_modeling_pwvb_segment, contract_option_modeling_pwvb_segment) AS contract_option_modeling_pwvb_segment,
            row_type,
            load_date,
            batchid
     FROM
         (SELECT uuid,
                 segment_id,
                 segment_sequence_number,
                 product_code,
                 ordinal_position,
                 company_code_override,
                 c_company_code,
                 company_code,
                 key_qualifier_override,
                 c_key_qualifier,
                 key_qualifier,
                 plan_code_override,
                 c_plan_code,
                 plan_code,
                 policy_issue_state_override,
                 c_policy_issue_state,
                 policy_issue_state,
                 policy_issue_age_override,
                 c_policy_issue_age,
                 policy_issue_age,
                 policy_eff_date_override,
                 c_policy_eff_date,
                 policy_eff_date,
                 contract_option_duration_in_months_override,
                 c_contract_option_duration_in_months,
                 contract_option_duration_in_months,
                 contract_option_modeling_type_group_indicator_override,
                 c_contract_option_modeling_type_group_indicator,
                 contract_option_modeling_type_group_indicator,
                 contract_option_single_joint_indicator_override,
                 c_contract_option_single_joint_indicator,
                 contract_option_single_joint_indicator,
                 contract_option_death_indicator_override,
                 c_contract_option_death_indicator,
                 contract_option_death_indicator,
                 contract_option_income_enhancement_indicator_override,
                 c_contract_option_income_enhancement_indicator,
                 contract_option_income_enhancement_indicator,
                 contract_option_modeling_pwvb_segment_override,
                 c_contract_option_modeling_pwvb_segment,
                 contract_option_modeling_pwvb_segment,
                 row_type,
                 load_date,
                 batchid,
                 rank() OVER(PARTITION BY key_qualifier,
                                       company_code,
                                       ordinal_position
                            ORDER BY cast(batchid AS int) DESC,
                                     segment_sequence_number DESC) AS latest_rank
          FROM {source_database}.gdqp5dprodsegmentdb
          WHERE cast(batchid AS int) <= {batchid}) db
     WHERE latest_rank = 1) db
LEFT JOIN gdqp5deltaextract_mainfile_keys delta
ON db.key_qualifier = delta.key_qualifier
AND db.company_code = delta.company_code
AND delta.segment_id = 'DB';