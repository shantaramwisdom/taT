SELECT a.uiddocumentgroupkey,
       a.contractoptiontypegroup,
       a.contractoptionplancode,
       a.contractoptiongenerationid,
       a.contractoptionissuestatecode,
       a.contractoptionreturnofpremiumvalue,
       a.contractoptionstepvalue,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code,';',cast(db.ordinal_position as string))) as uiddocumentgroupkey,
             null as contractoptiontypegroup,
             db.plan_code as contractoptionplancode,
             null as contractoptiongenerationid,
             db.operation_code as operationcode,
             ('cycle_date') as load_date,
             {batchid} as batchid,
             db.policy_issue_state as contractoptionissuestatecode,
             null as contractoptionreturnofpremiumvalue,
             null as contractoptionstepvalue,
             row_number() over(PARTITION BY db.company_code,
                                             db.segment_id,
                                             db.key_qualifier,
                                             db.ordinal_position
                               ORDER BY cast(delta.batchid as int) desc, cast(db.batchid as int) desc) as seq_num
      FROM (SELECT delta.*
            FROM gdqp5deltaextract_mainfile_keys delta
            WHERE cast(delta.batchid as int) <= {batchid}
              and delta.system_name = 'PS'
              and delta.file_name like 'CLPROXX'
              and delta.segment_id = 'DB') delta
           join (SELECT policy_issue_state,
                        batchid,
                        key_qualifier,
                        company_code,
                        plan_code,
                        operation_code,
                        ordinal_position,
                        segment_id
                 FROM gdqp5dprodsegmentdb
                 WHERE cast(batchid as int) <= {batchid}) db
           ON db.key_qualifier = delta.key_qualifier
           and db.company_code = delta.company_code)a
WHERE a.seq_num = 1
UNION
SELECT a.uiddocumentgroupkey,
       a.contractoptiontypegroup,
       a.contractoptionplancode,
       a.contractoptiongenerationid,
       a.contractoptionissuestatecode,
       a.contractoptionreturnofpremiumvalue,
       a.contractoptionstepvalue,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat('PS',';',db.key_qualifier,';',db.company_code,';','0')) as uiddocumentgroupkey,
             'GMDB' as contractoptiontypegroup,
             db.plan_code as contractoptionplancode,
             null as contractoptiongenerationid,
             db.operation_code as operationcode,
             ('cycle_date') as load_date,
             {batchid} as batchid,
             null as contractoptionissuestatecode,
             cast(inf.contract_option_return_of_premium_value as string) as contractoptionreturnofpremiumvalue,
             cast(inf.contract_option_step_value as string) as contractoptionstepvalue,
             row_number() over(PARTITION BY inf.ex_policy_number,
                                             db.key_qualifier,
                                             db.company_code
                               ORDER BY cast(inf.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM (SELECT *
            FROM (SELECT key_qualifier,
                         company_code,
                         batchid,
                         segment_sequence_number,
                         plan_code,
                         operation_code,
                         row_number() over(PARTITION BY key_qualifier,
                                                      company_code
                                           ORDER BY segment_sequence_number desc) as seq_num
                  FROM gdqp5dprodsegmentdb
                  WHERE cast(batchid as int) <= {batchid}) db1
            WHERE db1.seq_num = 1)db
           join (SELECT ex_policy_number,
                        batchid,
                        coalesce(contract_option_return_of_premium_value_override,
                                 c_contract_option_return_of_premium_value,
                                 contract_option_return_of_premium_value,
                                 'NULL') as contract_option_return_of_premium_value,
                        coalesce(contract_option_step_value_override,
                                 c_contract_option_step_value,
                                 contract_option_step_value) as contract_option_step_value
                 FROM {source_database}.gdqp5inforceevaluation
                 WHERE cast(batchid as int) <= cast('{batchidinf}' as int)) inf
           ON db.key_qualifier = inf.ex_policy_number)a
WHERE a.seq_num = 1;