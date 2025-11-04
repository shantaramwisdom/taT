SELECT a.uiddocumentgroupkey,
       a.contractoptionstatuscode,
       a.contractoptionmaximumanniversaryvalue,
       a.contractoptionbenefitsoptindicator,
       a.source_system_ordinal_position,
       a.contractoptionissueage,
       a.contractoptiondurationinmonths,
       a.contractoptionsinglejointindicator,
       a.contractoptiondeathindicator,
       a.contractoptionincomeenhancementindicator,
       a.contractoptionmodelingpwvbsegment,
       a.contractoptionmodelingtypegroupindicator,
       a.contractdocumentid,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code,';',cast(db.ordinal_position as string))) as uiddocumentgroupkey,
             null as contractoptionstatuscode,
             null as contractoptionmaximumanniversaryvalue,
             null as contractoptionbenefitsoptindicator,
             cast(db.ordinal_position as string) as source_system_ordinal_position,
             cast(db.policy_issue_age as string) as contractoptionissueage,
             cast(db.contract_option_duration_in_months as string) as contractoptiondurationinmonths,
             db.contract_option_single_joint_indicator as contractoptionsinglejointindicator,
             db.contract_option_death_indicator as contractoptiondeathindicator,
             db.contract_option_income_enhancement_indicator as contractoptionincomeenhancementindicator,
             db.contract_option_modeling_pwvb_segment as contractoptionmodelingpwvbsegment,
             db.contract_option_modeling_type_group_indicator as contractoptionmodelingtypegroupindicator,
             generateasameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code)) as contractdocumentid,
             db.operation_code as operationcode,
             ('cycle_date') as load_date,
             {batchid} as batchid,
             row_number() over(PARTITION BY db.company_code,
                                             db.segment_id,
                                             db.key_qualifier,
                                             db.ordinal_position
                               ORDER BY cast(delta.batchid as int) desc, cast(db.batchid as int) desc, db.policy_issue_age desc) as seq_num
      FROM (SELECT delta.*
            FROM gdqp5deltaextract_mainfile_keys delta
            WHERE cast(delta.batchid as int) <= {batchid}
              and delta.system_name = 'PS'
              and delta.file_name like 'CLPROXX'
              and delta.segment_id = 'DB') delta
           join (SELECT key_qualifier,
                        company_code,
                        ordinal_position,
                        batchid,
                        policy_issue_age,
                        contract_option_duration_in_months,
                        contract_option_single_joint_indicator,
                        contract_option_death_indicator,
                        contract_option_income_enhancement_indicator,
                        contract_option_modeling_pwvb_segment,
                        contract_option_modeling_type_group_indicator,
                        operation_code,
                        segment_id
                 FROM gdqp5dprodsegmentdb
                 WHERE cast(batchid as int) <= {batchid}) db
           ON db.key_qualifier = delta.key_qualifier
           and db.company_code = delta.company_code)a
WHERE a.seq_num = 1
UNION
SELECT a.uiddocumentgroupkey,
       a.contractoptionstatuscode,
       a.contractoptionmaximumanniversaryvalue,
       a.contractoptionbenefitsoptindicator,
       a.source_system_ordinal_position,
       a.contractoptionissueage,
       a.contractoptiondurationinmonths,
       a.contractoptionsinglejointindicator,
       a.contractoptiondeathindicator,
       a.contractoptionincomeenhancementindicator,
       a.contractoptionmodelingpwvbsegment,
       a.contractoptionmodelingtypegroupindicator,
       a.contractdocumentid,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat('PS',';',db.key_qualifier,';',db.company_code,';','0')) as uiddocumentgroupkey,
             null as contractoptionstatuscode,
             null as contractoptionmaximumanniversaryvalue,
             null as contractoptionbenefitsoptindicator,
             '0' as source_system_ordinal_position,
             null as contractoptionissueage,
             null as contractoptiondurationinmonths,
             null as contractoptionsinglejointindicator,
             null as contractoptiondeathindicator,
             null as contractoptionincomeenhancementindicator,
             null as contractoptionmodelingpwvbsegment,
             db.contract_option_modeling_type_group_indicator as contractoptionmodelingtypegroupindicator,
             generateasameuuid(concat('PS',';',db.key_qualifier,';',db.company_code)) as contractdocumentid,
             db.operation_code as operationcode,
             ('cycle_date') as load_date,
             {batchid} as batchid,
             row_number() over(PARTITION BY inf.ex_policy_number,
                                             db.key_qualifier,
                                             db.company_code
                               ORDER BY cast(inf.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM (SELECT *
            FROM (SELECT *,
                         row_number() over(PARTITION BY key_qualifier,
                                                      company_code
                                           ORDER BY segment_sequence_number desc) as seq_num
                  FROM gdqp5dprodsegmentdb
                  WHERE cast(batchid as int) <= {batchid}) db1
            WHERE db1.seq_num = 1)db
           join (SELECT ex_policy_number,
                        batchid
                 FROM {source_database}.gdqp5inforceevaluation
                 WHERE cast(batchid as int) <= cast('{batchidinf}' as int)) inf
           ON db.key_qualifier = inf.ex_policy_number)a
WHERE a.seq_num = 1;
