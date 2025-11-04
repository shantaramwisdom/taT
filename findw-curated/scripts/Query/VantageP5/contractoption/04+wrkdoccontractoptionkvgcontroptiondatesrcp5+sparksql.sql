SELECT a.uiddocumentgroupkey,
       a.contractoptioneffectivedate,
       a.contractoptionbenefitselectiondate,
       a.contractoptionbenefitsoptdate,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code,';',cast(db.ordinal_position as string))) as uiddocumentgroupkey,
             cast(db.policy_eff_date as string) as contractoptioneffectivedate,
             null as contractoptionbenefitselectiondate,
             null as contractoptionbenefitsoptdate,
             db.operation_code as operationcode,
             ('cycle_date') as load_date,
             {batchid} as batchid,
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
           join (SELECT key_qualifier,
                        company_code,
                        ordinal_position,
                        policy_eff_date,
                        segment_id,
                        batchid,
                        operation_code
                 FROM gdqp5dprodsegmentdb
                 WHERE cast(batchid as int) <= {batchid}) db
           ON delta.key_qualifier = db.key_qualifier
           and delta.company_code = db.company_code)a
WHERE a.seq_num = 1
UNION all
SELECT a.uiddocumentgroupkey,
       a.contractoptioneffectivedate,
       a.contractoptionbenefitselectiondate,
       a.contractoptionbenefitsoptdate,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT null as uiddocumentgroupkey,
             null as contractoptioneffectivedate,
             null as contractoptionbenefitselectiondate,
             null as contractoptionbenefitsoptdate,
             db.operation_code as operationcode,
             ('cycle_date') as load_date,
             {batchid} as batchid,
             row_number() over(PARTITION BY inf.ex_policy_number,
                                             db.key_qualifier,
                                             db.company_code
                               ORDER BY cast(inf.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM (SELECT *
            FROM (SELECT key_qualifier,
                         company_code,
                         batchid,
                         operation_code,
                         segment_sequence_number,
                         ordinal_position,
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