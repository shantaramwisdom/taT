SELECT a.key_qualifier,
       a.company_code,
       a.system_name,
       a.batchid,
       a.operation_code,
       a.segment_id
FROM (SELECT delta.key_qualifier,
             delta.company_code,
             delta.system_name,
             delta.batchid,
             delta.operation_code,
             delta.segment_id
      FROM gdqp5deltaextract_mainfile_keys delta
      WHERE cast(delta.batchid as int) <= (batchid)
        and delta.system_name = 'PS'
        and (delta.file_name like 'CLPRODX'
             or delta.file_name like 'DIRECT'
             or delta.file_name like 'SCHED')
        and delta.segment_id in ('DB','JL','GB')
      UNION (SELECT key_qualifier,
                    company_code,
                    system_name,
                    batchid,
                    operation_code,
                    segment_id
             FROM (SELECT a.key_qualifier,
                          a.company_code,
                          a.system_name,
                          a.batchid,
                          a.operation_code,
                          a.segment_id
                   FROM (SELECT db.key_qualifier,
                                db.company_code,
                                delta.system_name,
                                delta.batchid,
                                delta.operation_code,
                                delta.segment_id,
                                row_number() OVER(PARTITION BY db.company_code,
                                                             db.segment_id,
                                                             db.key_qualifier
                                                  ORDER BY cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
                         FROM gdqp5clprodsegmentdb db
                         join vantagedelta acct
                           on db.statutory_company_code = acct.company
                          and db.management_code = acct.mgt_code
                          and db.plan_code = acct.plan
                         join (SELECT key_qualifier,
                                      company_code,
                                      system_name,
                                      batchid,
                                      operation_code,
                                      segment_id
                               FROM {source_database}.gdqp5deltaextract_mainfile_keys
                               WHERE file_name like 'CLPRODX'
                                 and segment_id = 'DB') delta
                           on delta.key_qualifier = db.key_qualifier
                          and delta.company_code = db.company_code) a
                   WHERE a.seq_num = 1)
      UNION (SELECT delta.key_qualifier,
                    delta.company_code,
                    delta.system_name,
                    delta.batchid,
                    delta.operation_code,
                    delta.segment_id
             FROM (SELECT DISTINCT ex_policy_number
                   FROM {source_database}.gdqp5inforceevaluation inf
                   WHERE cast(batchid as int) = cast('{batchid}' as int)) inf
             join (SELECT key_qualifier,
                          company_code,
                          system_name,
                          batchid,
                          operation_code,
                          segment_id,
                          row_number() OVER(PARTITION BY company_code,
                                                       segment_id,
                                                       key_qualifier
                                            ORDER BY cast(batchid as int) desc) as seq_num
                   FROM {source_database}.gdqp5deltaextract_mainfile_keys
                   WHERE file_name like 'CLPRODX'
                     and segment_id = 'DB') delta
               on delta.key_qualifier = inf.ex_policy_number
              and delta.seq_num = 1)
      UNION (SELECT delta.key_qualifier,
                    delta.company_code,
                    delta.system_name,
                    delta.batchid,
                    delta.operation_code,
                    delta.segment_id
             FROM (SELECT DISTINCT policy_number
                   FROM {source_database}.gdqp5inforceevaluation_derived inf2
                   WHERE cast(batchid as int) = cast('{batchid}' as int)) inf2
             join (SELECT key_qualifier,
                          company_code,
                          system_name,
                          batchid,
                          operation_code,
                          segment_id,
                          row_number() OVER(PARTITION BY company_code,
                                                       segment_id,
                                                       key_qualifier
                                            ORDER BY cast(batchid as int) desc) as seq_num
                   FROM {source_database}.gdqp5deltaextract_mainfile_keys
                   WHERE file_name like 'CLPRODX'
                     and segment_id = 'DB') delta
               on delta.key_qualifier = inf2.policy_number
              and delta.seq_num = 1)) a
GROUP BY a.key_qualifier, a.company_code, a.system_name, a.batchid, a.operation_code, a.segment_id;