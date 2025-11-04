SELECT a.uiddocumentgroupkey,
       a.sourcesystemname,
       a.contractnumber,
       a.contractadministrationlocationcode,
       a.source_system_ordinal_position,
       a.contractoption_concatid,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat(delta.system_name,';',db.key_qualifier,';',db.company_code,';',cast(db.ordinal_position as string))) as uiddocumentgroupkey,
             delta.system_name as sourcesystemname,
             db.key_qualifier as contractnumber,
             db.company_code as contractadministrationlocationcode,
             cast(db.ordinal_position as string) as source_system_ordinal_position,
             concat(delta.system_name,';',
                    db.key_qualifier,
                    ';',
                    db.company_code,
                    ';',
                    cast(db.ordinal_position as string)) as contractoption_concatid,
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
              and delta.segment_id = 'DB'
              and delta.file_name like 'CLPROXX'
              and delta.system_name = 'PS') delta
           join (SELECT key_qualifier,
                        company_code,
                        batchid,
                        ordinal_position,
                        operation_code,
                        batchid,
                        segment_id
                 FROM gdqp5dprodsegmentdb
                 WHERE cast(batchid as int) <= {batchid}) db
           ON db.key_qualifier = delta.key_qualifier
           and db.company_code = delta.company_code)a
WHERE a.seq_num = 1
UNION
SELECT a.uiddocumentgroupkey,
       a.sourcesystemname,
       a.contractnumber,
       a.company_code,
       a.source_system_ordinal_position,
       a.contractoption_concatid,
       a.operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generateasameuuid(concat('PS',';',db.key_qualifier,';',db.company_code,';','0')) as uiddocumentgroupkey,
             'PS' as sourcesystemname,
             db.key_qualifier as contractnumber,
             db.company_code as company_code,
             '0' as source_system_ordinal_position,
             concat('PS',';',
                    db.key_qualifier,
                    ';',
                    db.company_code,
                    ';',
                    cast(db.ordinal_position as string)) as contractoption_concatid,
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
