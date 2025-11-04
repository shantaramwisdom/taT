SELECT a.uiddocumentgroupkey,
       a.sourcesystemname,
       a.contractnumber,
       a.contractadministrationlocationcode,
       operationcode,
       a.load_date,
       a.batchid
FROM (SELECT generatesameuuid(concat(delta.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             delta.system_name as sourcesystemname,
             db.key_qualifier as contractnumber,
             db.company_code as contractadministrationlocationcode,
             db.operation_code as operationcode,
             '{cycle_date}' as load_date,
             {batchid} as batchid,
             row_number() OVER(PARTITION BY db.company_code,
                                          db.segment_id,
                                          db.key_qualifier
                               ORDER BY cast(delta.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc) as seq_num
      FROM deltaunion delta
      JOIN (SELECT key_qualifier,
                   company_code,
                   segment_id,
                   segment_sequence_number,
                   batchid,
                   operation_code
            FROM gdqp5clprodsegmentdb) db
        ON delta.key_qualifier = db.key_qualifier
       AND delta.company_code = db.company_code) a
WHERE a.seq_num = 1;
