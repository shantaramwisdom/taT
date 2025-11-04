SELECT key_qualifier,
       company_code,
       segment_id,
       system_name,
       segment_sequence_number,
       file_name,
       batchid,
       seq_num,
       CASE
           WHEN operation_code = 'DELT' THEN 'DELT'
           ELSE 'NON-DELT'
       END AS operation_code
FROM (SELECT key_qualifier,
             company_code,
             segment_id,
             system_name,
             segment_sequence_number,
             file_name,
             batchid,
             operation_code,
             row_number() over(PARTITION BY key_qualifier, company_code, segment_id, CASE
                                                                                        WHEN segment_id = 'JL' THEN segment_sequence_number
                                                                                        ELSE NULL
                                                                                    END
                                ORDER BY cast(batchid AS int) DESC, (CASE
                                                                          WHEN operation_code = 'DELT' THEN 2
                                                                          ELSE 1
                                                                      END)) AS seq_num
      FROM {source_database}.gdqp5deltaextract_mainfile_keys
      WHERE cast(batchid AS int) <= (batchid)) del
WHERE del.seq_num = 1
  AND ((file_name like 'CLPRODX'
        AND segment_id in ('JL',
                           'DB'))
       OR (file_name = 'DIRECT'
           AND segment_id in ('GB',
                              'GA'))
       OR (file_name like 'SCHED'));