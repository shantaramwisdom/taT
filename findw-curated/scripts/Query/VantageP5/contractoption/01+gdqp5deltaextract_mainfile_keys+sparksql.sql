SELECT key_qualifier,
       company_code,
       segment_id,
       system_name,
       segment_sequence_number,
       file_name,
       batchid,
       CASE
           WHEN operation_code = 'DELT' THEN 'DELT'
           ELSE 'NON-DELT'
       END AS operation_code
FROM
    (SELECT key_qualifier,
            company_code,
            segment_id,
            system_name,
            segment_sequence_number,
            file_name,
            batchid,
            operation_code,
            row_number() OVER(PARTITION BY key_qualifier, company_code, segment_id
                              ORDER BY cast(batchid AS int) DESC,
                                       (CASE
                                            WHEN operation_code = 'DELT' THEN 2
                                            ELSE 1
                                        END)) AS seq_num
     FROM {source_database}.gdqp5deltaextract_mainfile_keys
     WHERE cast(batchid AS int) <= {batchid}
       AND system_name = 'PS'
       AND file_name LIKE 'CLPROD%'
       AND segment_id IN ('DB')
       AND (key_qualifier,
            company_code,
            segment_id) IN
           (SELECT DISTINCT key_qualifier,
                            company_code,
                            segment_id
            FROM {source_database}.gdqp5deltaextract_mainfile_keys
            WHERE cast(batchid AS int) <= {batchid}
              AND system_name = 'PS'
              AND file_name LIKE 'CLPROD%'
              AND segment_id IN ('DB'))) del
WHERE del.seq_num = 1;