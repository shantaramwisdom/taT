SELECT gb.*,
       CASE
            WHEN delta.operation_code = 'DELT' THEN 'DELT'
            ELSE 'NON-DELT'
       END As operation_code
FROM (SELECT *
      FROM (SELECT company_code,
                   directory_id,
                   master_number,
                   coalesce(role_code_override, a.role_code, role_code) AS role_code,
                   batchid,
                   rank() OVER (PARTITION BY directory_id,
                                             master_number,
                                             company_code
                                ORDER BY cast(batchid AS INT) DESC) AS seq_num
            FROM {source_database}.gdqp5directsegmentgb_role_identification
            WHERE cast(batchid AS INT) <= {batchid}
            AND NOT (role_code LIKE 'PEX')
            AND NOT (role_code LIKE 'DX')
            AND NOT (role_code IN ('CLI',
                                   'CAA'))) gb
      WHERE seq_num = 1 ) gb
LEFT JOIN gdqp5deltaextract_mainfile_keys delta ON delta.file_name = 'DIRECT'
AND delta.segment_id = 'GB'
AND gb.directory_id = delta.key_qualifier
AND gb.company_code = delta.company_code
