SELECT db.*,
       CASE
            WHEN delta.operation_code = 'DELT' THEN 'DELT'
            ELSE 'NON-DELT'
       END AS operation_code
FROM (SELECT key_qualifier,
             company_code,
             batchid,
             row_number() OVER (PARTITION BY key_qualifier,
                                               company_code
                                ORDER BY cast(batchid AS INT) DESC) AS seq_num
      FROM {source_database}.gdqp5clprodsegmentdb
      WHERE cast(batchid AS INT) <= {batchid}) db
WHERE seq_num = 1 ) db
LEFT JOIN gdqp5deltaextract_mainfile_keys delta
ON db.key_qualifier = delta.key_qualifier
AND db.company_code = delta.company_code
AND delta.segment_id = 'DB'
