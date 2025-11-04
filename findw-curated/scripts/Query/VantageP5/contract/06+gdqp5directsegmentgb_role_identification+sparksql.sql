SELECT *
FROM (SELECT master_number,
             directory_id,
             role_code,
             c_role_code,
             role_code_override,
             company_code,
             batchid,
             time_stamp,
             rank() OVER(PARTITION BY master_number,
                                  directory_id
                          ORDER BY cast(batchid as int) desc) as role_rank
      FROM {source_database}.gdqp5directsegmentgb_role_identification
      WHERE cast(batchid as int) <= (batchid)) a
WHERE a.role_rank = 1
  and not (role_code like 'PEX')
  and not (role_code like 'DX')
  and not (role_code in ('CLI','CAA'));
