SELECT directory_id,
       company_code,
       ascii_ignore(first_name) as first_name,
       concat(ascii_ignore(last_name), '_', directory_id) as last_name,
       concat(ascii_ignore(company_name), '_', directory_id) as company_name,
       company_individual_code,
       batchid
FROM (SELECT directory_id,
             company_code,
             first_name,
             last_name,
             company_name,
             company_individual_code,
             batchid,
             row_number() OVER(PARTITION BY directory_id,
                                          company_code,
                                          first_name,
                                          last_name,
                                          company_name
                               ORDER BY cast(batchid as int) desc) as ga_seq_num
      FROM {source_database}.gdqp5directsegmentga
      WHERE cast(batchid as int) <= (batchid)) g
WHERE ga_seq_num = 1
  and (company_name <> ''
       or first_name <> ''
       or last_name <> '');
