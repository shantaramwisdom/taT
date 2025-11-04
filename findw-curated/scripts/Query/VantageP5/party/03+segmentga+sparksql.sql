SELECT company_code,
       directory_id,
       company_individual_code,
       concat(company_name,'-',directory_id) as company_name,
       company_name as company_name_orig,
       concat(last_name,'-',directory_id) as last_name,
       last_name as last_name_orig,
       first_name,
       cast(birth_date AS date) AS dateofbirth,
       sex_code AS gender,
       state_code AS partyLocationstatecode,
       operation_code,
       batchid
FROM (SELECT ga.*,
             operation_code,
             row_number() over(PARTITION BY ga.company_code, ga.directory_id, ga.company_name, ga.last_name, ga.first_name, ga.company_individual_code
                               ORDER BY cast(ga.batchid As int) DESC) AS rtrn
      FROM (SELECT ga.company_code,
                   ga.directory_id,
                   ascii_ignore(coalesce(ga.company_individual_code_override, ga.c.company_individual_code, ga.company_individual_code)) as company_individual_code,
                   coalesce(ga.company_name_override, ga.c.company_name, ga.company_name) as company_name,
                   ascii_ignore(coalesce(ga.last_name_override, ga.c.last_name, ga.last_name)) as last_name,
                   ascii_ignore(coalesce(ga.first_name_override, ga.c.first_name, ga.first_name)) as first_name,
                   coalesce(ga.birth_date_override, ga.c.birth_date, ga.birth_date) as birth_date,
                   coalesce(ga.sex_code_override, ga.c.sex_code, ga.sex_code) as sex_code,
                   coalesce(ga.state_code_override, ga.c.state_code, ga.state_code) as state_code,
                   ga.batchid
            FROM {source_database}.gdqp5directsegmentga ga
            WHERE cast(batchid AS INT) <= {batchid}
            AND (company_name <> ''
                 OR first_name <> ''
                 OR last_name <> '')) ga
JOIN (SELECT key_qualifier,
             company_code,
             min(decode(segment_id, 'GB', 'NON DELT', operation_code)) AS operation_code
      FROM gdqp5deltaextract_mainfile_keys
      WHERE batchid = {batchid}
      AND segment_id in ('GA',
                         'GB')
      GROUP BY key_qualifier,
               company_code) delta ON
ga.directory_id = delta.key_qualifier
AND ga.company_code = delta.company_code)
WHERE fltrx = 1;
