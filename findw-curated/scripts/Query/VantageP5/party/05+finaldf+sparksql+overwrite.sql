SELECT delta.system_name,
       (CURRENT_TIMESTAMP, 'US/Central') as recordlast_timestamp,
       a.{source_system_name} as source_system_name,
       a.uuid as associateduuid,
       a.contractuuid,
       a.contractadminstrationlocationcode,
       a.partyfirstname,
       a.partylastname,
       a.partybirthdt,
       a.companyindividualcode,
       a.gender,
       a.partyLocationstatecode,
       a.operationcode,
       cast('{cycle_date}' as date) as cycle_date,
       cast('{batchid}' as int) as batch_id
FROM (SELECT CASE
                    WHEN ga.company_individual_code = 'I' THEN generateUUID(concat(delta.system_name, ':', db.key_qualifier, ':', db.company_code, ':', ga.first_name, ':', ga.last_name))
                    WHEN ga.company_individual_code = 'C' THEN generateUUID(concat(delta.system_name, ':', db.key_qualifier, ':', db.company_code, ':', ga.company_name))
             END as addcontractpartylogic,
             generateUUID(concat(delta.system_name, ':', db.key_qualifier, ':', db.company_code)) AS contractdocumentuuid,
             db.key_qualifier AS contractadminstrationlocationcode,
             db.company_code as contractadminstrationlocationcode,
             CASE
                    WHEN ga.company_individual_code = 'I' THEN ga.first_name
                    WHEN ga.company_individual_code = 'C' THEN ga.company_name_orig
             END AS partyfirstname,
             CASE
                    WHEN ga.company_individual_code = 'I' THEN ga.last_name_orig
                    WHEN ga.company_individual_code = 'C' THEN NULL
             END AS partylastname,
             ga.dateofbirth,
             ga.company_individual_code AS companyindividualcode,
             ga.gender,
             ga.partyLocationstatecode,
             CASE
                    WHEN db.operation_code = 'DELT'
                         OR gb.operation_code = 'DELT' THEN 'DELT'
                    ELSE 'NON-DELT'
             END AS operationcode,
             '{cycle_date}' AS loaddate,
             {batchid} AS batchid,
             row_number() over(PARTITION BY ga.company_code, ga.last_name, ga.first_name, ga.company_code, gb.master_number, db.key_qualifier, db.company_code
                               ORDER BY cast(delta.batchid As int) DESC, cast(gb.batchid As int) DESC, cast(ga.batchid As int) DESC, cast(db.batchid As int) DESC) AS seq_num
      FROM (SELECT DISTINCT delta.system_name,
                            delta.key_qualifier,
                            delta.company_code,
                            delta.batchid
            FROM gdqp5deltaextract_mainfile_keys delta
            WHERE cast(delta.batchid AS int) <= {batchid}) delta
JOIN gdqp5directsegmentgb_role_identification gb ON gb.directory_id = delta.key_qualifier
AND delta.company_code = gb.company_code
AND gb.company_code = gb.company_code
JOIN gdqp5clprodsegmentdb db ON gb.master_number = db.key_qualifier
AND gb.company_code = db.company_code
WHERE a.seq_num = 1 );