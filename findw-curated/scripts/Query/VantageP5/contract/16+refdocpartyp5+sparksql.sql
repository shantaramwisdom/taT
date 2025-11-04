WITH monthend_batch AS
(SELECT max(cast(a.batchid AS Int)) monthend_batchid
 FROM {source_database}.gdqcompletedbatchidinfo a
 JOIN
  (SELECT max(rtaa_effective_date) rtaa_effective_date,
          source_system
   FROM {source_database}.gdqcompletedbatchidinfo
   WHERE batchid = {batchid}
     AND batch_description = 'DAILY'
   GROUP BY source_system) b USING (source_system)
 WHERE a.batch_description = 'MONTHLY'
   AND a.rtaa_effective_date <= b.rtaa_effective_date),
 mainfilekeys AS
(SELECT key_qualifier,
        company_code,
        segment_id,
        operation_code,
        batchid
 FROM gdqp5deltaextract_mainfile_keys a
 WHERE cast(a.batchid AS Int) <= {batchid}
   AND a.system_name = 'P5'
   AND ((a.file_name = 'DIRECT'
         AND a.segment_id in ('GA',
                              'GB'))
        OR (a.file_name like 'CLPRODX'
            AND a.segment_id = 'DB'))),
 role_identification AS
(SELECT master_number,
        company_code,
        directory_id,
        role_code,
        CAST (batchid AS int) gb_batchid
 FROM gdqp5directsegmentgb_role_identification a
 GROUP BY master_number,
          company_code,
          directory_id,
          role_code,
          CAST (batchid AS int)),
 directsegmentga AS
(SELECT directory_id,
        company_code,
        last_name,
        first_name,
        company_name,
        company_individual_code,
        max(CAST (batchid AS Int)) ga_batchid
 FROM gdqp5directsegmentga
 GROUP BY directory_id,
          company_code,
          last_name,
          first_name,
          company_name,
          company_individual_code),
 SELECT CASE
        WHEN company_individual_code = 'I' THEN generatesameuuid(CONCAT('P5', ':', master_number, ':', company_code, ':', first_name, ':', last_name))
        WHEN company_individual_code = 'C' THEN generatesameuuid(CONCAT('P5', ':', master_number, ':', company_code, ':', company_name))
        END AS documentid
 FROM
 (SELECT directory_id,
         last_name,
         first_name,
         master_number,
         a.company_code,
         company_individual_code,
         company_name,
         row_number() over(PARTITION BY a.company_code, last_name, first_name, master_number, a.directory_id
                           ORDER BY ga_batchid DESC, gb_batchid DESC, cast(c.batchid AS Int) DESC) AS fltrx
  FROM role_identification a
  JOIN gdqp5clprodsegmentdb c ON a.master_number = c.key_qualifier
   AND c.operation_code != 'DELT'
   AND a.company_code = c.company_code
  JOIN
   (SELECT key_qualifier,
           company_code
    FROM mainfileKeys
    WHERE segment_id in ('GB')) d ON a.directory_id = d.key_qualifier
   AND a.company_code = d.company_code
  JOIN
   (SELECT company_individual_code,
           directory_id,
           a.company_code,
           last_name,
           first_name,
           company_name,
           ga_batchid
    FROM directsegmentga a) b
  JOIN
   (SELECT key_qualifier,
           company_code
    FROM mainfileKeys --JOIN monthend_batch b ON monthend_batchid <= cast(batchid AS int)
    WHERE operation_code != 'DELT'
      AND segment_id in ('GA')) b ON a.directory_id = b.key_qualifier
  USING (directory_id,
         company_code))
WHERE fltrx = 1;
