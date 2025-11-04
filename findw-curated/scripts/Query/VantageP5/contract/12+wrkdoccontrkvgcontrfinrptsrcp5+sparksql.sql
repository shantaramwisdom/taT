WITH gdqvantageoneacctctr AS
(SELECT *
 FROM (SELECT company,
              mgt_code,
              plan,
              batchid,
              coalesce(center_number_override, c_center_number, center_number) as center_number,
              row_number() OVER (PARTITION BY company,
                                           mgt_code,
                                           plan
                                 ORDER BY cast(batchid AS int) DESC, center_number) as fltr
       FROM {source_database}.gdqvantageoneacctctr
       WHERE cast(batchid AS int) <= cast('{batchidvantage}' AS int))
 WHERE fltr = 1)
SELECT a.uiddocumentgroupkey,
       a.contractadministrationlocationcode,
       a.centernumber,
       a.sourcelegalentitycode,
       a.contractqualifiedindicator,
       a.contractdacgroup,
       a.contractlegalcompanycode,
       a.originatinglegalentitycode,
       operationcode,
       a.dttloaddate,
       a.intbatchid
FROM (SELECT generatesameuuid(concat(delta.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             db.company_code as contractadministrationlocationcode,
             acct.center_number as centernumber,
             db.source_legal_entity_code as sourcelegalentitycode,
             db.qual_non_qual_ind as contractqualifiedindicator,
             db.contract_dac_group as contractdacgroup,
             db.legal_company_code as contractlegalcompanycode,
             db.statutory_company_code as originatinglegalentitycode,
             db.operation_code as operationcode,
             '{cycle_date}' as dttloaddate,
             {batchid} as intbatchid,
             row_number() OVER(PARTITION BY db.company_code,
                                          db.segment_id,
                                          db.key_qualifier
                               ORDER BY cast(delta.batchid as int) desc, cast(db.batchid as int) desc, db.segment_sequence_number desc, cast(acct.batchid as int) desc) as seq_num
      FROM deltaunion delta
      JOIN (SELECT key_qualifier,
                   company_code,
                   source_legal_entity_code,
                   qual_non_qual_ind,
                   contract_dac_group,
                   legal_company_code,
                   operation_code,
                   segment_id,
                   key_qualifier,
                   segment_sequence_number,
                   batchid,
                   statutory_company_cd,
                   plan_code,
                   management_code,
                   statutory_company_code
            FROM gdqp5clprodsegmentdb) db
        ON delta.key_qualifier = db.key_qualifier
       AND delta.company_code = db.company_code
      LEFT JOIN gdqvantageoneacctctr acct
        ON case when db.statutory_company_cd = 'MLI' then 'NHL' else db.statutory_company_cd end = acct.company
       AND db.management_code = acct.mgt_code
       AND db.plan_code = acct.plan) a
WHERE a.seq_num = 1;
