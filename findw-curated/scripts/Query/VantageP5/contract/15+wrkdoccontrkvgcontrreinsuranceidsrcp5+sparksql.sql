SELECT a.uiddocumentgroupkey,
       a.contractmodelingreinsuranceidentifier,
       operationcode,
       a.dttloaddate,
       a.intbatchid
FROM (SELECT generatesameuuid(concat(deltaunion.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             cast(inf.contract_modeling_reinsurance_identifier as string) as contractmodelingreinsuranceidentifier,
             db.operation_code as operationcode,
             '{cycle_date}' as dttloaddate,
             {batchid} as intbatchid,
             row_number() OVER(PARTITION BY db.company_code,
                                          db.segment_id,
                                          db.key_qualifier,
                                          db.segment_sequence_number
                               ORDER BY cast(deltaunion.batchid as int) desc, cast(db.batchid as int) desc, cast(inf.batchid as int) desc) as seq_num
      FROM deltaunion
      JOIN (SELECT batchid,
                   key_qualifier,
                   company_code,
                   segment_id,
                   segment_sequence_number,
                   operation_code
            FROM gdqp5clprodsegmentdb
            WHERE cast(batchid as int) <= {batchid}) db
        ON deltaunion.key_qualifier = db.key_qualifier
      LEFT JOIN (SELECT coalesce(contract_modeling_reinsurance_identifier_override,
                                 c_contract_modeling_reinsurance_identifier,
                                 contract_modeling_reinsurance_identifier,
                                 'NULL') as contract_modeling_reinsurance_identifier,
                        batchid,
                        policy_number
                 FROM {source_database}.gdqp5inforceevaluation_derived
                 WHERE cast(batchid as int) = cast('{batchidinf}' as int)) inf
        ON db.key_qualifier = inf.policy_number) a
WHERE a.seq_num = 1;
