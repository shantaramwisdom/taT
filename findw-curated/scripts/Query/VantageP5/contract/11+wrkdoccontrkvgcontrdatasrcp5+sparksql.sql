WITH sched AS
(SELECT *
 FROM (SELECT key_qualifier,
              company_code,
              batchid,
              coalesce(policy_status_override, c_policy_status, policy_status, 'NULL') AS policy_status,
              segment_sequence_number,
              row_number() OVER (PARTITION BY sch.company_code,
                                         sch.key_qualifier
                                ORDER BY cast(sch.batchid AS INT) DESC, sch.segment_sequence_number DESC) as fltr
       FROM {source_database}.gdqp5sched sch
       WHERE cast(batchid AS INT) <= {batchid} )
 WHERE fltr = 1)
SELECT a.uiddocumentgroupkey,
       a.contractnumber,
       a.contractissueage,
       a.contractstatuscode,
       a.contractplancode,
       a.contractnumbernumericid,
       a.contractdurationinyears,
       a.contractgroupindicator,
       a.residentstatecode,
       a.contractreportinglineofbusiness,
       a.contractparticipationindicator,
       a.contractproducttypename,
       a.contractformcodespecialneeds,
       operationcode,
       a.dttlloaddate,
       a.intbatchid
FROM (SELECT generatesameuuid(concat(delta.system_name,':', db.key_qualifier,':', db.company_code)) as uiddocumentgroupkey,
             db.key_qualifier as contractnumber,
             cast(db.policy_issue_age as string) as contractissueage,
             sch.policy_status as contractstatuscode,
             db.plan_code as contractplancode,
             db.contract_number_numeric_id as contractnumbernumericid,
             inf.contract_duration_in_years as contractdurationinyears,
             db.group_indiv_ind as contractgroupindicator,
             db.resident_state as residentstatecode,
             db.management_cd as contractreportinglineofbusiness,
             db.contract_participation_indicator as contractparticipationindicator,
             db.processing_system_id as contractproducttypename,
             db.contract_form as contractformcodespecialneeds,
             db.operation_code as operationcode,
             '{cycle_date}' as dttlloaddate,
             {batchid} as intbatchid,
             row_number() OVER(PARTITION BY db.company_code,
                                       db.segment_id,
                                       db.key_qualifier,
                                       inf.ex_policy_number
                            ORDER BY cast(db.batchid as int) desc,
                                     db.segment_sequence_number desc,
                                     cast(sch.batchid as int) desc,
                                     cast(inf.batchid as int) desc) as seq_num
      FROM deltaunion delta
      JOIN (SELECT key_qualifier,
                   company_code,
                   policy_issue_age,
                   group_indiv_ind,
                   resident_state,
                   management_code,
                   contract_participation_indicator,
                   management_cd,
                   plan_code,
                   contract_number_numeric_id,
                   segment_id,
                   segment_sequence_number,
                   contract_form,
                   processing_system_id,
                   batchid,
                   operation_code
            FROM gdqp5clprodsegmentdb
            WHERE cast(batchid as int) <= (batchid)) db
        ON delta.key_qualifier = db.key_qualifier
       AND delta.company_code = db.company_code
      INNER JOIN sched sch
        ON delta.key_qualifier = sch.key_qualifier
       AND delta.company_code = sch.company_code
      LEFT JOIN (SELECT batchid,
                        ex_policy_number,
                        coalesce(contract_duration_in_years_override,
                                 c_contract_duration_in_years,
                                 contract_duration_in_years) as contract_duration_in_years
                 FROM {source_database}.gdqp5inforcevaluation
                 WHERE cast(batchid as int) = {batchidinf}) inf
        ON db.key_qualifier = inf.ex_policy_number)a
WHERE a.seq_num = 1;
