with 
proddb as (select db.*, 
case when detail.operation_code = 'DELT' then 'DELT' 
     else 'NON-DELT' end as operation_code 
from (select distinct key_qualifier,company_code,ordinal_position,segment_sequence_number 
      from {source_database}.gdqp5dprodsegmentdb where cast(batchid as int) <= {batchid})db left join (select * 
      from (select key_qualifier,
                   company_code,
                   operation_code,
                   batchid,
                   row_number() over(partition by key_qualifier,
                                                  company_code 
                                     order by cast(batchid as int) desc,(case when operation_code = 'DELT' then 2 
                                                                               else 1 end)) as seq_num 
            from {source_database}.gdqp5deltaextract_mainfile_keys 
            where cast(batchid as int) <= {batchid} 
              and file_name like 'CLPROXX' 
              and segment_id in ('DB')) 
       where seq_num = 1) detail 
on db.key_qualifier = detail.key_qualifier and 
   db.company_code = detail.company_code), 
deltal as (select detail.* 
           from {source_database}.gdqp5deltaextract_mainfile_keys detail 
           where cast(detail.batchid as int) = {batchid} 
             and detail.segment_id in ('DB') 
             and detail.file_name like 'CLPROXX' ) 
select {batchid} batch_id, 
       '{cycle_date}' cycle_date, 
       '{domain_name}' domain, 
       '{source_system_name}' sys_nm, 
       '{source_system_name}' p_sys_nm, 
       'BALANCING COUNT' measure_name, 
       'curated' hop_name, 
       'recordcount' measure_field, 
       'INTEGER' measure_value_datatype, 
       'S' measure_src_tgt_adj_indicator, 
       'gdqp5deltaextract_mainfile_keys' measure_table, 
       count(1) measure_value from (select distinct db.company_code,
                                                       db.key_qualifier,
                                                       db.ordinal_position from deltal join proddb db 
                                                       on db.key_qualifier = deltal.key_qualifier 
                                                       and db.operation_code <> 'DELT' 
             union 
             select company_code,
                    key_qualifier,cast(ordinal_position as int) from (select distinct db.key_qualifier,db.company_code,'0' as ordinal_position,
                                                                              row_number() over(partition by key_qualifier,
                                                                                                             company_code 
                                                                                                order by db.segment_sequence_number desc) as seq_num 
                                                                from (select distinct db.company_code,
                                                                                       db.key_qualifier,
                                                                                       db.ordinal_position,
                                                                                       db.segment_sequence_number,
                                                                                       inf.ex_policy_number,
                                                                                       batchid 
                                                                      from proddb db join (select distinct ex_policy_number,
                                                                                                                  batchid 
                                                                                            from {source_database}.gdqp5inforceevaluation 
                                                                                            where cast(batchid as int) <= {batchidinf}) inf 
                                                                      on db.key_qualifier = inf.ex_policy_number 
                                                                      and db.operation_code <> 'DELT')db) where seq_num=1);
